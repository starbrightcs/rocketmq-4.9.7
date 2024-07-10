/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.producer;

import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.hook.*;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.latency.Resolver;
import org.apache.rocketmq.client.latency.ServiceDetector;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.CorrelationIdUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;

public class DefaultMQProducerImpl implements MQProducerInner {

    private final Logger log = LoggerFactory.getLogger(DefaultMQProducerImpl.class);
    private final Random random = new Random();
    private final DefaultMQProducer defaultMQProducer;
    private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable =
        new ConcurrentHashMap<>();
    private final ArrayList<SendMessageHook> sendMessageHookList = new ArrayList<>();
    private final ArrayList<EndTransactionHook> endTransactionHookList = new ArrayList<>();
    private final RPCHook rpcHook;
    private final BlockingQueue<Runnable> asyncSenderThreadPoolQueue;
    private final ExecutorService defaultAsyncSenderExecutor;
    protected BlockingQueue<Runnable> checkRequestQueue;
    protected ExecutorService checkExecutor;
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory;
    private ArrayList<CheckForbiddenHook> checkForbiddenHookList = new ArrayList<>();
    private MQFaultStrategy mqFaultStrategy;
    private ExecutorService asyncSenderExecutor;

    // backpressure related
    private Semaphore semaphoreAsyncSendNum;
    private Semaphore semaphoreAsyncSendSize;

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer) {
        this(defaultMQProducer, null);
    }

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer, RPCHook rpcHook) {
        // 将 defaultMQProducer 存放在成员变量中
        this.defaultMQProducer = defaultMQProducer;
        this.rpcHook = rpcHook;

        // 1、创建一个异步发送的队列
        this.asyncSenderThreadPoolQueue = new LinkedBlockingQueue<>(50000);
        // 2、创建一个异步发送的线程池
        this.defaultAsyncSenderExecutor = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.asyncSenderThreadPoolQueue,
            new ThreadFactoryImpl("AsyncSenderExecutor_"));
        if (defaultMQProducer.getBackPressureForAsyncSendNum() > 10) {
            // 默认值为 10000
            semaphoreAsyncSendNum = new Semaphore(Math.max(defaultMQProducer.getBackPressureForAsyncSendNum(), 10), true);
        } else {
            semaphoreAsyncSendNum = new Semaphore(10, true);
            log.info("semaphoreAsyncSendNum can not be smaller than 10.");
        }

        // 默认值为 100M = 1024 * 1024 * 1024
        if (defaultMQProducer.getBackPressureForAsyncSendSize() > 1024 * 1024) {
            semaphoreAsyncSendSize = new Semaphore(Math.max(defaultMQProducer.getBackPressureForAsyncSendSize(), 1024 * 1024), true);
        } else {
            semaphoreAsyncSendSize = new Semaphore(1024 * 1024, true);
            log.info("semaphoreAsyncSendSize can not be smaller than 1M.");
        }

        ServiceDetector serviceDetector = new ServiceDetector() {
            @Override
            public boolean detect(String endpoint, long timeoutMillis) {
                Optional<String> candidateTopic = pickTopic();
                if (!candidateTopic.isPresent()) {
                    return false;
                }
                try {
                    MessageQueue mq = new MessageQueue(candidateTopic.get(), null, 0);
                    mQClientFactory.getMQClientAPIImpl()
                            .getMaxOffset(endpoint, mq, timeoutMillis);
                    return true;
                } catch (Exception e) {
                    return false;
                }
            }
        };

        this.mqFaultStrategy = new MQFaultStrategy(defaultMQProducer.cloneClientConfig(), new Resolver() {
            @Override
            public String resolve(String name) {
                return DefaultMQProducerImpl.this.mQClientFactory.findBrokerAddressInPublish(name);
            }
        }, serviceDetector);
    }
    private Optional<String> pickTopic() {
        if (topicPublishInfoTable.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(topicPublishInfoTable.keySet().iterator().next());
    }
    public void registerCheckForbiddenHook(CheckForbiddenHook checkForbiddenHook) {
        this.checkForbiddenHookList.add(checkForbiddenHook);
        log.info("register a new checkForbiddenHook. hookName={}, allHookSize={}", checkForbiddenHook.hookName(),
            checkForbiddenHookList.size());
    }

    public void setSemaphoreAsyncSendNum(int num) {
        semaphoreAsyncSendNum = new Semaphore(num, true);
    }

    public void setSemaphoreAsyncSendSize(int size) {
        semaphoreAsyncSendSize = new Semaphore(size, true);
    }

    public void initTransactionEnv() {
        TransactionMQProducer producer = (TransactionMQProducer) this.defaultMQProducer;
        if (producer.getExecutorService() != null) {
            this.checkExecutor = producer.getExecutorService();
        } else {
            this.checkRequestQueue = new LinkedBlockingQueue<>(producer.getCheckRequestHoldMax());
            this.checkExecutor = new ThreadPoolExecutor(
                producer.getCheckThreadPoolMinSize(),
                producer.getCheckThreadPoolMaxSize(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.checkRequestQueue);
        }
    }

    public void destroyTransactionEnv() {
        if (this.checkExecutor != null) {
            this.checkExecutor.shutdown();
        }
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register sendMessage Hook, {}", hook.hookName());
    }

    public void registerEndTransactionHook(final EndTransactionHook hook) {
        this.endTransactionHookList.add(hook);
        log.info("register endTransaction Hook, {}", hook.hookName());
    }

    public void start() throws MQClientException {
        this.start(true);
    }

    /**
     * MQ producer 启动
     *
     * @param startFactory
     * @throws MQClientException
     */
    public void start(final boolean startFactory) throws MQClientException {
        switch (this.serviceState) {
            // 如果状态为 CREATE_JUST，执行启动逻辑。该对象创建时默认状态为 CREATE_JUST
            case CREATE_JUST:
                // 1、设置启动失败
                this.serviceState = ServiceState.START_FAILED;

                // 2、检查配置，比如生产者组名是否合法，是否超过最大字符限制等
                this.checkConfig();

                /*
                    3、改变生产者的 instanceName 为进程 ID，避免同一个服务器上的多个生产者实例名相同。即将 instanceName 属性更改为 PID，
                    如果实例名为默认值则将生产者的 instanceName 设置为 UtilAll.getPid() + "#" + System.nanoTime()
                 */
                if (!this.defaultMQProducer.getProducerGroup().equals(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
                    this.defaultMQProducer.changeInstanceNameToPID();
                }

                /*
                    4、创建客户端 MQClientInstance 对象实例，使用 MQClientManager.getInstance() 返回一个单例的 MQClientManager 对象，
                    defaultMQProducer 继承了 ClientConfig，因此 getOrCreateMQClientInstance 方法的参数可以是 defaultMQProducer，
                    mQClientFactory 是 MQClientInstance 的一个实例，MQClientInstance 是 MQClientManager 的内部类
                 */
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQProducer, rpcHook);

                /*
                    5、注册生产者信息到本地，即注册 producer 实例：将生产者组名作为 key，defaultMQProducerImpl 对象作为 value
                    保存到 MQClientInstance 的 producerTable 中，方便后续调用网络请求、进行心跳检测等
                 */
                boolean registerOK = mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
                if (!registerOK) {
                    // 如果注册失败则将 serviceState 重设为 CREATE_JUST
                    this.serviceState = ServiceState.CREATE_JUST;
                    throw new MQClientException("The producer group[" + this.defaultMQProducer.getProducerGroup()
                        + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                        null);
                }

                // 6、启动生产者客户端 MQClientInstance 实例，如果已经启动，则不会执行
                if (startFactory) {
                    /*
                        MQClientInstance 的 start 方法会启动 MQClientInstance 的定时任务
                        包括定时向所有 broker 发送心跳、定时清理过期的 topic、定时清理过期的 consumer、定时清理过期的 producer
                     */
                    mQClientFactory.start();
                }

                /*
                    7、设置 topic 路由表信息，不过这里的 topic 是 “TBW102”
                    将 defaultMQProducer 的 createTopicKey 作为 key，TopicPublishInfo 作为 value，放入到 defaultMQProducerImpl
                    的topicPublishInfoTable中，createTopicKey 的默认值为 TBW102
                    topicPublishInfoTable 的作用是存储 topic 的路由信息，包括 topic 的 queue 数目、brokerName、brokerId 等
                 */
                this.initTopicRoute();

                this.mqFaultStrategy.startDetector();

                log.info("the producer [{}] start OK. sendMessageWithVIPChannel={}", this.defaultMQProducer.getProducerGroup(),
                    this.defaultMQProducer.isSendMessageWithVIPChannel());

                // 8、启动成功则将 serviceState 设置为 RUNNING
                this.serviceState = ServiceState.RUNNING;
                break;
            // 其他状态直接抛异常
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The producer service state not OK, maybe started once, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            default:
                break;
        }

        // 9、启动后马上向 NameServer 发送心跳
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();

        // 10、开启一个定时任务来处理所有的 Request 状态，对异步的请求根据状态处理回调函数，
        // 这个异步请求指的并不是在 send 中的异步回调机制，而是 Request-Reply 特性，用来模拟 RPC 调用
        RequestFutureHolder.getInstance().startScheduledTask(this);

    }

    /**
     * 检验生产者组名称
     *
     * @throws MQClientException
     */
    private void checkConfig() throws MQClientException {
        // 检验生产者组名称
        Validators.checkGroup(this.defaultMQProducer.getProducerGroup());

        // 生产所属组 不能等于 DEFAULT_PRODUCER，直接抛异常
        if (this.defaultMQProducer.getProducerGroup().equals(MixAll.DEFAULT_PRODUCER_GROUP)) {
            throw new MQClientException("producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP + ", please specify another one.",
                null);
        }
    }

    public void shutdown() {
        this.shutdown(true);
    }

    public void shutdown(final boolean shutdownFactory) {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.mQClientFactory.unregisterProducer(this.defaultMQProducer.getProducerGroup());
                this.defaultAsyncSenderExecutor.shutdown();
                if (shutdownFactory) {
                    this.mQClientFactory.shutdown();
                }
                this.mqFaultStrategy.shutdown();
                RequestFutureHolder.getInstance().shutdown(this);
                log.info("the producer [{}] shutdown OK", this.defaultMQProducer.getProducerGroup());
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    @Override
    public Set<String> getPublishTopicList() {
        return new HashSet<>(this.topicPublishInfoTable.keySet());
    }

    @Override
    public boolean isPublishTopicNeedUpdate(String topic) {
        TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);

        return null == prev || !prev.ok();
    }

    /**
     * @deprecated This method will be removed in the version 5.0.0 and {@link DefaultMQProducerImpl#getCheckListener} is recommended.
     */
    @Override
    @Deprecated
    public TransactionCheckListener checkListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionCheckListener();
        }

        return null;
    }

    @Override
    public TransactionListener getCheckListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionListener();
        }
        return null;
    }

    @Override
    public void checkTransactionState(final String addr, final MessageExt msg,
        final CheckTransactionStateRequestHeader header) {
        Runnable request = new Runnable() {
            private final String brokerAddr = addr;
            private final MessageExt message = msg;
            private final CheckTransactionStateRequestHeader checkRequestHeader = header;
            private final String group = DefaultMQProducerImpl.this.defaultMQProducer.getProducerGroup();

            @Override
            public void run() {
                TransactionCheckListener transactionCheckListener = DefaultMQProducerImpl.this.checkListener();
                TransactionListener transactionListener = getCheckListener();
                if (transactionCheckListener != null || transactionListener != null) {
                    LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
                    Throwable exception = null;
                    try {
                        if (transactionCheckListener != null) {
                            localTransactionState = transactionCheckListener.checkLocalTransactionState(message);
                        } else {
                            log.debug("TransactionCheckListener is null, used new check API, producerGroup={}", group);
                            localTransactionState = transactionListener.checkLocalTransaction(message);
                        }
                    } catch (Throwable e) {
                        log.error("Broker call checkTransactionState, but checkLocalTransactionState exception", e);
                        exception = e;
                    }

                    this.processTransactionState(
                        checkRequestHeader.getTopic(),
                        localTransactionState,
                        group,
                        exception);
                } else {
                    log.warn("CheckTransactionState, pick transactionCheckListener by group[{}] failed", group);
                }
            }

            private void processTransactionState(
                final String topic,
                final LocalTransactionState localTransactionState,
                final String producerGroup,
                final Throwable exception) {
                final EndTransactionRequestHeader thisHeader = new EndTransactionRequestHeader();
                thisHeader.setTopic(topic);
                thisHeader.setCommitLogOffset(checkRequestHeader.getCommitLogOffset());
                thisHeader.setProducerGroup(producerGroup);
                thisHeader.setTranStateTableOffset(checkRequestHeader.getTranStateTableOffset());
                thisHeader.setFromTransactionCheck(true);
                thisHeader.setBrokerName(checkRequestHeader.getBrokerName());

                String uniqueKey = message.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                if (uniqueKey == null) {
                    uniqueKey = message.getMsgId();
                }
                thisHeader.setMsgId(uniqueKey);
                thisHeader.setTransactionId(checkRequestHeader.getTransactionId());
                switch (localTransactionState) {
                    case COMMIT_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                        break;
                    case ROLLBACK_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                        log.warn("when broker check, client rollback this transaction, {}", thisHeader);
                        break;
                    case UNKNOW:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                        log.warn("when broker check, client does not know this transaction state, {}", thisHeader);
                        break;
                    default:
                        break;
                }

                String remark = null;
                if (exception != null) {
                    remark = "checkLocalTransactionState Exception: " + UtilAll.exceptionSimpleDesc(exception);
                }
                doExecuteEndTransactionHook(msg, uniqueKey, brokerAddr, localTransactionState, true);

                try {
                    DefaultMQProducerImpl.this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, thisHeader, remark,
                        3000);
                } catch (Exception e) {
                    log.error("endTransactionOneway exception", e);
                }
            }
        };

        this.checkExecutor.submit(request);
    }

    @Override
    public void updateTopicPublishInfo(final String topic, final TopicPublishInfo info) {
        if (info != null && topic != null) {
            TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
            if (prev != null) {
                log.info("updateTopicPublishInfo prev is not null, " + prev);
            }
        }
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQProducer.isUnitMode();
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.makeSureStateOK();
        Validators.checkTopic(newTopic);
        Validators.isSystemTopic(newTopic);

        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag, null);
    }

    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The producer service state not OK, "
                + this.serviceState
                + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                null);
        }
    }

    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public MessageExt viewMessage(String topic,
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.makeSureStateOK();

        return this.mQClientFactory.getMQAdminImpl().viewMessage(topic, msgId);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey)
        throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    /**
     * DEFAULT ASYNC -------------------------------------------------------
     */
    public void send(Message msg,
        SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        send(msg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * @param msg
     * @param sendCallback
     * @param timeout      the <code>sendCallback</code> will be invoked at most time
     * @throws RejectedExecutionException
     * @deprecated It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     */
    @Deprecated
    public void send(final Message msg, final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        BackpressureSendCallBack newCallBack = new BackpressureSendCallBack(sendCallback);

        final long beginStartTime = System.currentTimeMillis();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeout > costTime) {
                    try {
                        sendDefaultImpl(msg, CommunicationMode.ASYNC, newCallBack, timeout - costTime);
                    } catch (Exception e) {
                        newCallBack.onException(e);
                    }
                } else {
                    newCallBack.onException(
                        new RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout"));
                }
            }
        };
        executeAsyncMessageSend(runnable, msg, newCallBack, timeout, beginStartTime);
    }

    class BackpressureSendCallBack implements SendCallback {
        public boolean isSemaphoreAsyncSizeAcquired = false;
        public boolean isSemaphoreAsyncNumbAcquired = false;
        public int msgLen;
        private final SendCallback sendCallback;

        public BackpressureSendCallBack(final SendCallback sendCallback) {
            this.sendCallback = sendCallback;
        }

        @Override
        public void onSuccess(SendResult sendResult) {
            if (isSemaphoreAsyncSizeAcquired) {
                semaphoreAsyncSendSize.release(msgLen);
            }
            if (isSemaphoreAsyncNumbAcquired) {
                semaphoreAsyncSendNum.release();
            }
            sendCallback.onSuccess(sendResult);
        }

        @Override
        public void onException(Throwable e) {
            if (isSemaphoreAsyncSizeAcquired) {
                semaphoreAsyncSendSize.release(msgLen);
            }
            if (isSemaphoreAsyncNumbAcquired) {
                semaphoreAsyncSendNum.release();
            }
            sendCallback.onException(e);
        }
    }

    public void executeAsyncMessageSend(Runnable runnable, final Message msg, final BackpressureSendCallBack sendCallback,
        final long timeout, final long beginStartTime)
        throws MQClientException, InterruptedException {
        ExecutorService executor = this.getAsyncSenderExecutor();
        boolean isEnableBackpressureForAsyncMode = this.getDefaultMQProducer().isEnableBackpressureForAsyncMode();
        boolean isSemaphoreAsyncNumbAcquired = false;
        boolean isSemaphoreAsyncSizeAcquired = false;
        int msgLen = msg.getBody() == null ? 1 : msg.getBody().length;

        try {
            if (isEnableBackpressureForAsyncMode) {
                long costTime = System.currentTimeMillis() - beginStartTime;
                isSemaphoreAsyncNumbAcquired = timeout - costTime > 0
                    && semaphoreAsyncSendNum.tryAcquire(timeout - costTime, TimeUnit.MILLISECONDS);
                if (!isSemaphoreAsyncNumbAcquired) {
                    sendCallback.onException(
                        new RemotingTooMuchRequestException("send message tryAcquire semaphoreAsyncNum timeout"));
                    return;
                }
                costTime = System.currentTimeMillis() - beginStartTime;
                isSemaphoreAsyncSizeAcquired = timeout - costTime > 0
                    && semaphoreAsyncSendSize.tryAcquire(msgLen, timeout - costTime, TimeUnit.MILLISECONDS);
                if (!isSemaphoreAsyncSizeAcquired) {
                    sendCallback.onException(
                        new RemotingTooMuchRequestException("send message tryAcquire semaphoreAsyncSize timeout"));
                    return;
                }
            }
            sendCallback.isSemaphoreAsyncSizeAcquired = isSemaphoreAsyncSizeAcquired;
            sendCallback.isSemaphoreAsyncNumbAcquired = isSemaphoreAsyncNumbAcquired;
            sendCallback.msgLen = msgLen;
            executor.submit(runnable);
        } catch (RejectedExecutionException e) {
            if (isEnableBackpressureForAsyncMode) {
                runnable.run();
            } else {
                throw new MQClientException("executor rejected ", e);
            }
        }
    }

    public MessageQueue invokeMessageQueueSelector(Message msg, MessageQueueSelector selector, Object arg,
                                                   final long timeout) throws MQClientException, RemotingTooMuchRequestException {
        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            MessageQueue mq = null;
            try {
                List<MessageQueue> messageQueueList =
                        mQClientFactory.getMQAdminImpl().parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());
                Message userMessage = MessageAccessor.cloneMessage(msg);
                String userTopic = NamespaceUtil.withoutNamespace(userMessage.getTopic(), mQClientFactory.getClientConfig().getNamespace());
                userMessage.setTopic(userTopic);

                mq = mQClientFactory.getClientConfig().queueWithNamespace(selector.select(messageQueueList, userMessage, arg));
            } catch (Throwable e) {
                throw new MQClientException("select message queue threw exception.", e);
            }

            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeout < costTime) {
                throw new RemotingTooMuchRequestException("sendSelectImpl call timeout");
            }
            if (mq != null) {
                return mq;
            } else {
                throw new MQClientException("select message queue return null.", null);
            }
        }

        validateNameServerSetting();
        throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName, final boolean resetIndex) {
        return this.mqFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName, resetIndex);
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation,
                                boolean reachable) {
        this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation, reachable);
    }

    private void validateNameServerSetting() throws MQClientException {
        List<String> nsList = this.getMqClientFactory().getMQClientAPIImpl().getNameServerAddressList();
        if (null == nsList || nsList.isEmpty()) {
            throw new MQClientException(
                "No name server address, please set it." + FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL), null).setResponseCode(ClientErrorCode.NO_NAME_SERVER_EXCEPTION);
        }

    }

    /**
     * 发送消息实现：
     * <br> 1. 验证合法性 checkMessage
     * <br> 2. 查找主题路由信息 tryToFindTopicPublishInfo
     * <br> 3. 选择消息队列 selectOneMessageQueue
     * <br> 4. 发送消息 sendKernelImpl
     *
     * @param msg               消息
     * @param communicationMode 通信模式
     * @param sendCallback      回调
     * @param timeout           超时时间
     * @return 回调结果
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    private SendResult sendDefaultImpl(
        Message msg,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback,
        final long timeout
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        // 1、检验当前生产者服务是否处于处于 running 状态
        this.makeSureStateOK();
        // 2、检验 topic、消息长度等
        Validators.checkMessage(msg, this.defaultMQProducer);
        final long invokeID = random.nextLong();
        long beginTimestampFirst = System.currentTimeMillis();
        long beginTimestampPrev = beginTimestampFirst;
        long endTimestamp = beginTimestampFirst;
        /*
            3、获取到 topic 的路由信息，这里会有两种情况：
            1. 直接从 topicRouterTable 中获取到 topic 的路由信息
            2. 从 name server 中获取到 topic 的路由信息，并更新到 topicRouterTable 中
         */
        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        // 4、topicPublishInfo 不为空且可用
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            boolean callTimeout = false;
            MessageQueue mq = null;
            Exception exception = null;
            SendResult sendResult = null;
            // 4.1、判断当前发送的模式是否为同步发送，同步发送重试次数为 3 次，否则为 1 次，对应的是 oneway 模式
            int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1;
            int times = 0;
            String[] brokersSent = new String[timesTotal];
            boolean resetIndex = false;
            // 循环执行发送，处理同步发送重试。同步发送共重试 timesTotal 次，默认为 2 次，异步发送只执行一次
            for (; times < timesTotal; times++) {
                // 获取最近一次发送的 broker 的名称
                String lastBrokerName = null == mq ? null : mq.getBrokerName();
                if (times > 0) {
                    resetIndex = true;
                }
                // 4.1.1、选择一个消息队列，将消息发送到该队列，默认情况下：消息是轮询的发送到对应的消息队列，达到消息负载均衡的作用
                MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName, resetIndex);
                if (mqSelected != null) {
                    mq = mqSelected;
                    brokersSent[times] = mq.getBrokerName();
                    try {
                        beginTimestampPrev = System.currentTimeMillis();
                        if (times > 0) {
                            //Reset topic with namespace during resend.
                            msg.setTopic(this.defaultMQProducer.withNamespace(msg.getTopic()));
                        }
                        long costTime = beginTimestampPrev - beginTimestampFirst;
                        if (timeout < costTime) {
                            callTimeout = true;
                            break;
                        }

                        // 4.1.2、发送消息
                        sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback, topicPublishInfo, timeout - costTime);
                        endTimestamp = System.currentTimeMillis();
                        // 4.1.3、处理发送异常，更新失败条目
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false, true);
                        switch (communicationMode) {
                            case ASYNC:
                                return null;
                            case ONEWAY:
                                return null;
                            case SYNC:
                                if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                                    if (this.defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK()) {
                                        continue;
                                    }
                                }

                                return sendResult;
                            default:
                                break;
                        }
                    } catch (MQClientException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false, true);
                        log.warn("sendKernelImpl exception, resend at once, InvokeID: {}, RT: {}ms, Broker: {}", invokeID, endTimestamp - beginTimestampPrev, mq, e);
                        log.warn(msg.toString());
                        exception = e;
                        continue;
                    } catch (RemotingException e) {
                        endTimestamp = System.currentTimeMillis();
                        if (this.mqFaultStrategy.isStartDetectorEnable()) {
                            // Set this broker unreachable when detecting schedule task is running for RemotingException.
                            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true, false);
                        } else {
                            // Otherwise, isolate this broker.
                            this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true, true);
                        }
                        log.warn("sendKernelImpl exception, resend at once, InvokeID: {}, RT: {}ms, Broker: {}", invokeID, endTimestamp - beginTimestampPrev, mq, e);
                        if (log.isDebugEnabled()) {
                            log.debug(msg.toString());
                        }
                        exception = e;
                        continue;
                    } catch (MQBrokerException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true, false);
                        log.warn("sendKernelImpl exception, resend at once, InvokeID: {}, RT: {}ms, Broker: {}", invokeID, endTimestamp - beginTimestampPrev, mq, e);
                        if (log.isDebugEnabled()) {
                            log.debug(msg.toString());
                        }
                        exception = e;
                        if (this.defaultMQProducer.getRetryResponseCodes().contains(e.getResponseCode())) {
                            continue;
                        } else {
                            if (sendResult != null) {
                                return sendResult;
                            }

                            throw e;
                        }
                    } catch (InterruptedException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false, true);
                        log.warn("sendKernelImpl exception, throw exception, InvokeID: {}, RT: {}ms, Broker: {}", invokeID, endTimestamp - beginTimestampPrev, mq, e);
                        if (log.isDebugEnabled()) {
                            log.debug(msg.toString());
                        }
                        throw e;
                    }
                } else {
                    break;
                }
            }

            // 4.2、发送成功，返回发送结果
            if (sendResult != null) {
                return sendResult;
            }
            String info = String.format("Send [%d] times, still failed, cost [%d]ms, Topic: %s, BrokersSent: %s",
                times,
                System.currentTimeMillis() - beginTimestampFirst,
                msg.getTopic(),
                Arrays.toString(brokersSent));

            info += FAQUrl.suggestTodo(FAQUrl.SEND_MSG_FAILED);

            MQClientException mqClientException = new MQClientException(info, exception);
            if (callTimeout) {
                throw new RemotingTooMuchRequestException("sendDefaultImpl call timeout");
            }

            if (exception instanceof MQBrokerException) {
                mqClientException.setResponseCode(((MQBrokerException) exception).getResponseCode());
            } else if (exception instanceof RemotingConnectException) {
                mqClientException.setResponseCode(ClientErrorCode.CONNECT_BROKER_EXCEPTION);
            } else if (exception instanceof RemotingTimeoutException) {
                mqClientException.setResponseCode(ClientErrorCode.ACCESS_BROKER_TIMEOUT);
            } else if (exception instanceof MQClientException) {
                mqClientException.setResponseCode(ClientErrorCode.BROKER_NOT_EXIST_EXCEPTION);
            }

            throw mqClientException;
        }

        // 5、校验 NameServer 配置是否正确
        validateNameServerSetting();

        throw new MQClientException("No route info of this topic: " + msg.getTopic() + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO),
            null).setResponseCode(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION);
    }

    /**
     * 根据 topic 获取对应的 topic 详细信息
     *
     * @param topic topic
     * @return topic 的详细信息
     */
    private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
        // 1、从本地缓存 TopicPublishInfo 中尝试获取，第一次肯定为空
        TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
        // 如果路由信息没有找到，则从 NameServer 上获取
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            // 2、先写入一条数据到 topicPublishInfoTable 表中
            this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
            // 3、尝试从 NameServer 获取特定 topic 路由信息并更新本地缓存配置
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            // 再重新获取一次
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
        }

        // 4、如果找到可用的路由信息并返回
        if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
            return topicPublishInfo;
        } else {
            // 5、如果第一次未找到路由信息，则再次尝试使用默认的 topic 获取路由信息，核心，注意看，第二个参数是 true，
            // 这里使用的 topic 就是 TBW102 进行获取路由信息，然后再根据特定的（用户指定的 topic）进行替换默认的 topic 从而获得路由信息
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic, true, this.defaultMQProducer);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
            return topicPublishInfo;
        }
    }

    /**
     * 消息发送 API 核心入口
     * 1. 根据 MessageQueue 获取 Broker 地址
     * 2. 为消息分配全局唯一 ID，执行消息压缩和事务
     * 3. 如果注册了发送钩子函数，则执行发送之前的钩子函数
     * 4. 构建消息发送请求包
     * 5. 根据消息发送方式（同步、异步、单项）进行网络传输
     * 6. 如果注册了发送钩子函数，执行发送之后的钩子函数
     *
     * @param msg               待发送消息
     * @param mq                发送的消息队列
     * @param communicationMode 消息发送模式：SYNC、ASYNC、ONEWAY
     * @param sendCallback      异步发送回调函数
     * @param topicPublishInfo  主题路由信息
     * @param timeout           消息发送超时时间
     * @return 消息发送结果
     */
    private SendResult sendKernelImpl(final Message msg,
        final MessageQueue mq,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback,
        final TopicPublishInfo topicPublishInfo,
        final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        // 获取 brokerId
        String brokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(mq);
        // 根据 MessageQueue 获取 Broker 的网络地址
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(brokerName);
        if (null == brokerAddr) {
            // 如果 MQClientInstance 的 brokerAddrTable 未缓存该 Broker 信息，则尝试从 nameserever 主动获取 topic 的路由信息
            tryToFindTopicPublishInfo(mq.getTopic());
            // 重新设置 broker 信息
            brokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(mq);
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(brokerName);
        }

        SendMessageContext context = null;
        if (brokerAddr != null) {
            // 判断是否为 VIP 通道发送，如果是 vip 通道发送，对应的端口是 10909，默认端口是 10911
            brokerAddr = MixAll.brokerVIPChannel(this.defaultMQProducer.isSendMessageWithVIPChannel(), brokerAddr);

            byte[] prevBody = msg.getBody();
            try {
                //for MessageBatch,ID has been set in the generating process
                // 检查消息是否为 MessageBatch 类型
                if (!(msg instanceof MessageBatch)) {
                    // 设置消息的全局唯一 ID（UNIQUE_ID），对于批量消息，在生成过程中已经设置了 ID
                    MessageClientIDSetter.setUniqID(msg);
                }

                // 处理命名空间逻辑
                boolean topicWithNamespace = false;
                // 检查客户端配置中是否设置了命名空间
                if (null != this.mQClientFactory.getClientConfig().getNamespace()) {
                    msg.setInstanceId(this.mQClientFactory.getClientConfig().getNamespace());
                    topicWithNamespace = true;
                }

                // sysFlag 是消息的系统标志位，包含压缩标志位、事务标志位、批量标志位、多队列标志位等
                int sysFlag = 0;
                // 处理压缩，默认消息体超过 4KB 的消息进行 zip 压缩，并设置压缩标识
                boolean msgBodyCompressed = false;
                // 尝试压缩消息体
                if (this.tryToCompressMessage(msg)) {
                    sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
                    sysFlag |= this.defaultMQProducer.getCompressType().getCompressionFlag();
                    msgBodyCompressed = true;
                }

                // 处理事务 Prepared 消息，并设置事务标识
                final String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                // 检查消息是否为事务消息
                if (Boolean.parseBoolean(tranMsg)) {
                    sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
                }

                // 校验禁用钩子
                if (hasCheckForbiddenHook()) {
                    CheckForbiddenContext checkForbiddenContext = new CheckForbiddenContext();
                    checkForbiddenContext.setNameSrvAddr(this.defaultMQProducer.getNamesrvAddr());
                    checkForbiddenContext.setGroup(this.defaultMQProducer.getProducerGroup());
                    checkForbiddenContext.setCommunicationMode(communicationMode);
                    checkForbiddenContext.setBrokerAddr(brokerAddr);
                    checkForbiddenContext.setMessage(msg);
                    checkForbiddenContext.setMq(mq);
                    checkForbiddenContext.setUnitMode(this.isUnitMode());
                    this.executeCheckForbiddenHook(checkForbiddenContext);
                }

                // 发送消息前的钩子
                if (this.hasSendMessageHook()) {
                    context = new SendMessageContext();
                    context.setProducer(this);
                    context.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                    context.setCommunicationMode(communicationMode);
                    context.setBornHost(this.defaultMQProducer.getClientIP());
                    context.setBrokerAddr(brokerAddr);
                    context.setMessage(msg);
                    context.setMq(mq);
                    context.setNamespace(this.defaultMQProducer.getNamespace());
                    // 判断是不是事务消息，事务消息会先发送 half 消息，在最后进行 commit 的时候才会推送到 broker
                    String isTrans = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                    if (isTrans != null && isTrans.equals("true")) {
                        context.setMsgType(MessageType.Trans_Msg_Half);
                    }

                    // 判断是不是延迟消息
                    if (msg.getProperty("__STARTDELIVERTIME") != null || msg.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null) {
                        context.setMsgType(MessageType.Delay_Msg);
                    }
                    this.executeSendMessageHookBefore(context);
                }

                // 构建发送消息的请求头
                SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
                // 设置生产者组
                requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                // 设置 topic
                requestHeader.setTopic(msg.getTopic());
                // 设置默认的 topic：TBW102
                requestHeader.setDefaultTopic(this.defaultMQProducer.getCreateTopicKey());
                // 设置默认的队列数量：4
                requestHeader.setDefaultTopicQueueNums(this.defaultMQProducer.getDefaultTopicQueueNums());
                // 设置 queueId
                requestHeader.setQueueId(mq.getQueueId());
                // 设置 sysFlag，它是消息的系统标志位，包含压缩标志位、事务标志位、批量标志位、多队列标志位等
                requestHeader.setSysFlag(sysFlag);
                // 设置消息发送的时间
                requestHeader.setBornTimestamp(System.currentTimeMillis());
                requestHeader.setFlag(msg.getFlag());
                // 设置 properties，比如 TAGS,KEYS
                requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
                requestHeader.setReconsumeTimes(0);
                requestHeader.setUnitMode(this.isUnitMode());
                // 是否为批量消息
                requestHeader.setBatch(msg instanceof MessageBatch);
                requestHeader.setBrokerName(brokerName);
                // 如果是重发消息，则设置重发消息的次数
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    // 重发消息的次数
                    String reconsumeTimes = MessageAccessor.getReconsumeTime(msg);
                    if (reconsumeTimes != null) {
                        requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTimes));
                        // 清除消息的重发次数属性，因为消息的重发次数属性是在消息重发时设置的
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME);
                    }

                    // 消息重发的最大次数
                    String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(msg);
                    if (maxReconsumeTimes != null) {
                        // 设置重发消息的最大次数
                        requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
                        // 清除消息的最大重发次数属性，因为消息的最大重发次数属性是在消息重发时设置的
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
                    }
                }

                // 根据消息发送方式进行网络传输
                SendResult sendResult = null;
                switch (communicationMode) {
                    case ASYNC:
                        Message tmpMessage = msg;
                        boolean messageCloned = false;
                        if (msgBodyCompressed) {
                            //If msg body was compressed, msgbody should be reset using prevBody.
                            //Clone new message using commpressed message body and recover origin massage.
                            //Fix bug:https://github.com/apache/rocketmq-externals/issues/66
                            tmpMessage = MessageAccessor.cloneMessage(msg);
                            messageCloned = true;
                            // 防止压缩后的消息体重发时被再次压缩
                            msg.setBody(prevBody);
                        }

                        if (topicWithNamespace) {
                            if (!messageCloned) {
                                tmpMessage = MessageAccessor.cloneMessage(msg);
                                messageCloned = true;
                            }
                            // 防止设置了命名空间的 topic 重发时被再次设置命名空间
                            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
                        }

                        long costTimeAsync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeAsync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        // 异步发送：Producer 发出消息后无需等待 MQ 返回 ACK，直接发送下⼀条消息。
                        // 该方式的消息可靠性可以得到保障，消息发送效率也可以。
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                            brokerAddr,
                            brokerName,
                            tmpMessage,
                            requestHeader,
                            timeout - costTimeAsync,
                            communicationMode,
                            sendCallback,
                            topicPublishInfo,
                            this.mQClientFactory,
                            this.defaultMQProducer.getRetryTimesWhenSendAsyncFailed(),
                            context,
                            this);
                        break;
                    case ONEWAY:
                    case SYNC:
                        long costTimeSync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeSync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        // 执行客户端同步发送方法：Producer 发出⼀条消息后，会在收到 MQ 返回的 ACK 之后才发下⼀条消息。
                        // 该方式的消息可靠性最高，但消息发送效率太低。
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                            brokerAddr,
                            brokerName,
                            msg,
                            requestHeader,
                            timeout - costTimeSync,
                            communicationMode,
                            context,
                            this);
                        break;
                    default:
                        assert false;
                        break;
                }

                // 发送消息后的钩子函数
                if (this.hasSendMessageHook()) {
                    context.setSendResult(sendResult);
                    this.executeSendMessageHookAfter(context);
                }

                return sendResult;
            } catch (RemotingException | InterruptedException | MQBrokerException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } finally {
                msg.setBody(prevBody);
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
            }
        }

        // 主动更新后还是找不到路由信息，则抛出异常
        throw new MQClientException("The broker[" + brokerName + "] not exist", null);
    }

    public MQClientInstance getMqClientFactory() {
        return mQClientFactory;
    }

    @Deprecated
    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    private boolean tryToCompressMessage(final Message msg) {
        if (msg instanceof MessageBatch) {
            //batch does not support compressing right now
            return false;
        }
        byte[] body = msg.getBody();
        if (body != null) {
            // 默认消息大小为 4KB，消息大小超过 4KB 会进行 zlib 压缩
            if (body.length >= this.defaultMQProducer.getCompressMsgBodyOverHowmuch()) {
                try {
                    byte[] data = this.defaultMQProducer.getCompressor().compress(body, this.defaultMQProducer.getCompressLevel());
                    if (data != null) {
                        msg.setBody(data);
                        return true;
                    }
                } catch (IOException e) {
                    log.error("tryToCompressMessage exception", e);
                    if (log.isDebugEnabled()) {
                        log.debug(msg.toString());
                    }
                }
            }
        }

        return false;
    }

    public boolean hasCheckForbiddenHook() {
        return !checkForbiddenHookList.isEmpty();
    }

    public void executeCheckForbiddenHook(final CheckForbiddenContext context) throws MQClientException {
        if (hasCheckForbiddenHook()) {
            for (CheckForbiddenHook hook : checkForbiddenHookList) {
                hook.checkForbidden(context);
            }
        }
    }

    public boolean hasSendMessageHook() {
        return !this.sendMessageHookList.isEmpty();
    }

    public void executeSendMessageHookBefore(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageBefore(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookBefore", e);
                }
            }
        }
    }

    public void executeSendMessageHookAfter(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageAfter(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookAfter", e);
                }
            }
        }
    }

    public boolean hasEndTransactionHook() {
        return !this.endTransactionHookList.isEmpty();
    }

    public void executeEndTransactionHook(final EndTransactionContext context) {
        if (!this.endTransactionHookList.isEmpty()) {
            for (EndTransactionHook hook : this.endTransactionHookList) {
                try {
                    hook.endTransaction(context);
                } catch (Throwable e) {
                    log.warn("failed to executeEndTransactionHook", e);
                }
            }
        }
    }

    public void doExecuteEndTransactionHook(Message msg, String msgId, String brokerAddr, LocalTransactionState state,
        boolean fromTransactionCheck) {
        if (hasEndTransactionHook()) {
            EndTransactionContext context = new EndTransactionContext();
            context.setProducerGroup(defaultMQProducer.getProducerGroup());
            context.setBrokerAddr(brokerAddr);
            context.setMessage(msg);
            context.setMsgId(msgId);
            context.setTransactionId(msg.getTransactionId());
            context.setTransactionState(state);
            context.setFromTransactionCheck(fromTransactionCheck);
            executeEndTransactionHook(context);
        }
    }

    /**
     * DEFAULT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        try {
            this.sendDefaultImpl(msg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * KERNEL SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueue mq)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, mq, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueue mq, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        if (!msg.getTopic().equals(mq.getTopic())) {
            throw new MQClientException("message's topic not equal mq's topic", null);
        }

        long costTime = System.currentTimeMillis() - beginStartTime;
        if (timeout < costTime) {
            throw new RemotingTooMuchRequestException("call timeout");
        }

        return this.sendKernelImpl(msg, mq, CommunicationMode.SYNC, null, null, timeout);
    }

    /**
     * KERNEL ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        send(msg, mq, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * @param msg
     * @param mq
     * @param sendCallback
     * @param timeout      the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     * @deprecated It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     */
    @Deprecated
    public void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        BackpressureSendCallBack newCallBack = new BackpressureSendCallBack(sendCallback);
        final long beginStartTime = System.currentTimeMillis();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    makeSureStateOK();
                    Validators.checkMessage(msg, defaultMQProducer);

                    if (!msg.getTopic().equals(mq.getTopic())) {
                        throw new MQClientException("Topic of the message does not match its target message queue", null);
                    }
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeout > costTime) {
                        try {
                            sendKernelImpl(msg, mq, CommunicationMode.ASYNC, newCallBack, null,
                                timeout - costTime);
                        } catch (MQBrokerException e) {
                            throw new MQClientException("unknown exception", e);
                        }
                    } else {
                        newCallBack.onException(new RemotingTooMuchRequestException("call timeout"));
                    }
                } catch (Exception e) {
                    newCallBack.onException(e);
                }
            }

        };

        executeAsyncMessageSend(runnable, msg, newCallBack, timeout, beginStartTime);
    }

    /**
     * KERNEL ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg,
        MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        try {
            this.sendKernelImpl(msg, mq, CommunicationMode.ONEWAY, null, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * SELECT SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, selector, arg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.sendSelectImpl(msg, selector, arg, CommunicationMode.SYNC, null, timeout);
    }

    private SendResult sendSelectImpl(
        Message msg,
        MessageQueueSelector selector,
        Object arg,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback, final long timeout
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            MessageQueue mq = null;
            try {
                List<MessageQueue> messageQueueList =
                    mQClientFactory.getMQAdminImpl().parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());
                Message userMessage = MessageAccessor.cloneMessage(msg);
                String userTopic = NamespaceUtil.withoutNamespace(userMessage.getTopic(), mQClientFactory.getClientConfig().getNamespace());
                userMessage.setTopic(userTopic);

                mq = mQClientFactory.getClientConfig().queueWithNamespace(selector.select(messageQueueList, userMessage, arg));
            } catch (Throwable e) {
                throw new MQClientException("select message queue threw exception.", e);
            }

            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeout < costTime) {
                throw new RemotingTooMuchRequestException("sendSelectImpl call timeout");
            }
            if (mq != null) {
                return this.sendKernelImpl(msg, mq, communicationMode, sendCallback, null, timeout - costTime);
            } else {
                throw new MQClientException("select message queue return null.", null);
            }
        }

        validateNameServerSetting();
        throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
    }

    /**
     * SELECT ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        send(msg, selector, arg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param selector
     * @param arg
     * @param sendCallback
     * @param timeout      the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    @Deprecated
    public void send(final Message msg, final MessageQueueSelector selector, final Object arg,
        final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        BackpressureSendCallBack newCallBack = new BackpressureSendCallBack(sendCallback);
        final long beginStartTime = System.currentTimeMillis();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeout > costTime) {
                    try {
                        try {
                            sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, newCallBack,
                                timeout - costTime);
                        } catch (MQBrokerException e) {
                            throw new MQClientException("unknown exception", e);
                        }
                    } catch (Exception e) {
                        newCallBack.onException(e);
                    }
                } else {
                    newCallBack.onException(new RemotingTooMuchRequestException("call timeout"));
                }
            }

        };
        executeAsyncMessageSend(runnable, msg, newCallBack, timeout, beginStartTime);
    }

    /**
     * SELECT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, InterruptedException {
        try {
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    public TransactionSendResult sendMessageInTransaction(final Message msg,
        final TransactionListener localTransactionListener, final Object arg)
        throws MQClientException {
        TransactionListener transactionListener = getCheckListener();
        if (null == localTransactionListener && null == transactionListener) {
            throw new MQClientException("tranExecutor is null", null);
        }

        // ignore DelayTimeLevel parameter
        if (msg.getDelayTimeLevel() != 0) {
            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        }

        Validators.checkMessage(msg, this.defaultMQProducer);

        SendResult sendResult = null;
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, this.defaultMQProducer.getProducerGroup());
        try {
            sendResult = this.send(msg);
        } catch (Exception e) {
            throw new MQClientException("send message Exception", e);
        }

        LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
        Throwable localException = null;
        switch (sendResult.getSendStatus()) {
            case SEND_OK: {
                try {
                    if (sendResult.getTransactionId() != null) {
                        msg.putUserProperty("__transactionId__", sendResult.getTransactionId());
                    }
                    String transactionId = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                    if (null != transactionId && !"".equals(transactionId)) {
                        msg.setTransactionId(transactionId);
                    }
                    if (null != localTransactionListener) {
                        localTransactionState = localTransactionListener.executeLocalTransaction(msg, arg);
                    } else {
                        log.debug("Used new transaction API");
                        localTransactionState = transactionListener.executeLocalTransaction(msg, arg);
                    }
                    if (null == localTransactionState) {
                        localTransactionState = LocalTransactionState.UNKNOW;
                    }

                    if (localTransactionState != LocalTransactionState.COMMIT_MESSAGE) {
                        log.info("executeLocalTransactionBranch return: {} messageTopic: {} transactionId: {} tag: {} key: {}",
                            localTransactionState, msg.getTopic(), msg.getTransactionId(), msg.getTags(), msg.getKeys());
                    }
                } catch (Throwable e) {
                    log.error("executeLocalTransactionBranch exception, messageTopic: {} transactionId: {} tag: {} key: {}",
                        msg.getTopic(), msg.getTransactionId(), msg.getTags(), msg.getKeys(), e);
                    localException = e;
                }
            }
            break;
            case FLUSH_DISK_TIMEOUT:
            case FLUSH_SLAVE_TIMEOUT:
            case SLAVE_NOT_AVAILABLE:
                localTransactionState = LocalTransactionState.ROLLBACK_MESSAGE;
                break;
            default:
                break;
        }

        try {
            this.endTransaction(msg, sendResult, localTransactionState, localException);
        } catch (Exception e) {
            log.warn("local transaction execute " + localTransactionState + ", but end broker transaction failed", e);
        }

        TransactionSendResult transactionSendResult = new TransactionSendResult();
        transactionSendResult.setSendStatus(sendResult.getSendStatus());
        transactionSendResult.setMessageQueue(sendResult.getMessageQueue());
        transactionSendResult.setMsgId(sendResult.getMsgId());
        transactionSendResult.setQueueOffset(sendResult.getQueueOffset());
        transactionSendResult.setTransactionId(sendResult.getTransactionId());
        transactionSendResult.setLocalTransactionState(localTransactionState);
        return transactionSendResult;
    }

    /**
     * DEFAULT SYNC -------------------------------------------------------
     */
    public SendResult send(
        Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public void endTransaction(
        final Message msg,
        final SendResult sendResult,
        final LocalTransactionState localTransactionState,
        final Throwable localException) throws RemotingException, MQBrokerException, InterruptedException, UnknownHostException {
        final MessageId id;
        if (sendResult.getOffsetMsgId() != null) {
            id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
        } else {
            id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
        }
        String transactionId = sendResult.getTransactionId();
        final String destBrokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(defaultMQProducer.queueWithNamespace(sendResult.getMessageQueue()));
        final String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(destBrokerName);
        EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
        requestHeader.setTopic(msg.getTopic());
        requestHeader.setTransactionId(transactionId);
        requestHeader.setCommitLogOffset(id.getOffset());
        requestHeader.setBrokerName(destBrokerName);
        switch (localTransactionState) {
            case COMMIT_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                break;
            case ROLLBACK_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                break;
            case UNKNOW:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                break;
            default:
                break;
        }

        doExecuteEndTransactionHook(msg, sendResult.getMsgId(), brokerAddr, localTransactionState, false);
        requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
        requestHeader.setTranStateTableOffset(sendResult.getQueueOffset());
        requestHeader.setMsgId(sendResult.getMsgId());
        String remark = localException != null ? ("executeLocalTransactionBranch exception: " + localException.toString()) : null;
        this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, requestHeader, remark,
            this.defaultMQProducer.getSendMsgTimeout());
    }

    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.mQClientFactory.getMQClientAPIImpl().getRemotingClient().setCallbackExecutor(callbackExecutor);
    }

    public ExecutorService getAsyncSenderExecutor() {
        return null == asyncSenderExecutor ? defaultAsyncSenderExecutor : asyncSenderExecutor;
    }

    public void setAsyncSenderExecutor(ExecutorService asyncSenderExecutor) {
        this.asyncSenderExecutor = asyncSenderExecutor;
    }

    public SendResult send(Message msg,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        /*
         * 这里能看到默认发送的模式就是同步发送，参数解析
         * - msg: 我们要发送的消息
         * - communicationMode: 发送的模式，是个枚举值，SYNC、ASYNC、ONEWAY
         * - sendCallback: 当 CommunicationMode.ASYNC 时才会设置回调函数
         * - timeout: 发送超时时间
         */
        return this.sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeout);
    }

    public Message request(final Message msg,
        long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            return waitResponse(msg, timeout, requestResponseFuture, cost);
        } finally {
            RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(Message msg, final RequestCallback requestCallback, long timeout)
        throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
                requestResponseFuture.executeRequestCallback();
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);
    }

    public Message request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final long timeout) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException, RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            return waitResponse(msg, timeout, requestResponseFuture, cost);
        } finally {
            RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final RequestCallback requestCallback, final long timeout)
        throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);

    }

    public Message request(final Message msg, final MessageQueue mq, final long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, null, timeout - cost);

            return waitResponse(msg, timeout, requestResponseFuture, cost);
        } finally {
            RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        }
    }

    private Message waitResponse(Message msg, long timeout, RequestResponseFuture requestResponseFuture,
        long cost) throws InterruptedException, RequestTimeoutException, MQClientException {
        Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost);
        if (responseMessage == null) {
            if (requestResponseFuture.isSendRequestOk()) {
                throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
                    "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
            } else {
                throw new MQClientException("send request message to <" + msg.getTopic() + "> fail", requestResponseFuture.getCause());
            }
        }
        return responseMessage;
    }

    public void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout)
        throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, null, timeout - cost);
    }

    private void requestFail(final String correlationId) {
        RequestResponseFuture responseFuture = RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        if (responseFuture != null) {
            responseFuture.setSendRequestOk(false);
            responseFuture.putResponseMessage(null);
            try {
                responseFuture.executeRequestCallback();
            } catch (Exception e) {
                log.warn("execute requestCallback in requestFail, and callback throw", e);
            }
        }
    }

    private void prepareSendRequest(final Message msg, long timeout) {
        String correlationId = CorrelationIdUtil.createCorrelationId();
        String requestClientId = this.getMqClientFactory().getClientId();
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, requestClientId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TTL, String.valueOf(timeout));

        boolean hasRouteData = this.getMqClientFactory().getTopicRouteTable().containsKey(msg.getTopic());
        if (!hasRouteData) {
            long beginTimestamp = System.currentTimeMillis();
            this.tryToFindTopicPublishInfo(msg.getTopic());
            this.getMqClientFactory().sendHeartbeatToAllBrokerWithLock();
            long cost = System.currentTimeMillis() - beginTimestamp;
            if (cost > 500) {
                log.warn("prepare send request for <{}> cost {} ms", msg.getTopic(), cost);
            }
        }
    }

    private void initTopicRoute() {
        List<String> topics = this.defaultMQProducer.getTopics();
        if (topics != null && topics.size() > 0) {
            topics.forEach(topic -> {
                String newTopic = NamespaceUtil.wrapNamespace(this.defaultMQProducer.getNamespace(), topic);
                TopicPublishInfo topicPublishInfo = tryToFindTopicPublishInfo(newTopic);
                if (topicPublishInfo == null || !topicPublishInfo.ok()) {
                    log.warn("No route info of this topic: " + newTopic + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO));
                }
            });
        }
    }

    public ConcurrentMap<String, TopicPublishInfo> getTopicPublishInfoTable() {
        return topicPublishInfoTable;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    public void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public long[] getNotAvailableDuration() {
        return this.mqFaultStrategy.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.mqFaultStrategy.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.mqFaultStrategy.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.mqFaultStrategy.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.mqFaultStrategy.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.mqFaultStrategy.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }

    public MQFaultStrategy getMqFaultStrategy() {
        return mqFaultStrategy;
    }
}
