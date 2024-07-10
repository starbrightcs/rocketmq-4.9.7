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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo.QueueFilter;
import org.apache.rocketmq.common.message.MessageQueue;

public class MQFaultStrategy {
    private LatencyFaultTolerance<String> latencyFaultTolerance;
    /**
     * 发送延迟故障启用，默认为 false
     */
    private volatile boolean sendLatencyFaultEnable;
    /**
     * 探测 broker 在不在的开关
     */
    private volatile boolean startDetectorEnable;
    private long[] latencyMax = {50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 2000L, 5000L, 6000L, 10000L, 30000L};

    public static class BrokerFilter implements QueueFilter {
        private String lastBrokerName;

        public void setLastBrokerName(String lastBrokerName) {
            this.lastBrokerName = lastBrokerName;
        }

        @Override public boolean filter(MessageQueue mq) {
            if (lastBrokerName != null) {
                return !mq.getBrokerName().equals(lastBrokerName);
            }
            return true;
        }
    }

    private ThreadLocal<BrokerFilter> threadBrokerFilter = new ThreadLocal<BrokerFilter>() {
        @Override protected BrokerFilter initialValue() {
            return new BrokerFilter();
        }
    };

    private QueueFilter reachableFilter = new QueueFilter() {
        @Override public boolean filter(MessageQueue mq) {
            return latencyFaultTolerance.isReachable(mq.getBrokerName());
        }
    };

    private QueueFilter availableFilter = new QueueFilter() {
        @Override public boolean filter(MessageQueue mq) {
            return latencyFaultTolerance.isAvailable(mq.getBrokerName());
        }
    };


    public MQFaultStrategy(ClientConfig cc, Resolver fetcher, ServiceDetector serviceDetector) {
        this.latencyFaultTolerance = new LatencyFaultToleranceImpl(fetcher, serviceDetector);
        this.latencyFaultTolerance.setDetectInterval(cc.getDetectInterval());
        this.latencyFaultTolerance.setDetectTimeout(cc.getDetectTimeout());
        this.setStartDetectorEnable(cc.isStartDetectorEnable());
        this.setSendLatencyFaultEnable(cc.isSendLatencyEnable());
    }

    // For unit test.
    public MQFaultStrategy(ClientConfig cc, LatencyFaultTolerance<String> tolerance) {
        this.setStartDetectorEnable(cc.isStartDetectorEnable());
        this.setSendLatencyFaultEnable(cc.isSendLatencyEnable());
        this.latencyFaultTolerance = tolerance;
        this.latencyFaultTolerance.setDetectInterval(cc.getDetectInterval());
        this.latencyFaultTolerance.setDetectTimeout(cc.getDetectTimeout());
    }


    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public QueueFilter getAvailableFilter() {
        return availableFilter;
    }

    public QueueFilter getReachableFilter() {
        return reachableFilter;
    }

    public ThreadLocal<BrokerFilter> getThreadBrokerFilter() {
        return threadBrokerFilter;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public boolean isStartDetectorEnable() {
        return startDetectorEnable;
    }

    public void setStartDetectorEnable(boolean startDetectorEnable) {
        this.startDetectorEnable = startDetectorEnable;
        this.latencyFaultTolerance.setStartDetectorEnable(startDetectorEnable);
    }

    public void startDetector() {
        this.latencyFaultTolerance.startDetector();
    }

    public void shutdown() {
        this.latencyFaultTolerance.shutdown();
    }

    /**
     * 选择发送的队列，根据是否启用 Broker 故障延迟机制走不同逻辑
     * <p>
     * sendLatencyFaultEnable=false，默认不启用 Broker 故障延迟机制
     * sendLatencyFaultEnable=true，启用 Broker 故障延迟机制
     *
     * @param tpInfo topic 路由信息
     * @param lastBrokerName 上一次选择的 broker name
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName, final boolean resetIndex) {
        BrokerFilter brokerFilter = threadBrokerFilter.get();
        brokerFilter.setLastBrokerName(lastBrokerName);
        // 默认值为 false，不会执行到该分支，启用 Broker 故障延迟机制
        if (this.sendLatencyFaultEnable) {
            if (resetIndex) {
                tpInfo.resetIndex();
            }
            // 轮询获取一个消息队列
            MessageQueue mq = tpInfo.selectOneMessageQueue(availableFilter, brokerFilter);
            if (mq != null) {
                return mq;
            }

            mq = tpInfo.selectOneMessageQueue(reachableFilter, brokerFilter);
            if (mq != null) {
                return mq;
            }

            return tpInfo.selectOneMessageQueue();
        }

        MessageQueue mq = tpInfo.selectOneMessageQueue(brokerFilter);
        if (mq != null) {
            return mq;
        }
        return tpInfo.selectOneMessageQueue();
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation,
                                final boolean reachable) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 10000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration, reachable);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i]) {
                return this.notAvailableDuration[i];
            }
        }

        return 0;
    }
}
