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
package org.apache.rocketmq.tools.command.topic;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.AttributeParser;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Map;
import java.util.Set;

/**
 * 创建或更新 topic 命令
 */
public class UpdateTopicSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateTopic";
    }

    @Override
    public String commandDesc() {
        return "Update or create topic.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        OptionGroup optionGroup = new OptionGroup();

        Option opt = new Option("b", "brokerAddr", true, "create topic to which broker");
        optionGroup.addOption(opt);

        opt = new Option("c", "clusterName", true, "create topic to which cluster");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("a", "attributes", true, "attribute(+a=b,+c=d,-e)");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("r", "readQueueNums", true, "set read queue nums");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("w", "writeQueueNums", true, "set write queue nums");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "perm", true, "set topic's permission(2|4|6), intro[2:W 4:R; 6:RW]");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("o", "order", true, "set topic's order(true|false)");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("u", "unit", true, "is unit topic (true|false)");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "hasUnitSub", true, "has unit sub (true|false)");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setReadQueueNums(8);
            topicConfig.setWriteQueueNums(8);
            topicConfig.setTopicName(commandLine.getOptionValue('t').trim());

            if (commandLine.hasOption('a')) {
                String attributesModification = commandLine.getOptionValue('a').trim();
                Map<String, String> attributes = AttributeParser.parseToMap(attributesModification);
                topicConfig.setAttributes(attributes);
            }

            // readQueueNums
            if (commandLine.hasOption('r')) {
                topicConfig.setReadQueueNums(Integer.parseInt(commandLine.getOptionValue('r').trim()));
            }

            // writeQueueNums
            if (commandLine.hasOption('w')) {
                topicConfig.setWriteQueueNums(Integer.parseInt(commandLine.getOptionValue('w').trim()));
            }

            // perm
            if (commandLine.hasOption('p')) {
                topicConfig.setPerm(Integer.parseInt(commandLine.getOptionValue('p').trim()));
            }

            boolean isUnit = false;
            if (commandLine.hasOption('u')) {
                isUnit = Boolean.parseBoolean(commandLine.getOptionValue('u').trim());
            }

            boolean isCenterSync = false;
            if (commandLine.hasOption('s')) {
                isCenterSync = Boolean.parseBoolean(commandLine.getOptionValue('s').trim());
            }

            int topicCenterSync = TopicSysFlag.buildSysFlag(isUnit, isCenterSync);
            topicConfig.setTopicSysFlag(topicCenterSync);

            boolean isOrder = false;
            if (commandLine.hasOption('o')) {
                isOrder = Boolean.parseBoolean(commandLine.getOptionValue('o').trim());
            }
            topicConfig.setOrder(isOrder);

            // 对应的 -b 和 -c 命令只能执行一个：
            // -b 对应的是在指定的 broker 上创建 topic
            // -c 对应的是在指定的集群上的每一个 broker 创建 topic
            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();

                defaultMQAdminExt.start();
                defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);

                if (isOrder) {
                    String brokerName = CommandUtil.fetchBrokerNameByAddr(defaultMQAdminExt, addr);
                    String orderConf = brokerName + ":" + topicConfig.getWriteQueueNums();
                    defaultMQAdminExt.createOrUpdateOrderConf(topicConfig.getTopicName(), orderConf, false);
                    System.out.printf("%s%n", String.format("set broker orderConf. isOrder=%s, orderConf=[%s]",
                        isOrder, orderConf.toString()));
                }
                System.out.printf("create topic to %s success.%n", addr);
                System.out.printf("%s%n", topicConfig);
                return;

            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                defaultMQAdminExt.start();

                Set<String> masterSet =
                    CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
                    System.out.printf("create topic to %s success.%n", addr);
                }

                if (isOrder) {
                    Set<String> brokerNameSet =
                        CommandUtil.fetchBrokerNameByClusterName(defaultMQAdminExt, clusterName);
                    StringBuilder orderConf = new StringBuilder();
                    String splitor = "";
                    for (String s : brokerNameSet) {
                        orderConf.append(splitor).append(s).append(":")
                            .append(topicConfig.getWriteQueueNums());
                        splitor = ";";
                    }
                    defaultMQAdminExt.createOrUpdateOrderConf(topicConfig.getTopicName(),
                        orderConf.toString(), true);
                    System.out.printf("set cluster orderConf. isOrder=%s, orderConf=[%s]%n", isOrder, orderConf);
                }

                System.out.printf("%s%n", topicConfig);
                return;
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

}
