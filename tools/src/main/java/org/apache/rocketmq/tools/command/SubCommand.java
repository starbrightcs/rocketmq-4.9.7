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
package org.apache.rocketmq.tools.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;

/**
 * 子命令接口
 */
public interface SubCommand {

    /**
     * 子命令名称
     */
    String commandName();

    /**
     * 子命令别名
     *
     * @return 子命令别名
     */
    default String commandAlias() {
        return null;
    }

    /**
     * 命令描述
     *
     * @return 命令描述
     */
    String commandDesc();

    /**
     * 构建命令解析器
     *
     * @param options 命令
     * @return 构建命令解析器
     */
    Options buildCommandlineOptions(final Options options);

    /**
     * 执行命令
     *
     * @param commandLine 命令行
     * @param options     命令
     * @param rpcHook     回调
     * @throws SubCommandException 异常
     */
    void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) throws SubCommandException;
}
