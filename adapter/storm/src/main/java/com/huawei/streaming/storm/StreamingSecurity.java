/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.huawei.streaming.storm;

import com.huawei.streaming.exception.StreamingException;

/**
 * Streaming安全接口
 *
 */
public interface StreamingSecurity
{
    /**
     * 初始化安全配置
     *
     * @throws StreamingException 初始化异常
     */
    public void initSecurity()
     throws StreamingException;

    /**
     * 销毁安全配置
     *
     * @throws StreamingException 销毁异常
     */
    public void destroySecurity()
     throws StreamingException;
}
