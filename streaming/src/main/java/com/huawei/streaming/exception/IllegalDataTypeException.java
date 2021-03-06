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

package com.huawei.streaming.exception;

/**
 * 返回结果类型推测异常
 *
 */
public class IllegalDataTypeException extends StreamingException
{
    
    private static final long serialVersionUID = 6341012270013225862L;
    
    /**
     * <默认构造函数>
     *
     * @param errorCode 异常码
     * @param errorArgs 异常参数
     */
    public IllegalDataTypeException(ErrorCode errorCode, String... errorArgs)
    {
        this(null, errorCode, errorArgs);
    }
    
    /**
     * <默认构造函数>
     *
     * @param cause 错误堆栈
     * @param errorCode 异常码
     * @param errorArgs 异常参数
     */
    public IllegalDataTypeException(Throwable cause, ErrorCode errorCode, String... errorArgs)
    {
        super(cause, errorCode, errorArgs);
    }
}
