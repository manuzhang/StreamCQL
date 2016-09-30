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
package com.huawei.streaming.gearpump;

import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IEmitter;
import org.apache.gearpump.Message;
import org.apache.gearpump.streaming.task.TaskContext;

public class GearpumpEmitter implements IEmitter {

  private final String stream;
  private final TaskContext context;
  private Long timestamp;

  public GearpumpEmitter(String stream, TaskContext context) {
    this.stream = stream;
    this.context = context;
  }

  @Override
  public void emit(Object[] data) throws StreamingException {
    if (null == timestamp) {
      timestamp = System.currentTimeMillis();
    }

    context.output(Message.apply(new GearpumpMessage(stream, data), timestamp));
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }
}
