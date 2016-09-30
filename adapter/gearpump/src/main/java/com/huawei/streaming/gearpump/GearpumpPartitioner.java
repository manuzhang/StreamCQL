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

import org.apache.gearpump.Message;
import org.apache.gearpump.partitioner.UnicastPartitioner;

public class GearpumpPartitioner implements UnicastPartitioner {

  private final UnicastPartitioner partitoner;
  private final String stream;

  public GearpumpPartitioner(UnicastPartitioner partitioner, String stream) {
    this.partitoner = partitioner;
    this.stream = stream;
  }

  @Override
  public int getPartition(Message message, int partitionNum, int upstreamTaskIndex) {
    GearpumpMessage msg = (GearpumpMessage) message.msg();
    if (msg.stream.equals(stream)) {
      return partitoner.getPartition(message, partitionNum, upstreamTaskIndex);
    } else {
      // the message will be filtered out
      return -1;
    }
  }

  @Override
  public int getPartition(Message message, int partitionNum) {
    return getPartition(message, partitionNum, -1);
  }
}
