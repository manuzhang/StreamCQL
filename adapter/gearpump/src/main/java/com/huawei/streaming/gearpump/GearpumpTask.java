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
import org.apache.gearpump.Message;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.streaming.javaapi.Task;
import org.apache.gearpump.streaming.task.StartTime;
import org.apache.gearpump.streaming.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GearpumpTask extends Task
{
  private static final Logger LOG = LoggerFactory.getLogger(GearpumpTask.class);
  private GearpumpAdapter adapter;


  public GearpumpTask(TaskContext taskContext, UserConfig userConfig) {
    super(taskContext, userConfig);
    this.adapter = (GearpumpAdapter) userConfig.getValue(Constants.GEARPUMP_ADAPTER,
            taskContext.system()).get();
  }

  @Override
  public void onStart(StartTime startTime)
  {
    LOG.debug("Start input stream at {}.", startTime);
    try
    {
      adapter.start();
    } catch (StreamingException e)
    {
      throw new RuntimeException("Failed to start input stream", e);

    }
  }

  @Override
  public void onNext(Message message)
  {
    LOG.debug("Process next message {}.", message);
    try
    {
      adapter.process(message);
    } catch (StreamingException e)
    {
      throw new RuntimeException("Failed to process message: " + message, e);

    }
  }

  @Override
  public void onStop()
  {
    LOG.debug("Stop input stream.");
    try
    {
      adapter.close();
    }
    catch (StreamingException e)
    {
      throw new RuntimeException("Failed to stop input stream", e);
    }
  }



}
