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

import com.huawei.streaming.application.Application;
import com.huawei.streaming.application.ApplicationResults;
import com.huawei.streaming.application.DistributeType;
import com.huawei.streaming.application.GroupInfo;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IRichOperator;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.cluster.embedded.EmbeddedCluster;
import org.apache.gearpump.partitioner.HashPartitioner;
import org.apache.gearpump.partitioner.Partitioner;
import org.apache.gearpump.partitioner.ShufflePartitioner;
import org.apache.gearpump.streaming.javaapi.Graph;
import org.apache.gearpump.streaming.javaapi.Processor;
import org.apache.gearpump.streaming.javaapi.StreamApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Gearpump application
 */
public class GearpumpApplication extends Application
{
  private static final Logger LOG = LoggerFactory.getLogger(GearpumpApplication.class);
  private ClientContext client;
  private Graph graph = new Graph();
  private Map<String, Processor>  processors = new HashMap<>();
  private StreamingConfig config;
  private String appName;
  private Integer appId;
  private String jarPath;

  public GearpumpApplication(String appName, StreamingConfig config) throws StreamingException
  {
    super(appName, config);
    this.appName = appName;
    this.config = config;
  }

  @Override
  public void launch() throws StreamingException
  {
    EmbeddedCluster cluster = EmbeddedCluster.apply();
    client = cluster.newClientContext();
    buildGraph();
    StreamApplication application = new StreamApplication(appName, UserConfig.empty(), graph);
    appId = client.submit(application, jarPath);
    long sleepTime = config.getLongValue(StreamingConfig.STREAMING_LOCALTASK_ALIVETIME_MS);
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    cluster.stop();
  }

  @Override
  public ApplicationResults getApplications() throws StreamingException
  {
    return new GearpumpApplicationResults(appId, appName);
  }

  @Override
  public boolean isApplicationExists() throws StreamingException
  {
    return client.resolveAppID(appId) != null;
  }

  @Override
  public void killApplication() throws StreamingException
  {
    client.shutdown(appId);
  }

  @Override
  public void setUserPackagedJar(String userJar)
  {
    this.jarPath = userJar;
  }

  private void buildGraph() throws StreamingException
  {
    List< ? extends IRichOperator> sources = getInputStreams();
    for (IRichOperator operator: sources)
    {
      Processor processor = getProcessor(operator);
      graph.addVertex(processor);
      processors.put(operator.getOperatorId(), processor);
    }

    List<IRichOperator> orderedFunOp = genFunctionOpsOrder();
    if (orderedFunOp == null || orderedFunOp.isEmpty())
    {
      LOG.debug("Topology don't have any function operator");
      return;
    }

    for (IRichOperator operator : orderedFunOp)
    {
      Processor processor = getProcessor(operator);
      graph.addVertex(processor);
      processors.put(operator.getOperatorId(), processor);
      List<String> streams = operator.getInputStream();
      if (streams == null)
      {
        StreamingException exception = new StreamingException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
        LOG.error("The operator input streaming is null.");
        throw exception;
      }

      for (String input: operator.getInputStream())
      {
        GroupInfo groupInfo = operator.getGroupInfo().get(input);
        setGrouping(processor, input, groupInfo);
      }
    }
  }

  private Processor getProcessor(IRichOperator operator)
  {
    SourceAdapter adapter = new SourceAdapter();
    adapter.setOperator(operator);
    return new Processor(GearpumpTask.class)
            .withParallelism(operator.getParallelNumber())
            .withDescription(operator.getOperatorId())
            .withConfig(UserConfig.empty()
                    .withValue(Constants.GEARPUMP_ADAPTER, adapter, client.system()));
  }

  private void setGrouping(Processor processor, String input, GroupInfo groupInfo)
          throws StreamingException
  {
    if (null == groupInfo)
    {
      setDefaultGrouping(processor, input);
      return;
    }

    DistributeType distribute = groupInfo.getDitributeType();
    switch (distribute)
    {
      case FIELDS:
        Partitioner partitioner = new GearpumpPartitioner(new HashPartitioner(), input);
        graph.addEdge(processors.get(input), partitioner, processor);
        break;
      case GLOBAL:
        break;
      case LOCALORSHUFFLE:
        break;
      case ALL:
        break;
      case DIRECT:
        break;
      case CUSTOM:
        break;
      case SHUFFLE:
      case NONE:
      default:
        setDefaultGrouping(processor, input);
    }
  }

  private void setDefaultGrouping(Processor processor, String input)
          throws StreamingException
  {
    Partitioner partitioner = new GearpumpPartitioner(new ShufflePartitioner(), input);
    graph.addEdge(processors.get(input), partitioner, processor);
  }
}
