package com.huawei.streaming.gearpump;

import com.google.common.collect.Maps;
import com.huawei.streaming.application.StreamAdapter;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IEmitter;
import com.huawei.streaming.operator.IOperator;
import com.huawei.streaming.operator.IRichOperator;
import org.apache.gearpump.Message;
import org.apache.gearpump.streaming.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

public abstract class GearpumpAdapter implements StreamAdapter, Serializable
{
  private static final Logger LOG = LoggerFactory.getLogger(GearpumpTask.class);
  protected TaskContext context;
  protected IRichOperator operator;
  protected GearpumpEmitter emitter;

  public void setTaskContext(TaskContext context) {
    this.context = context;
  }

  @Override
  public void setOperator(IRichOperator operator)
  {
    this.operator = operator;
  }

  public void start() throws StreamingException
  {
    operator.initialize(createEmitters());
  }

  public abstract void process(Message message) throws StreamingException;

  public void close() throws StreamingException {
    operator.destroy();
  }

  private Map<String, IEmitter> createEmitters()
  {
    Map<String, IEmitter> emitters = Maps.newHashMap();
    emitter = new GearpumpEmitter(operator.getOutputStream(), context);
    emitters.put(operator.getOutputStream(), emitter);

    return emitters;
  }

}
