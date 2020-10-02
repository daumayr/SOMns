package tools.replay.nodes;

import com.oracle.truffle.api.dsl.GenerateNodeFactory;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.Node;

import som.interpreter.actors.EventualMessage;
import som.interpreter.actors.EventualMessage.DirectMessage;
import som.interpreter.actors.EventualMessage.PromiseMessage;
import tools.concurrency.TracingActors.TracingActor;
import tools.replay.TraceRecord;
import tools.replay.nodes.RecordEventNodes.RecordOneEvent;


@GenerateNodeFactory
public abstract class RecordMessageReceivedNode extends Node {
  @Child protected RecordOneEvent message = new RecordOneEvent(TraceRecord.MESSAGE);
  @Child protected RecordOneEvent promise = new RecordOneEvent(TraceRecord.PROMISE_MESSAGE);

  public abstract void execute(EventualMessage msg);

  @Specialization
  protected void doMessage(final DirectMessage msg) {
    message.record(((TracingActor) msg.getSender()).getId());
  }

  @Specialization
  protected void doPromiseMessage(final PromiseMessage msg) {
    promise.record(msg.getMessageId());
    message.record(((TracingActor) msg.getSender()).getId());
  }

  @Specialization
  protected void doGeneric(final EventualMessage msg) {
    if (msg instanceof PromiseMessage) {
      promise.record(msg.getMessageId());
    }
    message.record(((TracingActor) msg.getSender()).getId());
  }
}
