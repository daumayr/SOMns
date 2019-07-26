package tools.replay.nodes;

import com.oracle.truffle.api.dsl.Specialization;

import som.interpreter.actors.EventualMessage;
import som.interpreter.actors.EventualMessage.DirectMessage;
import som.interpreter.actors.EventualMessage.PromiseCallbackMessage;
import som.interpreter.actors.EventualMessage.PromiseSendMessage;
import som.interpreter.actors.SPromise.STracingPromise;
import tools.concurrency.RecordEventNodes.RecordOneEvent;
import tools.concurrency.RecordEventNodes.RecordThreeEvent;
import tools.concurrency.RecordEventNodes.RecordTwoEvent;
import tools.concurrency.TracingActors.TracingActor;
import tools.replay.actors.ActorExecutionTrace;
import tools.replay.actors.ExternalEventualMessage.ExternalDirectMessage;
import tools.replay.actors.ExternalEventualMessage.ExternalPromiseCallbackMessage;
import tools.replay.actors.ExternalEventualMessage.ExternalPromiseSendMessage;


public abstract class TraceMessageNode extends TraceNode {

  @Child TraceContextNode           tracer      = TraceContextNodeGen.create();
  @Child protected RecordOneEvent   recDMsg     =
      new RecordOneEvent(ActorExecutionTrace.MESSAGE);
  @Child protected RecordTwoEvent   recEDMsg    = new RecordTwoEvent(
      (byte) (ActorExecutionTrace.EXTERNAL_BIT | ActorExecutionTrace.MESSAGE));
  @Child protected RecordTwoEvent   recPromMsg  =
      new RecordTwoEvent(ActorExecutionTrace.PROMISE_MESSAGE);
  @Child protected RecordThreeEvent recEPromMsg = new RecordThreeEvent(
      (byte) (ActorExecutionTrace.EXTERNAL_BIT | ActorExecutionTrace.PROMISE_MESSAGE));

  public abstract void execute(EventualMessage msg);

  @Specialization
  public void trace(final DirectMessage msg) {
    recDMsg.record(((TracingActor) msg.getSender()).getActorId());
  }

  @Specialization
  public void trace(final ExternalDirectMessage msg) {
    long edata = (msg.getMethod() << Integer.BYTES) | msg.getDataId();
    recEDMsg.record(((TracingActor) msg.getSender()).getActorId(), edata);
  }

  @Specialization
  public void trace(final PromiseCallbackMessage msg) {
    recPromMsg.record(((TracingActor) msg.getSender()).getActorId(),
        ((STracingPromise) msg.getPromise()).getResolvingActor());
  }

  @Specialization
  public void trace(final PromiseSendMessage msg) {
    recPromMsg.record(((TracingActor) msg.getSender()).getActorId(),
        ((STracingPromise) msg.getPromise()).getResolvingActor());
  }

  @Specialization
  public void trace(final ExternalPromiseCallbackMessage msg) {
    long edata = (msg.getMethod() << Integer.BYTES) | msg.getDataId();
    recEPromMsg.record(((TracingActor) msg.getSender()).getActorId(),
        ((STracingPromise) msg.getPromise()).getResolvingActor(), edata);
  }

  @Specialization
  public void trace(final ExternalPromiseSendMessage msg) {
    long edata = (msg.getMethod() << Integer.BYTES) | msg.getDataId();
    recEPromMsg.record(((TracingActor) msg.getSender()).getActorId(),
        ((STracingPromise) msg.getPromise()).getResolvingActor(), edata);
  }
}
