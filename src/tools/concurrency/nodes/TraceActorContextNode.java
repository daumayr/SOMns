package tools.concurrency.nodes;

import tools.concurrency.ActorExecutionTrace;
import tools.concurrency.ActorExecutionTrace.ActorTraceBuffer;
import tools.concurrency.TracingActors.TracingActor;


public final class TraceActorContextNode extends TraceNode {

  @Child protected RecordIdNode id = RecordIdNodeGen.create();

  public void trace(final TracingActor actor) {
    ActorTraceBuffer buffer = getCurrentBuffer();
    int pos = buffer.position();

    int idLen = id.execute(storage, pos + 5, actor.getActorId());
    int idBit = (idLen - 1) << 4;

    storage.putByteAt(pos, (byte) (ActorExecutionTrace.ACTOR_CONTEXT | idBit));
    storage.putIntAt(pos + 1, actor.getOrdering());

    storage.position(pos + idLen + 1 + 4);
    actor.incrementOrdering();
  }

    buffer.position(pos + idLen + 1 + 2);
  }
}
