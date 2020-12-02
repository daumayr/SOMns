package tools.snapshot.nodes;

import java.util.ArrayList;

import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.dsl.GenerateNodeFactory;
import com.oracle.truffle.api.dsl.Specialization;

import som.interpreter.actors.Actor;
import som.interpreter.actors.EventualMessage.PromiseMessage;
import som.interpreter.actors.SPromise;
import som.interpreter.actors.SPromise.Resolution;
import som.interpreter.actors.SPromise.SReplayPromise;
import som.interpreter.actors.SPromise.SResolver;
import som.interpreter.actors.SPromise.STracingPromise;
import som.primitives.ObjectPrims.ClassPrim;
import som.primitives.ObjectPrimsFactory.ClassPrimFactory;
import tools.concurrency.TracingActors.TracingActor;
import tools.snapshot.SnapshotBackend;
import tools.snapshot.SnapshotBuffer;
import tools.snapshot.SnapshotHeap;
import tools.snapshot.deserialization.DeserializationBuffer;
import tools.snapshot.deserialization.FixupInformation;


public abstract class PromiseSerializationNodes {
  private static final byte UNRESOLVED = 0;
  private static final byte CHAINED    = 1;
  private static final byte SUCCESS    = 2;
  private static final byte ERROR      = 3;

  public static void handleReferencedPromise(final SPromise prom,
      final SnapshotBuffer sb, final SnapshotHeap sh, final int location) {
    assert sb != null;
    assert sh.getOwner() != null;
    if (sh.getOwner() != null
        && prom.getOwner() == sh.getOwner().getCurrentActor()) {
      long promLocation = SPromise.getPromiseClass().serialize(prom, sh);
      sb.putLongAt(location, promLocation);
    } else {
      // The Promise belong to another Actor
      TracingActor ta = (TracingActor) prom.getOwner();
      ta.farReference(prom, sb, location);
    }
  }

  public static void ensurePromiseSerialized(final SPromise prom, final SnapshotHeap sh) {
    if (sh.getOwner() != null
        && prom.getOwner() == sh.getOwner().getCurrentActor()) {
      SPromise.getPromiseClass().serialize(prom, sh);
    } else {
      // The Promise belong to another Actor
      TracingActor ta = (TracingActor) prom.getOwner();
      ta.farReferenceNoFillIn(prom, sh.snapshotVersion);
    }
  }

  @GenerateNodeFactory
  public abstract static class PromiseSerializationNode extends AbstractSerializationNode {
    @Child ClassPrim classPrim = ClassPrimFactory.create(null);

    @Specialization(guards = "!prom.isCompleted()")
    public long doUnresolved(final STracingPromise prom, final SnapshotHeap sh) {
      long location = getObjectLocation(prom, sh.getSnapshotVersion());
      if (location != -1) {
        return location;
      }

      int ncp;
      int nwr;
      int noe;
      PromiseMessage whenRes;
      PromiseMessage onError;
      ArrayList<PromiseMessage> whenResExt;
      ArrayList<PromiseMessage> onErrorExt;
      SPromise chainedProm;
      ArrayList<SPromise> chainedPromExt;
      synchronized (prom) {
        chainedProm = prom.getChainedPromiseUnsync();
        chainedPromExt = prom.getChainedPromiseExtUnsync();
        ncp = getObjectCnt(chainedProm, chainedPromExt);

        whenRes = prom.getWhenResolvedUnsync();
        whenResExt = prom.getWhenResolvedExtUnsync();
        nwr = getObjectCnt(whenRes, whenResExt);

        onError = prom.getOnError();
        onErrorExt = prom.getOnErrorExtUnsync();
        noe = getObjectCnt(onError, onErrorExt);
      }

      SnapshotBuffer sb =
          sh.getBufferObject(1 + Long.BYTES + 10 + Long.BYTES * (noe + nwr + ncp));
      int start =
          sb.addObject(prom, SPromise.getPromiseClass(),
              1 + Long.BYTES + 10 + Long.BYTES * (noe + nwr + ncp));
      int base = start;

      // resolutionstate
      if (prom.getResolutionStateUnsync() == Resolution.CHAINED) {
        sb.putByteAt(base, CHAINED);
      } else {
        sb.putByteAt(base, UNRESOLVED);
      }

      sb.putLongAt(base + 1, ((TracingActor) prom.getOwner()).getId());
      base += 1 + Long.BYTES;
      base = serializeMessages(base, nwr, whenRes, whenResExt, sb);
      base = serializeMessages(base, noe, onError, onErrorExt, sb);
      serializeChainedPromises(base, ncp, chainedProm, chainedPromExt, sb);
      return sb.calculateReferenceB(start);
    }

    @Specialization(guards = {"prom.isCompleted()"})
    public long doResolved(final STracingPromise prom, final SnapshotHeap sh) {
      long location = getObjectLocation(prom, sh.getSnapshotVersion());
      if (location != -1) {
        return location;
      }

      SnapshotBuffer sb =
          sh.getBufferObject(1 + Long.BYTES + Long.BYTES + Long.BYTES + Long.BYTES);
      int start = sb.addObject(prom, SPromise.getPromiseClass(),
          1 + Long.BYTES + Long.BYTES + Long.BYTES + Long.BYTES);
      int base = start;

      // resolutionstate
      switch (prom.getResolutionStateUnsync()) {
        case SUCCESSFUL:
          sb.putByteAt(base, SUCCESS);
          break;
        case ERRONEOUS:
          sb.putByteAt(base, ERROR);
          break;
        default:
          throw new IllegalArgumentException("This shoud be unreachable");
      }

      Object value = prom.getValueForSnapshot();

      sb.putLongAt(base + 1, ((TracingActor) prom.getOwner()).getId());
      sb.putLongAt(base + 1 + Long.BYTES,
          classPrim.executeEvaluated(value).serialize(value, sh));
      sb.putLongAt(base + 1 + Long.BYTES + Long.BYTES,
          prom.getResolvingActor());
      sb.putLongAt(base + 1 + Long.BYTES + Long.BYTES + Long.BYTES,
          prom.getNextEventNumber());
      // base += (1 + Long.BYTES + Long.BYTES + Long.BYTES);

      return sb.calculateReferenceB(start);
    }

    private int getObjectCnt(final Object obj, final ArrayList<? extends Object> extension) {
      if (obj == null) {
        return 0;
      } else if (extension == null) {
        return 1;
      } else {
        return extension.size() + 1;
      }
    }

    private int serializeMessages(final int start, final int cnt,
        final PromiseMessage whenRes, final ArrayList<PromiseMessage> whenResExt,
        final SnapshotBuffer sb) {

      int base = start;
      int actualCnt = 0;
      assert cnt < Integer.MAX_VALUE : "Too many Messages" + cnt;

      base += 4;
      if (cnt > 0) {
        if (whenRes.getOriginalSnapshotPhase() != SnapshotBackend.getSnapshotVersion()) {
          if (whenRes.isDelivered()) {
            doDeliveredMessage(whenRes, base, sb);
          } else {
            sb.putLongAt(base, whenRes.forceSerialize(sb.getHeap()));
          }

          base += Long.BYTES;
          actualCnt++;
        }

        if (cnt > 1) {
          for (int i = 0; i < whenResExt.size(); i++) {
            PromiseMessage msg = whenResExt.get(i);

            if (msg.getOriginalSnapshotPhase() != SnapshotBackend.getSnapshotVersion()) {
              if (msg.isDelivered()) {
                doDeliveredMessage(msg, base, sb);
              } else {
                sb.putLongAt(base, msg.forceSerialize(sb.getHeap()));
              }
              base += Long.BYTES;
              actualCnt++;
            }
          }
        }
      }

      assert actualCnt <= cnt;
      sb.putIntAt(start, actualCnt);

      return base;
    }

    private void doDeliveredMessage(final PromiseMessage pm, final int location,
        final SnapshotBuffer sb) {
      assert pm.isDelivered();

      if (pm.getTarget() == sb.getHeap().getOwner().getCurrentActor()) {
        sb.putLongAt(location, pm.forceSerialize(sb.getHeap()));
      } else {
        TracingActor ta = (TracingActor) pm.getTarget();
        ta.farReferenceMessage(pm, sb, location);
      }
    }

    @TruffleBoundary // TODO: can we do better than a boundary?
    private void serializeChainedPromises(final int start, final int cnt,
        final SPromise chainedProm,
        final ArrayList<SPromise> chainedPromExt, final SnapshotBuffer sb) {
      int base = start;
      assert cnt < Short.MAX_VALUE;
      sb.putShortAt(base, (short) cnt);
      base += 2;
      if (cnt > 0) {
        handleReferencedPromise(chainedProm, sb, sb.getHeap(), base);
        base += Long.BYTES;

        if (cnt > 1) {
          for (int i = 0; i < chainedPromExt.size(); i++) {
            SPromise prom = chainedPromExt.get(i);
            handleReferencedPromise(prom, sb, sb.getHeap(), base + i * Long.BYTES);
          }
        }
      }
    }

    @Override
    public SPromise deserialize(final DeserializationBuffer sb) {
      byte state = sb.get();
      assert state >= 0 && state <= 3;
      if (state == SUCCESS || state == ERROR) {
        return deserializeCompletedPromise(state, sb);
      } else {
        return deserializeUnresolvedPromise(sb);
      }
    }

    private SPromise deserializeCompletedPromise(final byte state,
        final DeserializationBuffer sb) {
      long ownerId = sb.getLong();
      Actor owner = SnapshotBackend.lookupActor(ownerId);
      Object value = sb.getReference();
      long resolver = sb.getLong();
      long version = sb.getLong();

      SPromise p = SPromise.createResolved(owner, value,
          state == SUCCESS ? Resolution.SUCCESSFUL : Resolution.ERRONEOUS, 0);
      ((STracingPromise) p).setResolvingActorForSnapshot(resolver);
      if (DeserializationBuffer.needsFixup(value)) {
        sb.installFixup(new PromiseValueFixup(p));
      }

      return p;
    }

    private SPromise deserializeUnresolvedPromise(final DeserializationBuffer sb) {
      long ownerId = sb.getLong();
      Actor owner = SnapshotBackend.lookupActor(ownerId);
      SPromise promise = SPromise.createPromise(owner, false, false, null);

      // These messages aren't referenced by anything else, no need for fixup
      int whenResolvedCnt = sb.getInt();
      for (int i = 0; i < whenResolvedCnt; i++) {
        PromiseMessage pm = (PromiseMessage) sb.getReference();
        pm.setIdSnapshot(i);
        ((SReplayPromise) promise).registerOnResolvedSnapshot(pm, true);
      }

      int onErrorCnt = sb.getInt();
      for (int i = 0; i < onErrorCnt; i++) {
        PromiseMessage pm = (PromiseMessage) sb.getReference();
        pm.setIdSnapshot(i);
        ((SReplayPromise) promise).registerOnErrorSnapshot(pm, true);
      }

      int chainedPromCnt = sb.getShort();
      for (int i = 0; i < chainedPromCnt; i++) {
        SPromise remote = (SPromise) sb.getReference();
        // TODO set priority
        ((SReplayPromise) promise).registerChainedPromiseSnapshot((SReplayPromise) remote,
            true);
      }

      return promise;
    }

    private static void initialiseChainedPromise(final STracingPromise p,
        final SPromise remote) {
      boolean complete = remote.isCompleted();
      long resolver = p.getResolvingActor();

      p.addChainedPromise(remote);
      remote.resolveFromSnapshot(p.getValueForSnapshot(), p.getResolutionStateUnsync(),
          SnapshotBackend.lookupActor(resolver), !complete);
      ((STracingPromise) remote).setResolvingActorForSnapshot(resolver);
    }

    public static class PromiseValueFixup extends FixupInformation {
      private final SPromise promise;

      public PromiseValueFixup(final SPromise promise) {
        this.promise = promise;
      }

      @Override
      public void fixUp(final Object o) {
        promise.setValueFromSnapshot(o);
      }
    }

    public static class ChainedPromiseFixup extends FixupInformation {
      private final STracingPromise parent;

      public ChainedPromiseFixup(final STracingPromise parent) {
        this.parent = parent;
      }

      @Override
      public void fixUp(final Object o) {
        initialiseChainedPromise(parent, (SPromise) o);
      }
    }
  }

  /**
   * Resolvers are values and can be passed directly without being wrapped.
   * There is only a single resolver for every promise. But promises may be chained.
   * Resolver contains a reference to the promise, which knows it's owner.
   * If Identity of Resolvers becomes an issue, just add the actor information and turn into a
   * singleton when deserializing
   */
  @GenerateNodeFactory
  public abstract static class ResolverSerializationNode extends AbstractSerializationNode {

    @Specialization
    public long doResolver(final SResolver resolver, final SnapshotHeap sh) {

      long location = getObjectLocation(resolver, sh.getSnapshotVersion());
      if (location != -1) {
        return location;
      }

      SnapshotBuffer vb = sh.getBufferObject(Long.BYTES);
      int base = vb.addObject(resolver, SResolver.getResolverClass(),
          Long.BYTES);
      SPromise prom = resolver.getPromise();
      handleReferencedPromise(prom, vb, sh, base);
      return vb.calculateReferenceB(base);
    }

    @Override
    public SResolver deserialize(final DeserializationBuffer bb) {
      SPromise prom = (SPromise) bb.getReference();
      return SPromise.createResolver(prom);
    }
  }
}