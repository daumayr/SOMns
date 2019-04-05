package tools.snapshot;

import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;

import som.interpreter.actors.EventualMessage;
import som.vm.VmSettings;
import som.vm.constants.Classes;
import som.vmobjects.SAbstractObject;
import som.vmobjects.SClass;
import tools.concurrency.TraceBuffer;
import tools.concurrency.TracingActors.TracingActor;
import tools.replay.nodes.TraceContextNode;
import tools.snapshot.deserialization.DeserializationBuffer;


public class SnapshotBuffer extends TraceBuffer {

  public static final int FIELD_SIZE    = 8;
  public static final int CLASS_ID_SIZE = 4;
  public static final int MAX_FIELD_CNT = Byte.MAX_VALUE;
  public static final int THREAD_SHIFT  = Long.SIZE - Short.SIZE;

  protected final int          offset;
  protected final SnapshotHeap parent;

  public SnapshotBuffer(final SnapshotHeap parent,
      final int offset) {
    super(VmSettings.BUFFER_SIZE);
    this.offset = offset;
    this.parent = parent;
  }

  public SnapshotBuffer(final byte snapshotVersion, final SnapshotHeap parent,
      final int offset) {
    super(VmSettings.BUFFER_SIZE);
    this.offset = offset;
    this.parent = parent;
  }

  public final long calculateReference(final long start) {
    assert start != -1;
    return (parent.threadId << THREAD_SHIFT) | (offset + start);
  }

  public final long calculateReferenceB(final long start) {
    assert start != -1;
    return (parent.threadId << THREAD_SHIFT) | ((offset + start) - Integer.BYTES);
  }

  public int reserveSpace(final int bytes) {
    int oldPos = this.position;
    this.position += bytes;
    return oldPos;
  }

  public int addObject(final SAbstractObject o, final SClass clazz, final int payload) {
    assert (o.getSnapshotLocation() == -1
        || o.getSnapshotVersion() != parent.getSnapshotVersion()) : "Object serialized multiple times";

    int oldPos = this.position;
    o.updateSnapshotLocation(calculateReference(oldPos), parent.getSnapshotVersion());
    this.putIntAt(this.position, clazz.getIdentity());
    this.position += CLASS_ID_SIZE + payload;

    if (clazz.getSOMClass() != Classes.classClass) {
      TracingActor owner = clazz.getOwnerOfOuter();
      if (owner == null) {
        // owner = (TracingActor) SomLanguage.getCurrent().getVM().getMainActor();
        serializeWithBoundary(clazz);
      } else {
        assert owner != null;
        owner.farReference(clazz, null, 0);
      }
    }

    return oldPos + CLASS_ID_SIZE;
  }

  public SnapshotHeap getHeap() {
    return parent;
  }

  public int addValueObject(final SAbstractObject o, final SClass clazz, final int payload) {
    assert !SnapshotBackend.getValuepool().containsKey(o) : "Object serialized multiple times";

    int oldPos = this.position;
    o.updateSnapshotLocation(calculateReference(oldPos), parent.getSnapshotVersion());
    this.putIntAt(this.position, clazz.getIdentity());
    this.position += CLASS_ID_SIZE + payload;

    if (clazz.getSOMClass() != Classes.classClass) {
      TracingActor owner = clazz.getOwnerOfOuter();
      if (owner == null) {
        // owner = (TracingActor) SomLanguage.getCurrent().getVM().getMainActor();
        serializeWithBoundary(clazz);
      } else {
        assert owner != null;
        owner.farReference(clazz, null, 0);
      }
    }

    return oldPos + CLASS_ID_SIZE;
  }

  public int addValue(final Object o, final SClass clazz, final int payload) {
    assert !SnapshotBackend.getValuepool().containsKey(o) : "Object serialized multiple times";

    int oldPos = this.position;

    synchronized (SnapshotBackend.getValuepool()) {
      SnapshotBackend.getValuepool().put(o, calculateReference(oldPos));
    }
    this.putIntAt(this.position, clazz.getIdentity());
    this.position += CLASS_ID_SIZE + payload;

    if (clazz.getSOMClass() != Classes.classClass) {
      TracingActor owner = clazz.getOwnerOfOuter();
      if (owner == null) {
        // owner = (TracingActor) SomLanguage.getCurrent().getVM().getMainActor();
        serializeWithBoundary(clazz);
      } else {
        assert owner != null;
        owner.farReference(clazz, null, 0);
      }
    }

    return oldPos + CLASS_ID_SIZE;
  }

  @TruffleBoundary
  private void serializeWithBoundary(final SClass clazz) {
    clazz.getSOMClass().serialize(clazz, parent);
  }

  public int getSize() {
    return buffer.length;
  }

  public int addMessage(final int payload, final EventualMessage msg) {
    // we dont put messages into our lookup table as there should be only one reference to it
    // (either from a promise or a mailbox)
    assert (msg.getSnapshotLocation() == -1
        || msg.getSnapshotVersion() != parent.getSnapshotVersion()) : "Message serialized multiple times";

    int oldPos = this.position;
    msg.updateSnapshotLocation(calculateReference(oldPos), parent.getSnapshotVersion());
    // owner.addMessageLocation(ta.getActorId(), calculateReference(oldPos));

    this.putIntAt(this.position, Classes.messageClass.getIdentity());
    this.position += CLASS_ID_SIZE + payload;
    return oldPos + CLASS_ID_SIZE;
  }

  @Override
  protected void swapBufferWhenNotEnoughSpace(final TraceContextNode tracer) {
    throw new UnsupportedOperationException("TODO find a solution for snapshot size");
  }

  // for testing purposes
  public DeserializationBuffer getBuffer() {
    return new DeserializationBuffer(buffer);
  }

  public byte[] getRawBuffer() {
    return this.buffer;
  }
}
