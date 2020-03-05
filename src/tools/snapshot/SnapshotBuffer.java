package tools.snapshot;

import com.oracle.truffle.api.CompilerDirectives;

import som.interpreter.SomLanguage;
import som.interpreter.actors.Actor.ActorProcessingThread;
import som.interpreter.actors.EventualMessage;
import som.vm.VmSettings;
import som.vm.constants.Classes;
import som.vmobjects.SAbstractObject;
import som.vmobjects.SClass;
import tools.concurrency.TraceBuffer;
import tools.concurrency.TracingActivityThread;
import tools.concurrency.TracingActors.TracingActor;
import tools.replay.nodes.TraceContextNode;
import tools.snapshot.deserialization.DeserializationBuffer;


public class SnapshotBuffer extends TraceBuffer {

  public static final int FIELD_SIZE    = 8;
  public static final int CLASS_ID_SIZE = 4;
  public static final int MAX_FIELD_CNT = Byte.MAX_VALUE;
  public static final int THREAD_SHIFT  = Long.SIZE - Short.SIZE;

  protected byte                        snapshotVersion;
  public final long                     threadId;
  protected final ActorProcessingThread owner;

  public SnapshotBuffer(final ActorProcessingThread owner) {
    super(VmSettings.BUFFER_SIZE * 25);
    this.owner = owner;
    this.threadId = owner.getThreadId();
    this.snapshotVersion = owner.getSnapshotId();
  }

  public SnapshotBuffer(final byte snapshotVersion) {
    super(VmSettings.BUFFER_SIZE * 25);
    this.owner = null;
    this.threadId = TracingActivityThread.threadIdGen.getAndIncrement();
    this.snapshotVersion = snapshotVersion;
  }

  public TracingActor getRecord() {
    return CompilerDirectives.castExact(owner.getCurrentActor(), TracingActor.class);
  }

  public ActorProcessingThread getOwner() {
    return owner;
  }

  public final long calculateReference(final long start) {
    assert start != -1;
    return (threadId << THREAD_SHIFT) | start;
  }

  public final long calculateReferenceB(final long start) {
    assert start != -1;
    return (threadId << THREAD_SHIFT) | (start - Integer.BYTES);
  }

  public int reserveSpace(final int bytes) {
    int oldPos = this.position;
    this.position += bytes;
    return oldPos;
  }

  public int addObject(final SAbstractObject o, final SClass clazz, final int payload) {
    assert (o.getSnapshotLocation() == -1
        || o.getSnapshotVersion() != snapshotVersion) : "Object serialized multiple times";

    int oldPos = this.position;
    o.updateSnapshotLocation(calculateReference(oldPos), snapshotVersion);

    if (clazz.getSOMClass() == Classes.classClass) {
      TracingActor owner = clazz.getOwnerOfOuter();
      if (owner == null) {
        owner = (TracingActor) SomLanguage.getCurrent().getVM().getMainActor();
      }

      assert owner != null;
      owner.farReference(clazz, null, 0);
    }
    this.putIntAt(this.position, clazz.getIdentity());
    this.position += CLASS_ID_SIZE + payload;
    return oldPos + CLASS_ID_SIZE;
  }

  public int addValueObject(final Object o, final SClass clazz, final int payload) {
    assert !SnapshotBackend.getValuepool().containsKey(o) : "Object serialized multiple times";

    int oldPos = this.position;

    synchronized (SnapshotBackend.getValuepool()) {
      SnapshotBackend.getValuepool().put(o, calculateReference(oldPos));
    }

    if (clazz.getSOMClass() == Classes.classClass) {
      TracingActor owner = clazz.getOwnerOfOuter();
      if (owner == null) {
        owner = (TracingActor) SomLanguage.getCurrent().getVM().getMainActor();
      }

      assert owner != null;
      owner.farReference(clazz, null, 0);
    }
    this.putIntAt(this.position, clazz.getIdentity());
    this.position += CLASS_ID_SIZE + payload;
    return oldPos + CLASS_ID_SIZE;
  }

  public int getSize() {
    return buffer.length;
  }

  public int addMessage(final int payload, final EventualMessage msg) {
    // we dont put messages into our lookup table as there should be only one reference to it
    // (either from a promise or a mailbox)
    assert (msg.getSnapshotLocation() == -1
        || msg.getSnapshotVersion() != snapshotVersion) : "Message serialized multiple times";

    int oldPos = this.position;
    msg.updateSnapshotLocation(calculateReference(oldPos), snapshotVersion);
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

  public Byte getSnapshotVersion() {
    return snapshotVersion;
  }

  public boolean needsToBeSnapshot(final long messageId) {
    return VmSettings.TEST_SNAPSHOTS || VmSettings.TEST_SERIALIZE_ALL
        || snapshotVersion > messageId;
  }

  public boolean changeBufferIfNecessary() {
    if (this.position >= (this.getRawBuffer().length / 2)) {
      this.buffer = new byte[this.buffer.length];
      this.position = 0;
      return true;
    }
    return false;
  }
}
