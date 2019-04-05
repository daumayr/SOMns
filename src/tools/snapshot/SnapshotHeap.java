package tools.snapshot;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import com.oracle.truffle.api.CompilerDirectives;

import som.interpreter.actors.Actor.ActorProcessingThread;
import som.vm.VmSettings;
import tools.concurrency.TracingActivityThread;
import tools.concurrency.TracingActors.TracingActor;


public class SnapshotHeap {
  SnapshotBuffer             current;
  LinkedList<SnapshotBuffer> bufferStorage;
  ActorProcessingThread      owner;
  int                        size;
  public final long          threadId;
  public byte                snapshotVersion;

  public SnapshotHeap(final ActorProcessingThread actorProcessingThread) {
    this.owner = actorProcessingThread;
    this.threadId = owner.getThreadId();
    this.snapshotVersion = owner.getSnapshotId();
    current = new SnapshotBuffer(this, 0);
    bufferStorage = new LinkedList<>();
    bufferStorage.addFirst(current);
  }

  public SnapshotHeap(final byte version) {
    this.owner = null;
    this.threadId = TracingActivityThread.threadIdGen.getAndIncrement();
    this.snapshotVersion = version;
    current = new SnapshotBuffer(version, this, 0);
    bufferStorage = new LinkedList<>();
    bufferStorage.addFirst(current);
  }

  public TracingActor getActor() {
    return CompilerDirectives.castExact(owner.getCurrentActor(), TracingActor.class);
  }

  public ActorProcessingThread getOwner() {
    return owner;
  }

  /**
   * reserves size bytes and returns the buffer to use
   *
   * @param size
   * @return
   */
  public SnapshotBuffer getBuffer(final int size) {
    if ((current.getSize() - current.position()) <= size) {
      this.size += current.position();
      current = new SnapshotBuffer(this, this.size);
      bufferStorage.add(current);
    }

    return current;
  }

  public SnapshotBuffer getBufferObject(final int size) {
    if ((current.getSize() - current.position()) <= (size + Integer.BYTES)) {
      this.size += current.position();
      current = new SnapshotBuffer(this, this.size);
      bufferStorage.add(current);
    }

    return current;
  }

  public void writeToChannel(final FileOutputStream fos) throws IOException {

    for (SnapshotBuffer sb : bufferStorage) {
      fos.getChannel().write(ByteBuffer.wrap(sb.getRawBuffer(), 0, sb.position()));
      fos.flush();
    }
  }

  public int size() {
    return size + current.position();
  }

  public byte getSnapshotVersion() {
    return snapshotVersion;
  }

  public boolean needsToBeSnapshot(final long messageId) {

    return VmSettings.TEST_SNAPSHOTS || VmSettings.TEST_SERIALIZE_ALL
        || snapshotVersion > messageId;
  }

}
