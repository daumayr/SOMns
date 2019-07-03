package tools.snapshot;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.graalvm.collections.MapCursor;

import som.Output;
import som.vm.Symbols;
import som.vm.VmSettings;
import som.vmobjects.SSymbol;
import tools.snapshot.nodes.AbstractSerializationNode;


public class SnapshotWriterThread extends Thread {

  protected boolean                          cont              = true;
  private final ArrayBlockingQueue<Snapshot> finishedSnapshots =
      new ArrayBlockingQueue<>(5);

  public void addSnapshot(final Snapshot s) {
    finishedSnapshots.add(s);
  }

  private Snapshot tryToObtainSnapshot() {
    Snapshot snapshot;
    try {
      snapshot =
          finishedSnapshots.poll(VmSettings.BUFFER_TIMEOUT, TimeUnit.MILLISECONDS);
      return snapshot;
    } catch (InterruptedException e) {
      return null;
    }
  }

  @Override
  public void run() {
    while (cont || !finishedSnapshots.isEmpty() || !SnapshotBackend.snapshotDone()) {

      Snapshot s = tryToObtainSnapshot();
      if (s == null) {
        continue;
      }

      writeSnapshot(s);
    }
  }

  /**
   * Persist the current snapshot to a file.
   */

  public static void writeSnapshot(final Snapshot s) {
    if (s.buffers.size() == 0) {
      return;
    }
    // TODO fixate ValueBuffer for writing.

    s.buffers.add(s.valueBuffer);

    String name = VmSettings.TRACE_FILE + '.' + s.version;
    File f = new File(name + ".snap");
    f.getParentFile().mkdirs();

    try (FileOutputStream fos = new FileOutputStream(f)) {
      // Write Message Locations
      int offset = writeMessageLocations(s, fos);

      long location = AbstractSerializationNode.getObjectLocation(
          SnapshotBackend.resultPromise, s.version);
      assert location != -1;

      offset += writeClassEnclosures(s, fos);
      offset += writeLostResolutions(s, fos);

      writeSymbolTable();

      // WriteHeapMap
      writeHeapMap(s, fos, offset, location);

      // Write Heap
      while (!s.buffers.isEmpty()) {
        SnapshotHeap sh = s.buffers.poll();
        sh.writeToChannel(fos);
      }

      Output.println("Snapshotsize: " + fos.getChannel().size());

    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
  }

  // TODO: ValuePool may be modified while we persist it. Need to make this safe.
  private static int writeClassEnclosures(final Snapshot s, final FileOutputStream fos)
      throws IOException {
    int numClasses = s.classLocations.size() + s.valueBuffer.valueClassLocations.size();
    int size = (numClasses * 2 + 1) * Long.BYTES;
    ByteBuffer bb = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);

    bb.putLong(numClasses);

    MapCursor<Integer, Long> cursor = s.classLocations.getEntries();
    while (cursor.advance()) {
      bb.putLong(cursor.getKey());
      bb.putLong(cursor.getValue());
    }

    cursor = s.valueBuffer.valueClassLocations.getEntries();
    while (cursor.advance()) {
      bb.putLong(cursor.getKey());
      bb.putLong(cursor.getValue());
    }

    bb.rewind();
    fos.getChannel().write(bb);
    fos.flush();
    return size;
  }

  /**
   * This method creates a list that allows us to know where a {@link SnapshotBuffer} starts in
   * the file.
   */
  private static void writeHeapMap(final Snapshot s, final FileOutputStream fos,
      final int msgSize, final long resultLocation)
      throws IOException {
    // need to have a registry of the different heap areas
    int numBuffers = s.buffers.size();
    int registrySize = ((numBuffers * 2 + 2) * Long.BYTES);
    ByteBuffer bb = ByteBuffer.allocate(registrySize).order(ByteOrder.LITTLE_ENDIAN);
    // get and write location of the promise

    bb.putLong(resultLocation);

    bb.putLong(numBuffers);

    int bufferStart = msgSize + registrySize;
    for (SnapshotHeap sh : s.buffers) {
      long id = sh.threadId;
      bb.putLong(id);
      bb.putLong(bufferStart);
      bufferStart += sh.size();
    }

    bb.rewind();
    fos.getChannel().write(bb);
  }

  /**
   * This method persists the locations of messages in mailboxes, i.e. the roots of our object
   * graph
   */
  private static int writeMessageLocations(final Snapshot s, final FileOutputStream fos)
      throws IOException {
    int entryCount = 0;

    for (ArrayList<Long> al : s.messages) {
      assert al.size() % 2 == 0;
      entryCount += al.size();
    }
    Output.println("registered messages: " + (entryCount / 2));
    int msgSize = ((entryCount + 1) * Long.BYTES);
    ByteBuffer bb =
        ByteBuffer.allocate(msgSize).order(ByteOrder.LITTLE_ENDIAN);
    bb.putLong(entryCount);
    for (ArrayList<Long> al : s.messages) {
      for (long l : al) {
        bb.putLong(l);
      }
    }

    bb.rewind();
    fos.getChannel().write(bb);
    return msgSize;
  }

  private static int writeLostResolutions(final Snapshot s, final FileOutputStream fos)
      throws IOException {
    int entryCount = 0;
    for (ArrayList<Long> al : s.messages) {
      assert al.size() % 2 == 0;
      entryCount += al.size();
    }

    int sizeRes = (s.lostResolutions.size() + 1) * Long.BYTES;
    int sizeMsg = (s.lostMessages.size() + 1) * Long.BYTES;
    ByteBuffer bb =
        ByteBuffer.allocate(sizeRes + sizeMsg).order(ByteOrder.LITTLE_ENDIAN);
    bb.putLong(s.lostResolutions.size());
    for (long l : s.lostResolutions) {
      bb.putLong(l);
    }

    bb.putLong(s.lostMessages.size());
    for (long l : s.lostMessages) {
      bb.putLong(l);
    }

    bb.rewind();
    fos.getChannel().write(bb);
    return sizeRes + sizeMsg;
  }

  private static void writeSymbolTable() {
    Collection<SSymbol> symbols = Symbols.getSymbols();

    if (symbols.isEmpty()) {
      return;
    }
    File f = new File(VmSettings.TRACE_FILE + ".sym");

    try (FileOutputStream symbolStream = new FileOutputStream(f);
        BufferedWriter symbolWriter =
            new BufferedWriter(new OutputStreamWriter(symbolStream))) {

      for (SSymbol s : symbols) {
        symbolWriter.write(s.getSymbolId() + ":" + s.getString());
        symbolWriter.newLine();
      }
      symbolWriter.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
