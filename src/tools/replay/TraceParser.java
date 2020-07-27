package tools.replay;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.LinkedList;

import som.Output;
import som.interpreter.actors.EventualMessage;
import som.vm.VmSettings;
import tools.concurrency.TracingActors.ReplayActor;
import tools.replay.ReplayData.EntityNode;
import tools.replay.ReplayData.Subtrace;
import tools.replay.nodes.RecordEventNodes;


public final class TraceParser implements Closeable {

  private final String baseTraceName;

  private final HashMap<Integer, FileChannel> traceChannels;
  private final LinkedList<FileInputStream>   openTraceStreams;
  private final HashMap<Long, EntityNode>     entities = new HashMap<>();

  private static final TraceRecord[] parseTable = createParseTable();

  private final int standardBufferSize;
  private int       lastParsedTrace;

  public TraceParser(final String traceName, final int standardBufferSize) {
    traceChannels = new HashMap<Integer, FileChannel>();
    openTraceStreams = new LinkedList<FileInputStream>();
    this.baseTraceName = traceName;
    this.standardBufferSize = standardBufferSize;

    if (VmSettings.SNAPSHOT_REPLAY) {
      lastParsedTrace = VmSettings.SNAPSHOT_REPLAY_VERSION;
    }
  }

  public TraceParser(final String traceName) {
    this(traceName, VmSettings.BUFFER_SIZE);
  }

  public String getTraceName(final int snapshotVersion) {
    return baseTraceName
        + (VmSettings.SNAPSHOTS_ENABLED ? "." + snapshotVersion : "");
    // return baseTraceName + "." + snapshotVersion;
  }

  public void initialize() {
    parseTrace(true, null, null, lastParsedTrace);
    parseExternalData(lastParsedTrace);
  }

  protected boolean scanNextTrace() {
    // need to avoid synchronization issues
    File traceFile = new File(getTraceName(lastParsedTrace + 1) + ".trace");
    if (!traceFile.exists()) {
      return false;
    }

    lastParsedTrace++;
    parseTrace(true, null, null, lastParsedTrace);
    parseExternalData(lastParsedTrace);
    return true;
  }

  @Override
  public void close() throws IOException {
    for (FileInputStream traceInputStream : openTraceStreams) {
      traceInputStream.close();
    }
  }

  protected HashMap<Long, EntityNode> getEntities() {
    return entities;
  }

  public ByteBuffer getExternalData(final long actorId, final int dataId) {
    long pos = entities.get(actorId).externalData.get(dataId);
    int snapshotVersion = lastParsedTrace;
    Output.println("external " + snapshotVersion);
    // TODO manage snapshto info for external data
    return readExternalData(pos, snapshotVersion);
  }

  public ByteBuffer getSystemCallData() {
    ReplayActor ra = (ReplayActor) EventualMessage.getActorCurrentMessageIsExecutionOn();
    ReplayRecord rr = ra.getNextReplayEvent();
    assert rr.type == TraceRecord.SYSTEM_CALL;
    ByteBuffer bb = getExternalData(ra.getId(), (int) rr.eventNo);
    return bb;
  }

  public int getIntegerSysCallResult() {
    return getSystemCallData().getInt();
  }

  public long getLongSysCallResult() {
    return getSystemCallData().getLong();
  }

  public double getDoubleSysCallResult() {
    return getSystemCallData().getDouble();
  }

  public String getStringSysCallResult() {
    return new String(getSystemCallData().array());
  }

  public LinkedList<ReplayRecord> getReplayEventsForEntity(final long replayId) {
    EntityNode entity = entities.get(replayId);

    // if (entity == null) {
    // entity.retrieved = true; return new LinkedList<>();
    // }
    if (entity == null) {
      assert scanNextTrace() : "Failed scanning next Trace";
      entity = entities.get(replayId);
      assert entity != null : "No Data for Activity " + replayId;
    }

    return entity.getReplayEvents();
  }

  public boolean getMoreEventsForEntity(final long replayId) {
    EntityNode entity = entities.get(replayId);
    // if (entity == null) {
    // return false;
    // }
    assert entity != null : "Missing Entity: " + replayId;
    return entity.parseContexts(this);
  }

  public int getNumberOfSubtraces(final long replayId) {
    EntityNode entity = entities.get(replayId);
    assert entity != null : "Missing Entity: " + replayId;
    return entity.subtraces.size();
  }

  private static TraceRecord[] createParseTable() {
    TraceRecord[] result = new TraceRecord[22];

    result[TraceRecord.ACTIVITY_CREATION.value] = TraceRecord.ACTIVITY_CREATION;
    result[TraceRecord.ACTIVITY_CONTEXT.value] = TraceRecord.ACTIVITY_CONTEXT;
    result[TraceRecord.MESSAGE.value] = TraceRecord.MESSAGE;
    result[TraceRecord.PROMISE_MESSAGE.value] = TraceRecord.PROMISE_MESSAGE;
    result[TraceRecord.SYSTEM_CALL.value] = TraceRecord.SYSTEM_CALL;
    result[TraceRecord.CHANNEL_READ.value] = TraceRecord.CHANNEL_READ;
    result[TraceRecord.CHANNEL_WRITE.value] = TraceRecord.CHANNEL_WRITE;

    result[TraceRecord.LOCK_ISLOCKED.value] = TraceRecord.LOCK_ISLOCKED;
    result[TraceRecord.CONDITION_TIMEOUT.value] =
        TraceRecord.CONDITION_TIMEOUT;

    result[TraceRecord.CHANNEL_READ.value] = TraceRecord.CHANNEL_READ;
    result[TraceRecord.CHANNEL_WRITE.value] = TraceRecord.CHANNEL_WRITE;
    result[TraceRecord.LOCK_LOCK.value] = TraceRecord.LOCK_LOCK;
    result[TraceRecord.CONDITION_WAKEUP.value] = TraceRecord.CONDITION_WAKEUP;
    result[TraceRecord.PROMISE_RESOLUTION.value] = TraceRecord.PROMISE_RESOLUTION;
    result[TraceRecord.PROMISE_CHAINED.value] = TraceRecord.PROMISE_CHAINED;
    result[TraceRecord.PROMISE_RESOLUTION_END.value] = TraceRecord.PROMISE_RESOLUTION_END;
    result[TraceRecord.TRANSACTION_COMMIT.value] = TraceRecord.TRANSACTION_COMMIT;

    return result;
  }

  public FileChannel getTraceChannel(final int snapshotVersion) {
    FileChannel traceChannel = traceChannels.get(snapshotVersion);

    if (traceChannel == null) {
      // new trace file needs to be loaded
      File traceFile = new File(getTraceName(snapshotVersion) + ".trace");

      try {
        Output.println("channel for " + traceFile.getAbsolutePath());
        FileInputStream traceInputStream = new FileInputStream(traceFile);
        openTraceStreams.add(traceInputStream);
        traceChannel = traceInputStream.getChannel();
        traceChannels.put(snapshotVersion, traceChannel);
      } catch (FileNotFoundException e) {
        throw new RuntimeException(
            "Attempted to open trace file '" + traceFile.getAbsolutePath() +
                "', but failed. ",
            e);
      }
    }

    return traceChannel;
  }

  private static final class EventParseContext {
    int        ordering;     // only used during scanning!
    EntityNode currentEntity;
    boolean    once;

    Subtrace entityLocation;

    final long startTime = System.currentTimeMillis();
    long[]     metrics   = new long[parseTable.length];

    EventParseContext(final EntityNode context) {
      this.currentEntity = context;
      this.once = true;
    }

    public long getMetric(final TraceRecord tr) {
      return metrics[tr.value];
    }
  }

  protected void parseTrace(final boolean scanning, final Subtrace loc,
      final EntityNode context, final int snapshotVersion) {
    final int parseLength = loc == null || loc.length == 0 ? standardBufferSize
        : Math.min(standardBufferSize, (int) loc.length);

    ByteBuffer b = ByteBuffer.allocate(parseLength);
    b.order(ByteOrder.LITTLE_ENDIAN);

    FileChannel traceChannel = getTraceChannel(snapshotVersion);

    if (scanning) {
      Output.println("Scanning Trace " + snapshotVersion + " ...");
    }

    EventParseContext ctx = new EventParseContext(context);

    try {
      b.clear();
      int nextReadPosition = loc == null ? 0 : (int) loc.startOffset;
      int startPosition = nextReadPosition;

      nextReadPosition = readFromChannel(b, nextReadPosition, traceChannel);

      final boolean readAll = loc != null && loc.length == nextReadPosition - startPosition;

      boolean first = true;

      while ((!readAll && nextReadPosition < traceChannel.size()) || b.hasRemaining()) {
        if (!readAll
            && b.remaining() < (1 + Long.BYTES * 3)
            && nextReadPosition < traceChannel.size()) {
          int remaining = b.remaining();
          startPosition += b.position();
          b.compact();
          nextReadPosition = readFromChannel(b, startPosition + remaining, traceChannel);
        }

        boolean done = readTrace(first, scanning, b, ctx, startPosition, nextReadPosition);
        assert !done : "think the return value here is not needed";
        if (done) {
          return;
        }

        if (first) {
          first = false;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (scanning) {
      long end = System.currentTimeMillis();
      Output.println("Trace with " + ctx.getMetric(TraceRecord.MESSAGE) + " Messages and "
          + ctx.getMetric(TraceRecord.ACTIVITY_CREATION)
          + " Activities sucessfully scanned in " + (end - ctx.startTime) + "ms !");
    }
  }

  private int readFromChannel(final ByteBuffer b, int currentPosition,
      final FileChannel traceChannel) throws IOException {
    int numBytesRead = traceChannel.read(b, currentPosition);
    currentPosition += numBytesRead;
    b.flip();
    return currentPosition;
  }

  private boolean readTrace(final boolean first, final boolean scanning, final ByteBuffer b,
      final EventParseContext ctx, final int startPosition, final int nextReadPosition) {

    final int start = b.position();
    final byte type = b.get();

    TraceRecord recordType = parseTable[type];

    if (!scanning && first) {
      assert recordType == TraceRecord.ACTIVITY_CONTEXT;
    }

    switch (recordType) {
      case ACTIVITY_CONTEXT:
        if (scanning) {
          if (ctx.currentEntity != null) {
            // new activity closes the last one, length of previous context is calculated
            ctx.entityLocation.length = startPosition + start - ctx.entityLocation.startOffset;
          }
          ctx.metrics[type]++;
          ctx.ordering = Short.toUnsignedInt(b.getShort());
          long currentEntityId = getId(b, Long.BYTES);

          ctx.currentEntity = getOrCreateEntityEntry(recordType, currentEntityId);
          assert ctx.currentEntity != null;
          Subtrace loc =
              ctx.currentEntity.registerContext(ctx.ordering, startPosition + start);
          loc.snapshot = lastParsedTrace;

          assert b.position() == start + 11;
          ctx.entityLocation = loc;
        } else {
          if (ctx.once) {
            ctx.ordering = Short.toUnsignedInt(b.getShort());
            getId(b, Long.BYTES);
            ctx.once = false;
          } else {
            // When we are not scanning for contexts, this means that the context we wanted
            // to process is over.
            return true;
          }
        }
        break;
      case SYSTEM_CALL:
        int did = b.getInt();
        if (!scanning) {
          ctx.currentEntity.addReplayEvent(
              new ReplayRecord(did, recordType));
        } else {
          ctx.metrics[type]++;
        }
        break;

      case CONDITION_TIMEOUT:
      case LOCK_ISLOCKED:
      case PROMISE_MESSAGE:
      case PROMISE_RESOLUTION:
      case PROMISE_RESOLUTION_END:
      case PROMISE_CHAINED:
      case MESSAGE:
      case CHANNEL_READ:
      case CHANNEL_WRITE:
      case LOCK_LOCK:
      case CONDITION_WAKEUP:
      case TRANSACTION_COMMIT:
      case ACTIVITY_CREATION:
        long eventData = b.getLong();

        if (!scanning) {
          ctx.currentEntity.addReplayEvent(
              new ReplayRecord(eventData, recordType));

        } else {
          ctx.metrics[type]++;
        }
        assert b.position() == start + RecordEventNodes.ONE_EVENT_SIZE;
        break;
      default:
        assert false;
    }

    return false;
  }

  private EntityNode getOrCreateEntityEntry(final TraceRecord type,
      final long entityId) {
    if (entities.containsKey(entityId)) {
      return entities.get(entityId);
    } else {
      EntityNode newNode = new EntityNode(entityId);
      entities.put(entityId, newNode);
      return newNode;
    }
  }

  private ByteBuffer readExternalData(final long position, final int snapshotVersion) {
    File traceFile = new File(getTraceName(snapshotVersion) + ".dat");
    try (FileInputStream fis = new FileInputStream(traceFile);
        FileChannel channel = fis.getChannel()) {

      ByteBuffer bb = ByteBuffer.allocate(16);
      bb.order(ByteOrder.LITTLE_ENDIAN);

      channel.read(bb, position);
      bb.flip();

      bb.getLong(); // actorId
      bb.getInt(); // dataId
      int len = bb.getInt();

      ByteBuffer res = ByteBuffer.allocate(len);
      res.order(ByteOrder.LITTLE_ENDIAN);
      channel.read(res, position + 16);
      res.flip();
      return res;
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void parseExternalData(final int snapshotVersion) {
    File traceFile = new File(getTraceName(snapshotVersion) + ".dat");
    // TODO

    ByteBuffer bb = ByteBuffer.allocate(16);
    bb.order(ByteOrder.LITTLE_ENDIAN);

    try (FileInputStream fis = new FileInputStream(traceFile);
        FileChannel channel = fis.getChannel()) {
      while (channel.position() < channel.size()) {
        // read from file if buffer is empty

        long position = channel.position();
        bb.clear();
        channel.read(bb);
        bb.flip();

        long actor = bb.getLong();
        int dataId = bb.getInt();
        int len = bb.getInt();

        EntityNode en = entities.get(actor);
        assert en != null;
        if (en.externalData == null) {
          en.externalData = new HashMap<>();
        }

        en.externalData.put(dataId, position);

        position = channel.position();
        channel.position(position + len);
      }

    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected void processContext(final Subtrace location, final EntityNode context) {
    parseTrace(false, location, context, location.snapshot);
  }

  private static long getId(final ByteBuffer b, final int numbytes) {
    switch (numbytes) {
      case 1:
        return 0 | b.get();
      case Short.BYTES:
        return 0 | b.getShort();
      case 3:
        return (b.get() << 16) | b.getShort();
      case Integer.BYTES:
        return b.getInt();
      case Long.BYTES:
        return b.getLong();
    }
    assert false : "should not happen";
    return 0;
  }
}
