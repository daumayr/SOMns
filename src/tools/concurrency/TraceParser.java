package tools.concurrency;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Queue;

import som.Output;
import som.vm.VmSettings;
import som.vmobjects.SSymbol;


public final class TraceParser {

  /**
   * Types of elements in the trace, used to create a parse table.
   */
  private enum TraceRecord {
    ACTOR_CREATION,
    ACTOR_CONTEXT,
    MESSAGE,
    PROMISE_MESSAGE,
    SYSTEM_CALL
  }

  private final HashMap<Short, SSymbol>     symbolMapping = new HashMap<>();
  private ByteBuffer                        b             =
      ByteBuffer.allocate(TracingBackend.BUFFER_SIZE);
  private final HashMap<Integer, ActorNode> actors        = new HashMap<>();

  private long parsedMessages = 0;
  private long parsedActors   = 0;

  private static TraceParser parser;

  private final TraceRecord[] parseTable;

  public byte[] getExternalData(final int actorId, final int dataId) {
    return null;
  }

  public boolean getBooleanSysCallResult() {
    return false;
  }

  public int getIntegerSysCallResult() {
    return 0;
  }

  public String getStringSysCallResult() {
    return null;
  }

  public static synchronized Queue<MessageRecord> getExpectedMessages(final int replayId) {
    if (parser == null) {
      parser = new TraceParser();
      parser.parseTrace();
    }

    return parser.actors.get(replayId).getExpectedMessages();
  }

  public static synchronized int getReplayId(final int parentId, final int childNo) {
    if (parser == null) {
      parser = new TraceParser();
      parser.parseTrace();
    }
    assert parser.actors.containsKey(parentId) : "Parent doesn't exist";
    return (int) parser.actors.get(parentId).getChild(childNo).actorId;
  }

  private TraceParser() {
    assert VmSettings.REPLAY;
    this.parseTable = createParseTable();
    b.order(ByteOrder.LITTLE_ENDIAN);
  }

  private TraceRecord[] createParseTable() {
    TraceRecord[] result = new TraceRecord[8];

    result[ActorExecutionTrace.ACTOR_CREATION] = TraceRecord.ACTOR_CREATION;
    result[ActorExecutionTrace.ACTOR_CONTEXT] = TraceRecord.ACTOR_CONTEXT;
    result[ActorExecutionTrace.MESSAGE] = TraceRecord.MESSAGE;
    result[ActorExecutionTrace.PROMISE_MESSAGE] = TraceRecord.PROMISE_MESSAGE;
    result[ActorExecutionTrace.SYSTEM_CALL] = TraceRecord.SYSTEM_CALL;

    return result;
  }

  private void parseTrace() {
    boolean readMainActor = false;
    File traceFile = new File(VmSettings.TRACE_FILE + ".trace");

    int sender = 0;
    int resolver = 0;
    int currentActor = 0;
    int ordering = 0;
    int dataId;
    long startTime = System.currentTimeMillis();
    ArrayList<MessageRecord> contextMessages = null;

    Output.println("Parsing Trace ...");

    try (FileInputStream fis = new FileInputStream(traceFile);
        FileChannel channel = fis.getChannel()) {
      channel.read(b);
      b.flip(); // prepare for reading from buffer
      ActorNode current = null;

      while (channel.position() < channel.size() || b.remaining() > 0) {
        // read from file if buffer is empty
        if (!b.hasRemaining()) {
          b.clear();
          channel.read(b);
          b.flip();
        } else if (b.remaining() < 20) {
          b.compact();
          channel.read(b);
          b.flip();
        }

        final int start = b.position();
        final byte type = b.get();
        final int numbytes = ((type >> 4) & 3) + 1;
        boolean external = (type & 8) != 0;
        TraceRecord recordType = parseTable[type & 7];

        switch (recordType) {
          case ACTOR_CREATION:
            int newActorId = getId(numbytes);
            if (newActorId == 0) {
              assert !readMainActor : "There should be only one main actor.";
              readMainActor = true;
              if (!actors.containsKey(newActorId)) {
                actors.put(newActorId, new ActorNode(newActorId));
              }
            } else {
              if (!actors.containsKey(currentActor)) {
                actors.put(currentActor, new ActorNode(currentActor));
              }

              ActorNode node;
              if (actors.containsKey(newActorId)) {
                node = actors.get(newActorId);
              } else {
                node = new ActorNode(newActorId);
                actors.put(newActorId, node);
              }

              node.mailboxNo = ordering;
              actors.get(currentActor).addChild(node);
            }
            parsedActors++;

            assert b.position() == start + (numbytes + 1);
            break;

          case ACTOR_CONTEXT:
            /*
             * make two buckets, one for the current 256 contexs, and one for those that we
             * encounter prematurely
             * decision wher stuff goes is made based on bitset or whether htat ordering byte
             * already is used in the first bucket
             * when a bucket is full, we sort the contexts and put the messages inside a queue,
             * context can then be reclaimed by GC
             */

            ordering = Short.toUnsignedInt(b.getShort());
            currentActor = getId(numbytes);

            if (ordering == 1850 && currentActor == 132) {
              System.out.println("Found cluster on " + currentActor);
            }

            if (!actors.containsKey(currentActor)) {
              actors.put(currentActor, new ActorNode(currentActor));
            }
            current = actors.get(currentActor);
            assert current != null;
            contextMessages = null;
            assert b.position() == start + (numbytes + 2 + 1);
            break;
          case MESSAGE:
            parsedMessages++;
            if (contextMessages == null) {
              contextMessages = new ArrayList<>();
              assert actors.containsKey(currentActor);
              assert current != null;
              current.addMessageRecords(contextMessages, ordering);
            }
            assert contextMessages != null;

            sender = getId(numbytes);

            if (external) {
              System.out.println("EXTERNAL");
              b.getShort();
              b.getInt();
            }

            contextMessages.add(new MessageRecord(sender));
            assert b.position() == start + (numbytes + 1);
            break;
          case PROMISE_MESSAGE:
            parsedMessages++;
            if (contextMessages == null) {
              contextMessages = new ArrayList<>();
              assert actors.containsKey(currentActor);
              current.addMessageRecords(contextMessages, ordering);
            }
            sender = getId(numbytes);
            resolver = getId(numbytes);

            if (external) {
              System.out.println("EXTERNAL");
              b.getShort();
              b.getInt();
            }

            contextMessages.add(new PromiseMessageRecord(sender, resolver));
            assert b.position() == start + 1 + 2 * (numbytes);
            break;
          case SYSTEM_CALL:
            dataId = b.getInt();
            break;
          default:
            assert false;
        }
      }

    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    long end = System.currentTimeMillis();
    Output.println("Trace with " + parsedMessages + " Messages and " + parsedActors
        + " Actors sucessfully parsed in " + (end - startTime) + "ms !");
  }

  private int getId(final int numbytes) {
    switch (numbytes) {
      case 1:
        return 0 | b.get();
      case 2:
        return 0 | b.getShort();
      case 3:
        return (b.get() << 16) | b.getShort();
      case 4:
        return b.getInt();
    }
    assert false : "should not happen";
    return 0;
  }

  /**
   * Node in actor creation hierarchy.
   */
  private static class ActorNode implements Comparable<ActorNode> {
    final long                                 actorId;
    int                                        childNo;
    int                                        mailboxNo;
    boolean                                    sorted           = false;
    ArrayList<ActorNode>                       children;
    HashMap<Integer, ArrayList<MessageRecord>> bucket1          = new HashMap<>();
    HashMap<Integer, ArrayList<MessageRecord>> bucket2          = new HashMap<>();
    Queue<MessageRecord>                       expectedMessages = new java.util.LinkedList<>();
    int                                        max              = 0;
    int                                        max2             = 0;

    ActorNode(final long actorId) {
      super();
      this.actorId = actorId;
    }

    @Override
    public int compareTo(final ActorNode o) {
      int i = Integer.compare(mailboxNo, o.mailboxNo);
      if (i == 0) {
        i = Integer.compare(childNo, o.childNo);
      }
      if (i == 0) {
        i = Long.compare(actorId, o.actorId);
      }

      return i;
    }

    private void addChild(final ActorNode child) {
      if (children == null) {
        children = new ArrayList<>();
      }

      child.childNo = children.size();
      children.add(child);
    }

    protected ActorNode getChild(final int childNo) {
      assert children != null : "Actor does not exist in trace!";
      assert children.size() > childNo : "Actor does not exist in trace!";

      if (!sorted) {
        Collections.sort(children);
        sorted = true;
      }

      return children.get(childNo);
    }

    private void addMessageRecords(final ArrayList<MessageRecord> mr, final int order) {
      assert mr != null;
      // assert !bucket1.containsKey(order);
      // bucket1.put(order, mr);

      if (bucket1.containsKey(order)) {
        // use bucket two
        assert !bucket2.containsKey(order);
        bucket2.put(order, mr);
        max2 = Math.max(max2, order);
      } else {
        assert !bucket2.containsKey(order);
        bucket1.put(order, mr);
        max = Math.max(max, order);
        if (max == 0xFFFF) {
          // Bucket 1 is full, switch
          assert max2 < 0xEFFF;
          for (int i = 0; i <= max; i++) {
            if (!bucket1.containsKey(i)) {
              continue;
            }
            expectedMessages.addAll(bucket1.get(i));
          }
          bucket1.clear();
          HashMap<Integer, ArrayList<MessageRecord>> temp = bucket1;
          bucket1 = bucket2;
          bucket2 = temp;
          max = max2;
          max2 = 0;
        }
      }

    }

    public Queue<MessageRecord> getExpectedMessages() {

      assert bucket1.size() < 0xFFFF;
      assert bucket2.isEmpty();
      for (int i = 0; i <= max; i++) {
        if (bucket1.containsKey(i)) {
          expectedMessages.addAll(bucket1.get(i));
        } else {
          continue;
        }
      }

      /*
       * for (int i = 0; i <= max; i++) {
       * if (!bucket1.containsKey(i)) {
       * // System.out.println("Cluster " + i + " Missing in Actor" + this.actorId);
       * continue;
       * }
       * expectedMessages.addAll(bucket1.get(i));
       * }
       */

      return expectedMessages;
    }

    @Override
    public String toString() {
      return "" + actorId + ":" + childNo;
    }
  }

  public static class MessageRecord {
    public final int sender;
    // 0 means not external
    public final int extData;

    public MessageRecord(final int sender) {
      super();
      this.sender = sender;
      this.extData = 0;
    }

    public MessageRecord(final int sender, final int ext) {
      super();
      this.sender = sender;
      this.extData = ext;
    }

    public boolean isExternal() {
      return extData != 0;
    }
  }

  public static class PromiseMessageRecord extends MessageRecord {
    public int pId;

    public PromiseMessageRecord(final int sender, final int pId,
        final int ext) {
      super(sender, ext);
      this.pId = pId;
    }

    public PromiseMessageRecord(final int sender, final int pId) {
      super(sender);
      this.pId = pId;
    }
  }
}
