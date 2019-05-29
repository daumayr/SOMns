package tools.parser;

import java.awt.*;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.List;

import tools.debugger.entities.Marker;

public class KomposTraceParser {

    private static final int SOURCE_SECTION_SIZE = 8;

    private enum TraceRecords {
        ActivityCreation(11 + SOURCE_SECTION_SIZE),
        ActivityCompletion(1),
        DynamicScopeStart(9 + SOURCE_SECTION_SIZE),
        DynamicScopeEnd(1),
        PassiveEntityCreation(9 + SOURCE_SECTION_SIZE),
        PassiveEntityCompletion(0),
        SendOp(17),
        ReceiveOp(9),
        ImplThread(9),
        ImplThreadCurrentActivity(13);

        private int byteSize;

        TraceRecords(int byteSize) {
            this.byteSize = byteSize;
        }

        public int getByteSize() {
            return byteSize;
        }
    }

    private final List<Long> turns = new ArrayList<Long>();
    private final TraceRecords[] parseTable = createParseTable();
    private final ByteBuffer byteBuffer = ByteBuffer.allocate(2048);

    private final HashMap<Long, MsgObj> messages = new HashMap<Long, MsgObj>();

    private final List<Long> toResolvePromises = new ArrayList<Long>();

    public KomposTraceParser() {
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    private TraceRecords[] createParseTable() {
        TraceRecords[] result = new TraceRecords[23];

        result[Marker.PROCESS_CREATION] = TraceRecords.ActivityCreation;
        result[Marker.PROCESS_COMPLETION] = TraceRecords.ActivityCompletion;
        result[Marker.ACTOR_CREATION] = TraceRecords.ActivityCreation;
        result[Marker.TASK_SPAWN] = TraceRecords.ActivityCreation;
        result[Marker.THREAD_SPAWN] = TraceRecords.ActivityCreation;

        result[Marker.ACTOR_MSG_SEND] = TraceRecords.SendOp;
        result[Marker.PROMISE_MSG_SEND] = TraceRecords.SendOp;
        result[Marker.CHANNEL_MSG_SEND] = TraceRecords.SendOp;
        result[Marker.PROMISE_RESOLUTION] = TraceRecords.SendOp;

        result[Marker.CHANNEL_MSG_RCV] = TraceRecords.ReceiveOp;
        result[Marker.TASK_JOIN] = TraceRecords.ReceiveOp;
        result[Marker.THREAD_JOIN] = TraceRecords.ReceiveOp;

        result[Marker.TURN_START] = TraceRecords.DynamicScopeStart;
        result[Marker.TURN_END] = TraceRecords.DynamicScopeEnd;
        result[Marker.MONITOR_ENTER] = TraceRecords.DynamicScopeStart;
        result[Marker.MONITOR_EXIT] = TraceRecords.DynamicScopeEnd;
        result[Marker.TRANSACTION_START] = TraceRecords.DynamicScopeStart;
        result[Marker.TRANSACTION_END] = TraceRecords.DynamicScopeEnd;

        result[Marker.CHANNEL_CREATION] = TraceRecords.PassiveEntityCreation;
        result[Marker.PROMISE_CREATION] = TraceRecords.PassiveEntityCreation;

        result[Marker.IMPL_THREAD] = TraceRecords.ImplThread;
        result[Marker.IMPL_THREAD_CURRENT_ACTIVITY] = TraceRecords.ImplThreadCurrentActivity;

        return result;
    }

    private void parse(String path) {
        File traceFile = new File(path);

        long currentTurn = -1;
        try {
            FileInputStream fis = new FileInputStream(traceFile);
            FileChannel channel = fis.getChannel();

            channel.read(byteBuffer);
            byteBuffer.flip();

            long currentActivityId = -1;


            while(channel.position() < channel.size() || byteBuffer.remaining() > 0) {
                if (!byteBuffer.hasRemaining()) {
                    byteBuffer.clear();
                    channel.read(byteBuffer);
                    byteBuffer.flip();
                } else if (byteBuffer.remaining() < 20) {
                    byteBuffer.compact();
                    channel.read(byteBuffer);
                    byteBuffer.flip();
                }

                final int start = byteBuffer.position();
                final byte type = byteBuffer.get();

                TraceRecords recordType = parseTable[type];

                switch (recordType) {
                    case ActivityCreation:
                        long activityId = byteBuffer.getLong();
                        short symboldId = byteBuffer.getShort();
                        readSourceSection();
                        assert byteBuffer.position() == start + (TraceRecords.ActivityCreation.getByteSize());
                        break;
                    case ActivityCompletion:
                        assert byteBuffer.position() == start + (TraceRecords.ActivityCompletion.getByteSize());
                        break;
                    case DynamicScopeStart:
                        long id = byteBuffer.getLong();
                        turns.add(id);
                        if(type == Marker.TURN_START) {
                            currentTurn = id;
                            if(toResolvePromises.contains(id)) {
                                MsgObj message = messages.get(id);
                                message.receiverId = currentActivityId;
                                toResolvePromises.remove(id);
                            }
                        }

                        readSourceSection();
                        assert byteBuffer.position() == start + (TraceRecords.DynamicScopeStart.getByteSize());
                        break;
                    case DynamicScopeEnd:
                        assert byteBuffer.position() == start + (TraceRecords.DynamicScopeEnd.getByteSize());
                        break;
                    case SendOp:
                        long entityId = byteBuffer.getLong();
                        long targetId = byteBuffer.getLong();

                        if (type == Marker.ACTOR_MSG_SEND) {
                            messages.put(entityId, new MsgObj(entityId ,currentActivityId, targetId, currentTurn));
                        }

                        if(type == Marker.PROMISE_MSG_SEND) {
                            messages.put(entityId, new PromiseObj(entityId, currentActivityId, currentTurn, targetId));
                            // Need to retrieve Target from turn
                            toResolvePromises.add(entityId);
                        }

                        assert byteBuffer.position() == start + (TraceRecords.SendOp.getByteSize());
                        break;
                    case ReceiveOp:
                        long sourceId = byteBuffer.getLong();
                        assert byteBuffer.position() == start + (TraceRecords.ReceiveOp.getByteSize());
                    case PassiveEntityCreation:
                        id = byteBuffer.getLong();
                        readSourceSection();
                        assert byteBuffer.position() == start + (TraceRecords.PassiveEntityCreation.getByteSize());
                        break;
                    case PassiveEntityCompletion:
                        assert byteBuffer.position() == start + (TraceRecords.PassiveEntityCompletion.getByteSize());
                        break;
                    case ImplThread:
                        long currentImplThreadId = byteBuffer.getLong();
                        assert byteBuffer.position() == start + (TraceRecords.ImplThread.getByteSize());
                        break;
                    case ImplThreadCurrentActivity:
                        currentActivityId = byteBuffer.getLong();
                        int currentActivityBufferId = byteBuffer.getInt();
                        assert byteBuffer.position() == start + (TraceRecords.ImplThreadCurrentActivity.getByteSize());
                        break;
                    default:
                        assert false;
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<MsgObj> getStrackTraceOfMessage(long messageId) {
        List<MsgObj> stackTrace = new ArrayList<MsgObj>();

        if (!messages.containsKey(messageId)) {
            throw new IllegalArgumentException();
        }

        MsgObj message = messages.get(messageId);
        stackTrace.add(message);

        while(message.parentMsgId != -1) {
            if (messages.containsKey(message.parentMsgId)) {
                message = messages.get(message.parentMsgId);
                stackTrace.add(message);
            } else {
                System.out.println("Message not found?!");
                break;
            }
        }

        return stackTrace;
    }

    public void createStackTraceFile(String path) {
        File errorMsgFile = new File(path + "_errorMsgId.trace");

        if(errorMsgFile.exists() && !errorMsgFile.isDirectory()) {
            long errorMsgId = -1;

            try (Scanner sc = new Scanner(errorMsgFile)){
                errorMsgId = sc.nextLong();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

            if(errorMsgId != -1) {
                parse(path + ".trace");
                List<MsgObj> stackTrace = getStrackTraceOfMessage(errorMsgId);
                File stackTraceFile = new File(path + "_errorStack.trace");

                try(BufferedWriter writer = new BufferedWriter(new FileWriter(stackTraceFile))) {
                    for (MsgObj msg:stackTrace) {
                        writer.write(String.valueOf(msg.messageId) + "\n");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void readSourceSection() {
        short fileId = byteBuffer.getShort();
        short startLine = byteBuffer.getShort();
        short startCol = byteBuffer.getShort();
        short charLen = byteBuffer.getShort();
    }
}
