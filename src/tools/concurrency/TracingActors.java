package tools.concurrency;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;

import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;

import som.VM;
import som.interpreter.actors.Actor;
import som.interpreter.actors.EventualMessage;
import som.vm.VmSettings;
import tools.debugger.WebDebugger;
import tools.replay.PassiveEntityWithEvents;
import tools.replay.ReplayRecord;
import tools.replay.TraceParser;
import tools.snapshot.SnapshotRecord;
import tools.snapshot.deserialization.DeserializationBuffer;


public class TracingActors {
  public static class TracingActor extends Actor {
    protected final long     activityId;
    protected int            nextDataID;
    private int              traceBufferId;
    protected int            version;

    @CompilationFinal protected SnapshotRecord snapshotRecord;

    /**
     * Flag that indicates if a step-to-next-turn action has been made in the previous message.
     */
    protected boolean stepToNextTurn;

    public TracingActor(final VM vm) {
      super(vm);
      this.activityId = TracingActivityThread.newEntityId();
      this.version = 0;
      assert this.activityId >= 0;
      if (VmSettings.SNAPSHOTS_ENABLED) {
        snapshotRecord = new SnapshotRecord(this);
      }
    }

    protected TracingActor(final VM vm, final long id) {
      super(vm);
      this.activityId = id;
    }

    @Override
    public String toString() {
      return super.toString() + " #" + actorId;
    }

    public final int getActorId() {
      // TODO: remove after rebasing snapshot PR
      throw new UnsupportedOperationException("Please remove this call and use getId instead");
    }

    @Override
    @TruffleBoundary
    public synchronized void send(final EventualMessage msg,
        final ForkJoinPool actorPool) {
      super.send(msg, actorPool);
      if (VmSettings.UNIFORM_TRACING) {
        msg.getTracingNode().record(this.version);
        this.version++;
        // TODO maybe try to get the recording itself done outside the synchronized method
      }

    }

    @Override
    public synchronized void sendInitialStartMessage(final EventualMessage msg,
        final ForkJoinPool pool) {
      super.sendInitialStartMessage(msg, pool);

      if (VmSettings.UNIFORM_TRACING) {
        this.version++;
      }
    }

    @Override
    public long getId() {
      return activityId;
    }

    @Override
    public int getNextTraceBufferId() {
      return traceBufferId++;
    }

    @Override
    public synchronized int getDataId() {
      return nextDataID++;
    }

    public synchronized int peekDataId() {
      return nextDataID;
    }

    public TraceActorContextNode getActorContextNode() {
      return this.executor.getActorContextNode();
    }

    public boolean isStepToNextTurn() {
      return stepToNextTurn;
    }

    public SnapshotRecord getSnapshotRecord() {
      assert VmSettings.SNAPSHOTS_ENABLED;
      return snapshotRecord;
    }

    /**
     * For testing purposes.
     */
    public void replaceSnapshotRecord() {
      this.snapshotRecord = new SnapshotRecord(this);
    }

    @Override
    public void setStepToNextTurn(final boolean stepToNextTurn) {
      this.stepToNextTurn = stepToNextTurn;
    }

    public static void handleBreakpointsAndStepping(final EventualMessage msg,
        final WebDebugger dbg, final Actor actor) {
      if (msg.getHaltOnReceive() || ((TracingActor) actor).isStepToNextTurn()) {
        dbg.prepareSteppingUntilNextRootNode(Thread.currentThread());
        if (((TracingActor) actor).isStepToNextTurn()) { // reset flag
          actor.setStepToNextTurn(false);
        }
      }

      // check if a step-return-from-turn-to-promise-resolution has been triggered
      if (msg.getHaltOnPromiseMessageResolution()) {
        dbg.prepareSteppingUntilNextRootNode(Thread.currentThread());
      }
    }

    /**
     * To be Overrriden by ReplayActor.
     *
     * @return null
     */
    public DeserializationBuffer getDeserializationBuffer() {
      return null;
    }
  }

  public static final class ReplayActor extends TracingActor
      implements PassiveEntityWithEvents {
    protected int                                  children;
    private final LinkedList<ReplayRecord>         replayEvents;
    protected final PriorityQueue<EventualMessage> orderedMessages =
        new PriorityQueue<>(new MessageComparator());
    private static Map<Long, ReplayActor>          actorList;
    private BiConsumer<Short, Integer>             dataSource;
    public boolean                                 poisoned        = false;

    private final TraceParser traceParser;

    class MessageComparator implements Comparator<EventualMessage> {
      @Override
      public int compare(final EventualMessage o1, final EventualMessage o2) {
        return Long.compare(o1.getMessageId(), o2.getMessageId());
      }
    }

    static {
      if (VmSettings.REPLAY) {
        actorList = new HashMap<>();
      }
    }

    @Override
    public TraceParser getTraceParser() {
      return traceParser;
    }

    public BiConsumer<Short, Integer> getDataSource() {
      assert dataSource != null;
      return dataSource;
    }

    public void setDataSource(final ExternalDataSource ds) {
      if (dataSource != null) {
        throw new UnsupportedOperationException("Allready has a datasource!");
      }
      dataSource = ds;
    }

    @Override
    public LinkedList<ReplayRecord> getReplayEventBuffer() {
      return this.replayEvents;
    }

    public static ReplayActor getActorWithId(final long id) {
      return actorList.get(id);
    }

    @TruffleBoundary
    public ReplayActor(final VM vm) {
      super(vm);

      if (VmSettings.REPLAY) {
        replayEvents = vm.getTraceParser().getReplayEventsForEntity(activityId);

        if (VmSettings.SNAPSHOTS_ENABLED) {
          synchronized (actorList) {
            assert !actorList.containsKey(activityId);
            actorList.put(activityId, this);
          }
        }
        traceParser = vm.getTraceParser();
      } else {
        replayEvents = null;
        traceParser = null;
      }
    }

    @Override
    protected ExecAllMessages createExecutor(final VM vm) {
      if (VmSettings.REPLAY) {
        return new ExecAllMessagesReplay(this, vm);
      } else {
        return super.createExecutor(vm);
      }
    }

    @Override
    @TruffleBoundary
    public synchronized void send(final EventualMessage msg,
        final ForkJoinPool actorPool) {
      assert msg.getTarget() == this;

      if (!VmSettings.REPLAY) {
        super.send(msg, actorPool);
        return;
      }

      if (firstMessage == null) {
        firstMessage = msg;
      } else {
        appendToMailbox(msg);
      }

      if (!this.poisoned && this.replayEvents.isEmpty()
          && this.peekNextReplayEvent() == null) {
        this.poisoned = true;
      }

      // actor remains dormant until the expected message arrives
      if ((!this.isExecuting) && this.replayCanProcess(msg) && !this.poisoned) {
        isExecuting = true;
        execute(actorPool);
      }
    }

    public static void scheduleAllActors(final ForkJoinPool actorPool) {
      for (ReplayActor ra : actorList.values()) {
        ra.executeIfNecessarry(actorPool);
      }
    }

    protected boolean replayCanProcess(final EventualMessage msg) {
      if (!VmSettings.REPLAY) {
        return true;
      }

      return msg.getMessageId() == this.version;
    }

    private static class ExecAllMessagesReplay extends ExecAllMessages {
      ExecAllMessagesReplay(final Actor actor, final VM vm) {
        super(actor, vm);
      }

      private Queue<EventualMessage> determineNextMessages(
          final PriorityQueue<EventualMessage> orderedMessages) {
        final ReplayActor a = (ReplayActor) actor;
        int numReceivedMsgs = 1 + (mailboxExtension == null ? 0 : mailboxExtension.size());
        numReceivedMsgs += orderedMessages.size();

        Queue<EventualMessage> toProcess = new LinkedList<>();

        if (a.replayCanProcess(firstMessage)) {
          toProcess.add(firstMessage);
          a.version++;
        } else {
          orderedMessages.add(firstMessage);
        }

        if (mailboxExtension != null) {
          for (EventualMessage msg : mailboxExtension) {
            if (a.replayCanProcess(msg)) {
              toProcess.add(msg);
              a.version++;
            } else {
              orderedMessages.add(msg);
            }
          }
        }

        while (!orderedMessages.isEmpty() && a.replayCanProcess(orderedMessages.peek())) {
          EventualMessage msg = orderedMessages.poll();
          toProcess.add(msg);
          a.version++;
        }

        assert toProcess.size()
            + orderedMessages.size() == numReceivedMsgs : "We shouldn't lose any messages here.";
        return toProcess;
      }

      @Override
      protected void processCurrentMessages(final ActorProcessingThread currentThread,
          final WebDebugger dbg) {
        assert actor instanceof ReplayActor;
        assert size > 0;
        final ReplayActor a = (ReplayActor) actor;

        final ReplayActor a = (ReplayActor) actor;

        Queue<EventualMessage> todo = determineNextMessages(a.orderedMessages);

        for (EventualMessage msg : todo) {
          if (!a.poisoned && a.replayEvents.isEmpty()
              && a.peekNextReplayEvent() == null) {
            a.poisoned = true;
            return;
          }

          currentThread.currentMessage = msg;
          handleBreakpointsAndStepping(msg, dbg, a);
          msg.execute();
        }

        currentThread.createdMessages += todo.size();
      }
    }

    @Override
    public int getNextEventNumber() {
      return this.version;
    }
  }
}
