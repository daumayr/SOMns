package som.interpreter.actors;

import java.util.LinkedList;
import java.util.concurrent.ForkJoinPool;

import com.oracle.truffle.api.nodes.Node;

import som.interpreter.actors.EventualMessage.AbstractPromiseSendMessage;
import som.interpreter.actors.EventualMessage.PromiseMessage;
import som.interpreter.actors.SPromise.SReplayPromise;
import som.interpreter.actors.SPromise.STracingPromise;
import som.vm.VmSettings;
import tools.replay.ReplayRecord;
import tools.replay.TraceRecord;
import tools.replay.nodes.RecordEventNodes.RecordOneEvent;


public abstract class RegisterOnPromiseNode {

  public static final class RegisterWhenResolved extends Node {
    @Child protected SchedulePromiseHandlerNode schedule;
    @Child protected RecordOneEvent             promiseMsgSend;

    public RegisterWhenResolved(final ForkJoinPool actorPool) {
      schedule = SchedulePromiseHandlerNodeGen.create(actorPool);
      if (VmSettings.UNIFORM_TRACING) {
        promiseMsgSend = new RecordOneEvent(TraceRecord.PROMISE_MESSAGE);
      }
    }

    public void register(final SPromise promise, final PromiseMessage msg,
        final Actor current) {

      Object promiseValue;
      // LOCKING NOTE: this handling of the check for resolution and the
      // registering of the callback/msg needs to be done in the same step
      // otherwise, we are opening a race between completion and processing
      // of handlers. We can split the case for a resolved promise out, because
      // we need to schedule the callback/msg directly anyway

      synchronized (promise) {
        if (VmSettings.REPLAY) {
          ReplayRecord npr = current.peekNextReplayEvent();
          if (npr.type == TraceRecord.PROMISE_CHAINED && !promise.isResolvedUnsync()) {
            npr = current.getNextReplayEvent();

            assert msg instanceof AbstractPromiseSendMessage;
            LinkedList<ReplayRecord> events = new LinkedList<>();
            while (npr.type == TraceRecord.PROMISE_CHAINED) {
              events.add(npr);
              npr = current.getNextReplayEvent();
            }
            assert npr.type == TraceRecord.MESSAGE;
            msg.messageId = npr.eventNo;
            ((SReplayPromise) promise).registerPromiseMessageWithChainingEvents(msg, events);
            return;
          }

          assert npr.type == TraceRecord.PROMISE_MESSAGE
              || npr.type == TraceRecord.MESSAGE
              || npr.type == TraceRecord.PROMISE_CHAINED : "was: " + npr.type.name();

          if (npr.type == TraceRecord.PROMISE_MESSAGE) {
            current.getNextReplayEvent();
            msg.messageId = npr.eventNo;
            // Output.println("sent promise msg to " + promise +
            // System.identityHashCode(promise)
            // + " \n " + " with "
            // + npr.eventNo);
            // (new Throwable()).printStackTrace();
            ((SReplayPromise) promise).registerOnResolvedSnapshot(msg, true);
            return;
          }
        }

        if (!promise.isResolvedUnsync()) {
          if (VmSettings.REPLAY) {
            ReplayRecord npr = current.getNextReplayEvent();
            assert npr.type == TraceRecord.MESSAGE;
            msg.messageId = npr.eventNo;
          }

          if (VmSettings.UNIFORM_TRACING) {
            // This is whenResolved
            // Output.println("sent promise msg to " + promise
            // + " \n " + EventualMessage.getCurrentExecutingMessage() + " with "
            // + ((STracingPromise) promise).version);
            promiseMsgSend.record(((STracingPromise) promise).version);
            if (VmSettings.SNAPSHOTS_ENABLED) {
              msg.messageId = ((STracingPromise) promise).version;
            }

            ((STracingPromise) promise).version++;
          }

          if (promise.isErroredUnsync()) {
            // short cut on error, this promise will never resolve successfully, so,
            // just return promise, don't use isSomehowResolved(), because the other
            // case are not correct
            return;
          }

          promise.registerWhenResolvedUnsynced(msg);
          return;
        } else {
          promiseValue = promise.value;
          assert promiseValue != null;
        }
      }

      // LOCKING NOTE:
      // The schedule.execute() is moved out of the synchronized(promise) block,
      // because it leads to wrapping of arguments, which needs to look promises
      // and thus, could lead to a deadlock.
      //
      // However, we still need to wait until any other thread is done with
      // resolving the promise, thus we lock on the promise's value, which is
      // used to group setting the promise resolution state and processing the
      // message.
      synchronized (promiseValue) {
        if (promise.getHaltOnResolution()) {
          msg.enableHaltOnReceive();
        }

        // TODO check what this does
        schedule.execute(promise, msg, current);
      }
    }
  }

  public static final class RegisterOnError extends Node {
    @Child protected SchedulePromiseHandlerNode schedule;
    @Child protected RecordOneEvent             promiseMsgSend;

    public RegisterOnError(final ForkJoinPool actorPool) {
      this.schedule = SchedulePromiseHandlerNodeGen.create(actorPool);
      if (VmSettings.UNIFORM_TRACING) {
        this.promiseMsgSend = new RecordOneEvent(TraceRecord.PROMISE_MESSAGE);
      }
    }

    public void register(final SPromise promise, final PromiseMessage msg,
        final Actor current) {

      // Output.println("try register onerror " + promise.hashCode() + " in "
      // + TracingActivityThread.currentThread().getActivity().getId());

      Object promiseValue;
      // LOCKING NOTE: this handling of the check for resolution and the
      // registering of the callback/msg needs to be done in the same step
      // otherwise, we are opening a race between completion and processing
      // of handlers. We can split the case for a resolved promise out, because
      // we need to schedule the callback/msg directly anyway
      synchronized (promise) {
        if (VmSettings.REPLAY) {
          ReplayRecord npr = current.getNextReplayEvent();
          assert npr.type == TraceRecord.PROMISE_MESSAGE
              || npr.type == TraceRecord.MESSAGE;
          msg.messageId = npr.eventNo;

          if (npr.type == TraceRecord.PROMISE_MESSAGE) {
            ((SReplayPromise) promise).registerOnErrorSnapshot(msg, true);
            return;
          }
        }

        if (!promise.isErroredUnsync()) {

          if (VmSettings.UNIFORM_TRACING) {
            // This is whenResolved
            promiseMsgSend.record(((STracingPromise) promise).version);
            if (VmSettings.SNAPSHOTS_ENABLED) {
              msg.messageId = ((STracingPromise) promise).version;
            }
            ((STracingPromise) promise).version++;
          }

          if (promise.isResolvedUnsync()) {
            // short cut on resolved, this promise will never error, so,
            // just return promise, don't use isSomehowResolved(), because the other
            // case are not correct
            return;
          }

          promise.registerOnErrorUnsynced(msg);
          return;
        } else {
          promiseValue = promise.value;
          assert promiseValue != null;
        }
      }

      // LOCKING NOTE:
      // The schedule.execute() is moved out of the synchronized(promise) block,
      // because it leads to wrapping of arguments, which needs to look promises
      // and thus, could lead to a deadlock.
      //
      // However, we still need to wait until any other thread is done with
      // resolving the promise, thus we lock on the promise's value, which is
      // used to group setting the promise resolution state and processing the
      // message.
      synchronized (promiseValue) {
        schedule.execute(promise, msg, current);
      }
    }
  }
}
