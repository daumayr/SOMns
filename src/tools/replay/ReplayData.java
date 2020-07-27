package tools.replay;

import java.util.HashMap;
import java.util.LinkedList;

import som.Output;
import som.vm.Activity;
import som.vm.VmSettings;
import tools.concurrency.TracingActivityThread;


public class ReplayData {

  /***
   * This Method delays interaction of an activity with a passive entity until the passive
   * entity is in the right state, i.e., all predecessor events have been processed.
   *
   * @param pe The passive entity the current activity will interact with.
   * @param l
   * @param expectedNo The sequence number of the event to be performed.
   */
  public static void replayDelayNumberedEvent(final PassiveEntityWithEvents pe) {

    Activity reader = TracingActivityThread.currentThread().getActivity();
    ReplayRecord npr = reader.getNextReplayEvent();

    assert npr != null : reader.getId();

    try {
      while (pe.getNextEventNumber() != npr.eventNo) {
        Thread.sleep(5);
        // temporary solution for proof of concept.
        // maybe use some wait/notify all construct.
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  protected static class Subtrace {
    public final long startOffset;
    public long       length;
    public int        snapshot;

    Subtrace(final long startOffset) {
      this.startOffset = startOffset;
    }
  }

  protected static class EntityNode {
    final long             entityId;
    HashMap<Integer, Long> externalData;

    HashMap<Integer, Subtrace> subtraces;

    LinkedList<ReplayRecord> replayEvents;
    int                      nextContext = 0;
    boolean                  retrieved   = false;
    int                      minContext  = Integer.MAX_VALUE;


    public EntityNode(final long entityId) {
      this.entityId = entityId;
    }

    protected Subtrace registerContext(int ordering, final long location) {
      if (subtraces == null) {
        subtraces = new HashMap<>();
      }

      // assert !retrieved;

      // TODO probably can be done more efficiently
      while (subtraces.containsKey(ordering) || ordering < nextContext) {
        ordering += 0xFFFF;
      }

      Subtrace detail = new Subtrace(location);
      subtraces.put(ordering, detail);

      if (VmSettings.SNAPSHOT_REPLAY) {
        if (minContext > ordering) {
          minContext = ordering;
        }
      }

      return detail;
    }

    protected boolean parseContexts(final TraceParser parser) {
      Subtrace detail = subtraces.get(nextContext);
      if (detail != null) {
        parser.processContext(detail, this);
        nextContext++;
        return true;
      } else {
        Output.println("Context " + nextContext + " not present ");
        parser.scanNextTrace();

        // nextContext++;
        detail = subtraces.get(nextContext);
        if (detail != null) {
          parser.processContext(detail, this);
          nextContext++;
          return true;
        }

        return false;
      }
    }

    protected void addReplayEvent(final ReplayRecord mr) {
      if (replayEvents == null) {
        replayEvents = new LinkedList<>();
        Output.println("returned empty list");
      }
      replayEvents.add(mr);
    }

    public LinkedList<ReplayRecord> getReplayEvents() {
      assert !retrieved : entityId;
      retrieved = true;

      if (VmSettings.SNAPSHOT_REPLAY) {
        nextContext = minContext;
      }

      if (replayEvents == null) {
        replayEvents = new LinkedList<>();
      }
      return replayEvents;
    }

    protected void onContextStart(final int ordering) {}

    @Override
    public String toString() {
      return "" + entityId;
    }
  }
}
