package tools.snapshot;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.graalvm.collections.EconomicMap;

import som.vm.VmSettings;
import tools.concurrency.TracingActors.TracingActor;


public class Snapshot {
  final ConcurrentLinkedQueue<SnapshotHeap>      buffers;
  final ConcurrentLinkedQueue<ArrayList<Long>>   messages;
  final EconomicMap<Integer, Long>               classLocations;
  final ArrayList<Long>                          lostResolutions;
  final ArrayList<Long>                          lostMessages;
  final ConcurrentHashMap<TracingActor, Integer> deferredSerializations;
  public final byte                              version;
  final ValueHeap                                valueBuffer;
  public boolean                                 persisted = false;

  public Snapshot(final byte version, final ValueHeap values) {
    this.version = version;
    this.valueBuffer = values;

    if (VmSettings.TRACK_SNAPSHOT_ENTITIES) {
      buffers = new ConcurrentLinkedQueue<>();
      messages = new ConcurrentLinkedQueue<>();
      deferredSerializations = new ConcurrentHashMap<>();
      lostResolutions = new ArrayList<>();
      classLocations = null;
      lostMessages = null;
    } else if (VmSettings.SNAPSHOTS_ENABLED) {
      classLocations = EconomicMap.create();
      buffers = new ConcurrentLinkedQueue<>();
      messages = new ConcurrentLinkedQueue<>();
      deferredSerializations = new ConcurrentHashMap<>();
      lostResolutions = new ArrayList<>();
      lostMessages = new ArrayList<>();
    } else {
      buffers = null;
      messages = null;
      classLocations = null;
      deferredSerializations = null;
      lostResolutions = null;
      lostMessages = null;
    }
  }

}
