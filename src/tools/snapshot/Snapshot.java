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
  final EconomicMap<Object[], Long>              frameLocations;
  final ArrayList<Long>                          lostResolutions;
  final ArrayList<Long>                          lostMessages;
  final ArrayList<Long>                          lostErrorMessages;
  final ArrayList<Long>                          lostChains;
  final ArrayList<Long>                          actorVersions;
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
      frameLocations = null;
      actorVersions = null;
      lostMessages = null;
      lostErrorMessages = null;
      lostChains = null;
    } else if (VmSettings.SNAPSHOTS_ENABLED) {
      classLocations = EconomicMap.create();
      frameLocations = EconomicMap.create();
      buffers = new ConcurrentLinkedQueue<>();
      messages = new ConcurrentLinkedQueue<>();
      deferredSerializations = new ConcurrentHashMap<>();
      lostResolutions = new ArrayList<>();
      actorVersions = new ArrayList<>();
      lostMessages = new ArrayList<>();
      lostErrorMessages = new ArrayList<>();
      lostChains = new ArrayList<>();
    } else {
      buffers = null;
      messages = null;
      classLocations = null;
      frameLocations = null;
      deferredSerializations = null;
      lostResolutions = null;
      actorVersions = null;
      lostMessages = null;
      lostErrorMessages = null;
      lostChains = null;
    }
  }

}