package tools.snapshot;

import java.util.WeakHashMap;

import org.graalvm.collections.EconomicMap;

import som.interpreter.actors.Actor.ActorProcessingThread;


public class ValueHeap extends SnapshotHeap {

  final EconomicMap<Integer, Long> valueClassLocations;
  final WeakHashMap<Object, Long>  valuePool;

  public ValueHeap(final ActorProcessingThread actorProcessingThread) {
    super(actorProcessingThread);
    valueClassLocations = EconomicMap.create();
    valuePool = new WeakHashMap<>();
  }

  public ValueHeap(final byte version) {
    super(version);
    valueClassLocations = EconomicMap.create();
    valuePool = new WeakHashMap<>();
  }

}