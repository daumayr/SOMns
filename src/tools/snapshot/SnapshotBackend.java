package tools.snapshot;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.graalvm.collections.EconomicMap;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;

import bd.tools.structure.StructuralProbe;
import som.Output;
import som.VM;
import som.compiler.MixinDefinition;
import som.compiler.MixinDefinition.SlotDefinition;
import som.compiler.Variable;
import som.interpreter.SomLanguage;
import som.interpreter.Types;
import som.interpreter.actors.Actor;
import som.interpreter.actors.Actor.ActorProcessingThread;
import som.interpreter.actors.EventualMessage;
import som.interpreter.actors.EventualMessage.PromiseMessage;
import som.interpreter.actors.SPromise;
import som.interpreter.actors.SPromise.SResolver;
import som.interpreter.actors.SPromise.STracingPromise;
import som.interpreter.nodes.InstantiationNode.ClassInstantiationNode;
import som.interpreter.objectstorage.ClassFactory;
import som.vm.VmSettings;
import som.vmobjects.SClass;
import som.vmobjects.SInvokable;
import som.vmobjects.SObjectWithClass;
import som.vmobjects.SSymbol;
import tools.concurrency.TracingActivityThread;
import tools.concurrency.TracingActors.ReplayActor;
import tools.concurrency.TracingActors.TracingActor;
import tools.concurrency.TracingBackend;
import tools.snapshot.deserialization.DeserializationBuffer;
import tools.snapshot.deserialization.SnapshotParser;
import tools.snapshot.nodes.AbstractSerializationNode;
import tools.snapshot.nodes.PromiseSerializationNodes;


public class SnapshotBackend {
  private static byte snapshotVersion = 0;

  private static final StructuralProbe<SSymbol, MixinDefinition, SInvokable, SlotDefinition, Variable> probe;

  private static final EconomicMap<Short, SSymbol>  symbolDictionary;
  private static final EconomicMap<Integer, Object> classDictionary;

  private static final long SNAPSHOT_TIMEOUT = 500;

  private static ValueHeap valueBuffer;

  // this is a reference to the list maintained by the objectsystem
  private static EconomicMap<URI, MixinDefinition> loadedModules;
  protected static SPromise                        resultPromise;
  private static Snapshot                          snapshot;
  private static int                               numTries = 0;

  // Variables used to determine when a snapshot is stable enough to start persisting it.
  public static boolean        bubbleExecuted    = false;
  private static AtomicInteger cleanThreadsCount = new AtomicInteger(0);

  private static SnapshotWriterThread swt;

  @CompilationFinal private static VM vm;

  public static boolean snapshotStarted;

  static {
    if (VmSettings.TRACK_SNAPSHOT_ENTITIES) {
      classDictionary = EconomicMap.create();
      symbolDictionary = EconomicMap.create();
      probe = new StructuralProbe<>();
      // identity int, includes mixin info
      // long outer
      // essentially this is about capturing the outer
      // let's do this when the class is stucturally initialized
      swt = new SnapshotWriterThread();
      swt.start();
    } else if (VmSettings.SNAPSHOTS_ENABLED) {
      classDictionary = null;
      symbolDictionary = null;
      probe = null;
      valueBuffer = new ValueHeap((byte) 0);
      swt = new SnapshotWriterThread();
      swt.start();
    } else {
      classDictionary = null;
      symbolDictionary = null;
      probe = null;
    }
  }

  public static void initialize(final VM vm) {
    SnapshotBackend.vm = vm;
  }

  public static SSymbol getSymbolForId(final short id) {
    return symbolDictionary.get(id);
  }

  public static void registerSymbol(final SSymbol sym) {
    assert VmSettings.TRACK_SNAPSHOT_ENTITIES;
    symbolDictionary.put(sym.getSymbolId(), sym);
  }

  public static synchronized void registerClass(final SClass clazz) {
    assert VmSettings.TRACK_SNAPSHOT_ENTITIES;
    Object current = classDictionary.get(clazz.getIdentity());
    if (!(current instanceof LinkedList)) {
      classDictionary.put(clazz.getIdentity(), clazz);
    }
  }

  public static void registerLoadedModules(final EconomicMap<URI, MixinDefinition> loaded) {
    loadedModules = loaded;
  }

  public static SClass lookupClass(final int id) {
    assert VmSettings.TRACK_SNAPSHOT_ENTITIES;
    if (classDictionary.containsKey(id)) {
      if (!(classDictionary.get(id) instanceof SClass)) {
        return null;
      }

      return (SClass) classDictionary.get(id);
    }

    // doesn't exist yet
    return createSClass(id);
  }

  @SuppressWarnings("unchecked")
  public static SClass lookupClass(final int id, final long ref) {
    if (!classDictionary.containsKey(id)) {
      // doesn't exist yet
      return createSClass(id);
    }

    Object entry = classDictionary.get(id);

    if (entry instanceof SClass) {
      return (SClass) entry;
    }

    if (entry == null) {
      entry = new LinkedList<Long>();
      classDictionary.put(id, entry);
    }

    LinkedList<Long> todo = (LinkedList<Long>) entry;
    todo.add(ref);
    return null;
  }

  public static SClass finishCreateSClass(final int id, final SObjectWithClass enclosing) {
    assert classDictionary.containsKey(id);
    MixinDefinition mixin = acquireMixin(id);

    // Step 3: create Class
    Object superclassAndMixins =
        mixin.getSuperclassAndMixinResolutionInvokable().createCallTarget()
             .call(new Object[] {enclosing});

    ClassFactory factory = mixin.createClassFactory(superclassAndMixins, false, false, false);

    SClass result = new SClass(enclosing,
        ClassInstantiationNode.instantiateMetaclassClass(factory, enclosing));
    factory.initializeClass(result);

    // Step 4: fixup
    Object current = classDictionary.get(id);
    classDictionary.put(id, result);

    if (current instanceof LinkedList) {
      @SuppressWarnings("unchecked")
      LinkedList<Long> todo = (LinkedList<Long>) current;
      DeserializationBuffer db = SnapshotParser.getDeserializationBuffer();
      for (long ref : todo) {
        db.putAndFixUpIfNecessary(ref, result);
      }
    }
    return result;
  }

  private static SClass createSClass(final int id) {
    assert !classDictionary.containsKey(id);

    // Step 1: install placeholder
    classDictionary.put(id, null);
    // Output.println("creating Class" + mixin.getIdentifier() + " : " + (short) id);

    // Step 2: get outer object
    SObjectWithClass enclosingObject = SnapshotParser.getOuterForClass(id);
    if (enclosingObject == null) {
      return null;
    }

    return finishCreateSClass(id, enclosingObject);
  }

  private static MixinDefinition acquireMixin(final int id) {
    short symId = (short) (id >> 16);

    SSymbol location = getSymbolForId(symId);
    assert location != null : id;
    String[] parts = location.getString().split(":");
    if (parts.length != 2) {
      assert false : "" + id + " sym" + symId;
    }

    Path path = Paths.get(VmSettings.BASE_DIRECTORY, parts[0]);
    MixinDefinition mixin = loadedModules.get(path.toUri());

    if (mixin == null) {
      // need to load module
      try {
        mixin = vm.loadModule(path.toString());
        SClass module = mixin.instantiateModuleClass();
        classDictionary.put(module.getIdentity(), module);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    String[] nestings = parts[1].split("\\.");

    for (String sub : nestings) {
      MixinDefinition temp = mixin.getNestedMixinDefinition(sub);
      if (temp != null) {
        mixin = temp;
      }
    }
    return mixin;
  }

  public static long getFrameLoction(final Object[] objects) {
    assert snapshot != null;
    assert snapshot.frameLocations != null;
    return snapshot.frameLocations.get(objects, -1l);
  }

  public static void setFrameLoction(final Object[] objects, final long location) {
    assert snapshot != null;
    assert snapshot.frameLocations != null;

    snapshot.frameLocations.put(objects, location);
  }

  public static SInvokable lookupInvokable(final SSymbol sym) {
    assert VmSettings.TRACK_SNAPSHOT_ENTITIES;
    SInvokable result = probe.lookupMethod(sym);

    if (result == null) {
      // Module probably not loaded, attempt to do that.
      String[] parts = sym.getString().split(":");
      Path path = Paths.get(VmSettings.BASE_DIRECTORY, parts[0]);

      MixinDefinition mixin;
      mixin = loadedModules.get(path.toUri());
      if (mixin == null) {
        // need to load module
        try {
          mixin = vm.loadModule(path.toString());
          SClass module = mixin.instantiateModuleClass();
          classDictionary.put(module.getIdentity(), module);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    result = probe.lookupMethod(sym);
    return result;
  }

  public static SInvokable lookupInvokable(final short sym) {
    return lookupInvokable(getSymbolForId(sym));
  }

  public static synchronized void startSnapshot() {
    assert VmSettings.SNAPSHOTS_ENABLED;

    if (numTries == 0) {
      Output.println("starting snapshot");

      valueBuffer.snapshotVersion = (byte) (snapshotVersion + 1);
      if (valueBuffer.size > VmSettings.BUFFER_SIZE * 50) {
        synchronized (valueBuffer) {
          valueBuffer = new ValueHeap((byte) (snapshotVersion + 1));
        }
      }

      snapshot = new Snapshot((byte) (snapshotVersion + 1), valueBuffer);

      snapshotVersion++;

      // notify the worker in the tracingbackend about this change.
      TracingBackend.newSnapshot(snapshotVersion);
      snapshotStarted = true;

      bubbleExecuted = false;
      cleanThreadsCount.set(0);
      openDeferrals.set(0);

      // insert "bubble" into task queue to monitor progress
      CompilerDirectives.transferToInterpreter();
      SomLanguage.getCurrent().getVM().getActorPool().submit(new Runnable() {
        @Override
        public void run() {
          // When this is executed this means that no more actors with old messages are left in
          // the task queue.
          // when each thread in the pool finishes execution of a mailbox, the snapshot should
          // be
          // stable.

          bubbleExecuted = true;
        }
      });
    }

    numTries++;

    if (numTries == VmSettings.SNAPSHOT_FREQUENCY) {
      numTries = 0;
    }
  }

  public static synchronized void incrementPhaseForSerializationBench() {
    snapshotVersion++;
  }

  public static byte getSnapshotVersion() {
    assert VmSettings.SNAPSHOTS_ENABLED;
    // intentionally unsynchronized, as a result the line between snapshots will be a bit
    // fuzzy.
    return snapshotVersion;
  }

  public static WeakHashMap<Object, Long> getValuepool() {
    return valueBuffer.valuePool;
  }

  public static ValueHeap getValueHeap() {
    return valueBuffer;
  }

  public static Actor lookupActor(final long resolver) {
    assert resolver != -1;
    if (VmSettings.REPLAY) {
      ReplayActor ra = ReplayActor.getActorWithId(resolver);
      if (ra == null) {
        ra = new ReplayActor(vm, resolver);
      }
      return ra;
    } else {
      // For testing with snaphsotClone:
      return EventualMessage.getActorCurrentMessageIsExecutionOn();
    }
  }

  public static void registerSnapshotBuffer(final SnapshotHeap snapshotHeap,
      final ArrayList<Long> messageLocations) {
    if (VmSettings.TEST_SERIALIZE_ALL || snapshot == null) {
      return;
    }

    assert snapshotHeap != null;
    snapshot.buffers.add(snapshotHeap);
    snapshot.messages.add(messageLocations);
  }

  /**
   * Serialization of objects referenced from far references need to be deferred to the owning
   * actor.
   */
  @TruffleBoundary
  public static void deferSerialization(final TracingActor sr) {
    assert snapshot != null;
    assert snapshot.deferredSerializations != null;
    snapshot.deferredSerializations.put(sr, 0);
  }

  @TruffleBoundary
  public static void completedSerialization(final TracingActor sr) {
    snapshot.deferredSerializations.remove(sr);
  }

  public static StructuralProbe<SSymbol, MixinDefinition, SInvokable, SlotDefinition, Variable> getProbe() {
    assert probe != null;
    return probe;
  }

  public static TracingActor getCurrentActor() {
    if (VmSettings.REPLAY) {
      return SnapshotParser.getCurrentActor();
    } else {
      return (TracingActor) EventualMessage.getActorCurrentMessageIsExecutionOn();
    }
  }

  public static void registerActorVersion(final long actorId, final long version) {
    synchronized (snapshot.actorVersions) {
      snapshot.actorVersions.add(actorId);
      snapshot.actorVersions.add(version);
    }
  }

  public static void registerResultPromise(final SPromise promise) {
    assert resultPromise == null;
    resultPromise = promise;
  }

  public static void registerLostResolution(final SResolver resolver,
      final SnapshotHeap sh) {

    long resolverLocation = SResolver.getResolverClass().serialize(resolver, sh);
    assert snapshot.version == sh.getSnapshotVersion();

    if (!resolver.getPromise().isCompleted()) {
      Output.println("skipped!!");
      return;
    }

    Object result = resolver.getPromise().getValueForSnapshot();

    long resultLocation = Types.getClassOf(result).serialize(result, sh);

    SnapshotBuffer sb = sh.getBuffer((Long.BYTES * 3) + 1);
    int base = sb.reserveSpace((Long.BYTES * 2) + Integer.BYTES + 1);

    synchronized (snapshot.lostResolutions) {
      snapshot.lostResolutions.add(sb.calculateReference(base));
    }

    sb.putLongAt(base, resolverLocation);
    sb.putLongAt(base + Long.BYTES, resultLocation);
    sb.putLongAt(base + (Long.BYTES * 2),
        ((TracingActor) sh.getOwner().getCurrentActor()).getId());
    sb.putByteAt(base + (Long.BYTES * 3),
        (byte) resolver.getPromise().getResolutionStateUnsync().ordinal());

  }

  public static void registerLostMessage(final PromiseMessage pm, final SPromise promise) {

    if (snapshot == null
        || pm.getOriginalSnapshotPhase() == TracingActivityThread.currentThread()
                                                                 .getSnapshotId()) {
      return;
    }
    SnapshotHeap sh = TracingActivityThread.currentThread().getSnapshotHeapWithoutUpdate();

    SnapshotBuffer sb = sh.getBuffer(Long.BYTES * 3);
    int start = sb.reserveSpace(Long.BYTES * 3);

    // Messages is not going to be captured by the receiver, hence we can just serialize it
    // here. Multi resolution is not allowed -> serialized once.

    long loc = pm.forceSerialize(sh);

    sb.putLongAt(start, loc);
    // Promise itself has to be far reffed for cases where resolver is not the owner of the
    // promise.
    ((TracingActor) promise.getOwner()).farReference(promise, sb, start + Long.BYTES);
    sb.putLongAt(start + Long.BYTES + Long.BYTES, pm.getMessageId());
    synchronized (snapshot.lostMessages) {
      long l = sb.calculateReference(start);
      // Output.println("Lost Msg: " + l + " in " + sh.getSnapshotVersion() + " " + pm + " "
      // + snapshot.version);
      snapshot.lostMessages.add(l);
    }
  }

  public static void registerLostErrorMessage(final PromiseMessage pm,
      final SPromise promise) {

    if (snapshot == null
        || pm.getOriginalSnapshotPhase() == TracingActivityThread.currentThread()
                                                                 .getSnapshotId()) {
      return;
    }
    SnapshotHeap sh = TracingActivityThread.currentThread().getSnapshotHeapWithoutUpdate();

    SnapshotBuffer sb = sh.getBuffer(Long.BYTES * 3);
    int start = sb.reserveSpace(Long.BYTES * 3);

    // Messages is not going to be captured by the receiver, hence we can just serialize it
    // here. Multi resolution is not allowed -> serialized once.

    long loc = pm.forceSerialize(sh);

    sb.putLongAt(start, loc);
    // Promise itself has to be far reffed for cases where resolver is not the owner of the
    // promise.
    ((TracingActor) promise.getOwner()).farReference(promise, sb, start + Long.BYTES);
    sb.putLongAt(start + Long.BYTES + Long.BYTES, pm.getMessageId());
    synchronized (snapshot.lostErrorMessages) {
      long l = sb.calculateReference(start);
      // Output.println("Lost Msg: " + l + " in " + sh.getSnapshotVersion() + " " + pm + " "
      // + snapshot.version);
      snapshot.lostErrorMessages.add(l);
    }
  }

  public static void registerLostChain(final STracingPromise chained,
      final STracingPromise promise) {
    TracingActivityThread tat = TracingActivityThread.currentThread();
    SnapshotHeap sh;

    if (snapshot == null || !chained.canBeLost()) {// || snapshotVersion !=
                                                   // sh.getSnapshotVersion()) {
      return;
    }

    if (tat.getNextSnapshotHeap().snapshotVersion == snapshotVersion) {
      sh = tat.getNextSnapshotHeap();
    } else {
      sh = tat.getSnapshotHeapWithoutUpdate();
    }

    SnapshotBuffer sb = sh.getBuffer(Long.BYTES * 3);
    int start = sb.reserveSpace(Long.BYTES * 3);
    chained.getResolvingActor();// version promise was chained at is stored there temporarily.

    // delegate to owners
    ((TracingActor) chained.getOwner()).farReference(chained, sb, start);
    ((TracingActor) promise.getOwner()).farReference(promise, sb, start + Long.BYTES);
    sb.putLongAt(start + Long.BYTES + Long.BYTES, chained.getResolvingActor());

    // TODO own infrastructure
    synchronized (snapshot.lostChains) {
      long loc = sb.calculateReference(start);
      // Output.println("Lost Chain: " + loc + " in " + sh.getSnapshotVersion());
      snapshot.lostChains.add(sb.calculateReference(start));
    }
  }

  public static void registerClassLocation(final int identity, final long classLocation) {
    if (VmSettings.TEST_SNAPSHOTS) {
      return;
    }
    synchronized (snapshot.classLocations) {
      snapshot.classLocations.put(identity, classLocation);
    }
  }

  public static void registerValueClassLocation(final int identity, final long classLocation) {
    if (VmSettings.TEST_SNAPSHOTS) {
      return;
    }

    synchronized (valueBuffer.valueClassLocations) {
      valueBuffer.valueClassLocations.put(identity, classLocation);
    }
  }

  public static void ensureLastSnapshotPersisted() {
    if (swt != null) {
      try {
        if (snapshot != null && !snapshot.persisted) {
          Thread.sleep(SNAPSHOT_TIMEOUT);
        }

        swt.cont = false;
        swt.join(SNAPSHOT_TIMEOUT);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @TruffleBoundary
  private static void persistSnapshot() {
    // Output.println("persisting");
    if (!snapshot.persisted) {
      snapshot.persisted = true;
      swt.addSnapshot(snapshot);
      snapshot = null;
    }
  }

  private static AtomicInteger openDeferrals = new AtomicInteger(0);

  /**
   * Calling thread makes known that it should not have any more messages to serialize for the
   * current snapshot.
   * Last thread calling arriving causes snapshot to be persisted.
   */
  @TruffleBoundary
  public static void threadClean() {
    int cleanThreads = cleanThreadsCount.incrementAndGet();
    if (cleanThreads == VmSettings.NUM_THREADS) {
      // ensure the result promise is serialized

      ActorProcessingThread apt =
          (ActorProcessingThread) ActorProcessingThread.currentThread();

      long location = AbstractSerializationNode.getObjectLocation(
          SnapshotBackend.resultPromise, snapshotVersion);
      if (location == -1) {
        PromiseSerializationNodes.ensurePromiseSerialized(SnapshotBackend.resultPromise,
            apt.getSnapshotHeapWithoutUpdate());
      }

      if (!snapshot.deferredSerializations.isEmpty()) {
        TracingActor currentActor = (TracingActor) apt.getCurrentActor();
        currentActor.handleObjectsReferencedFromFarRefs(apt.getSnapshotHeapWithoutUpdate());

        for (TracingActor ta : snapshot.deferredSerializations.keySet()) {
          snapshot.deferredSerializations.remove(ta);
          ta.setReportDeferredDone(true);
          openDeferrals.incrementAndGet();
          ta.executeForDeferred(vm.getActorPool());
        }

        if (currentActor.isReportDeferredDone()) {
          currentActor.setReportDeferredDone(false);
          doneWithDeferred(currentActor);
        }

        if (openDeferrals.get() == 0) {
          persistSnapshot();// only current actor was missing.
        }
      } else {
        // allready complete
        persistSnapshot();
      }
    }
  }

  @TruffleBoundary
  public static void doneWithDeferred(final TracingActor actor) {
    int open = openDeferrals.decrementAndGet();
    actor.setReportDeferredDone(false);

    if (open == 0) {
      if (snapshot.deferredSerializations.isEmpty()) {
        // allready complete
        persistSnapshot();
      } else {
        // another round

        for (TracingActor ta : snapshot.deferredSerializations.keySet()) {
          snapshot.deferredSerializations.remove(ta);
          ta.setReportDeferredDone(true);
          openDeferrals.incrementAndGet();
          ta.executeForDeferred(vm.getActorPool());
        }

        if (actor.isReportDeferredDone()) {
          ActorProcessingThread apt =
              (ActorProcessingThread) ActorProcessingThread.currentThread();
          Output.println("done deffered");
          actor.handleObjectsReferencedFromFarRefs(apt.getSnapshotHeapWithoutUpdate());
          openDeferrals.decrementAndGet();
        }

        if (openDeferrals.get() == 0) {
          persistSnapshot();// only current actor was missing.
        }
      }
    }
  }

  public static boolean snapshotDone() {
    if (snapshot == null) {
      return true;
    }
    return snapshot.persisted;
  }
}
