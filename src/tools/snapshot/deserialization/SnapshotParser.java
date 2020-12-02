package tools.snapshot.deserialization;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;

import org.graalvm.collections.EconomicMap;

import som.Output;
import som.VM;
import som.interpreter.actors.EventualMessage;
import som.interpreter.actors.EventualMessage.PromiseCallbackMessage;
import som.interpreter.actors.EventualMessage.PromiseMessage;
import som.interpreter.actors.SPromise;
import som.interpreter.actors.SPromise.Resolution;
import som.interpreter.actors.SPromise.SReplayPromise;
import som.interpreter.actors.SPromise.SResolver;
import som.interpreter.actors.SPromise.STracingPromise;
import som.vm.Symbols;
import som.vm.VmSettings;
import som.vm.constants.Nil;
import som.vmobjects.SClass;
import som.vmobjects.SObjectWithClass;
import tools.concurrency.TracingActors.ReplayActor;
import tools.snapshot.SnapshotBackend;
import tools.snapshot.SnapshotBuffer;
import tools.snapshot.deserialization.DeserializationBuffer.FileDeserializationBuffer;


public final class SnapshotParser {

  private static SnapshotParser parser;

  private EconomicMap<Long, Long>             heapOffsets;
  private EconomicMap<Long, LinkedList<Long>> messageLocations;
  private EconomicMap<Long, Long>             actorVersions;
  private SPromise                            resultPromise;
  private ReplayActor                         currentActor;
  private VM                                  vm;
  private EconomicMap<Integer, Long>          classLocations;
  private DeserializationBuffer               db;
  private int                                 objectcnt;
  private HashSet<EventualMessage>            sentPMsgs;
  private HashSet<Long>                       messagel;

  private SnapshotParser(final VM vm) {
    this.vm = vm;
    this.heapOffsets = EconomicMap.create();
    this.messageLocations = EconomicMap.create();
    this.actorVersions = EconomicMap.create();
    this.classLocations = EconomicMap.create();
    this.sentPMsgs = new HashSet<>();
    this.messagel = new HashSet<>();
  }

  // preparations to be done before anything else
  public static void preparations() {
    parseSymbols();
  }

  public static void inflate(final VM vm) {
    if (parser == null) {
      parser = new SnapshotParser(vm);
    }
    parser.parseMetaData();
  }

  /**
   * Read the Method Pointers, their actorIds, and most importantly the start addresses for the
   * thread areas.
   */
  private void parseMetaData() {
    ByteBuffer b = ByteBuffer.allocate(VmSettings.BUFFER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    String fileName =
        VmSettings.TRACE_FILE + "." + VmSettings.SNAPSHOT_REPLAY_VERSION + ".snap";
    File traceFile = new File(fileName);
    try (FileInputStream fis = new FileInputStream(traceFile);
        FileChannel channel = fis.getChannel()) {
      channel.read(b);
      b.flip(); // prepare for reading from buffer

      long numMessages = b.getLong() / 2;
      for (int i = 0; i < numMessages; i++) {
        ensureRemaining(Long.BYTES * 2, b, channel);
        long actorId = b.getLong();
        long location = b.getLong();

        if (!messageLocations.containsKey(actorId)) {
          messageLocations.put(actorId, new LinkedList<>());
        }
        messageLocations.get(actorId).add(location);
        messagel.add(location);
      }

      long numActors = b.getLong() / 2;
      for (int i = 0; i < numActors; i++) {
        ensureRemaining(Long.BYTES * 2, b, channel);
        long actorId = b.getLong();
        long version = b.getLong();

        // Output.println("Actor " + actorId + " starting at " + version);
        actorVersions.put(actorId, version);

        if (actorId == 0 && vm.getMainActor() != null) {
          ((ReplayActor) vm.getMainActor()).setInitialMainActorVersion((int) version);
        }
      }

      long numOuters = b.getLong();
      for (int i = 0; i < numOuters; i++) {
        ensureRemaining(Long.BYTES * 2, b, channel);
        int identity = (int) b.getLong();
        long classLocation = b.getLong();
        classLocations.put(identity, classLocation);
      }

      long numResolutions = b.getLong();
      ArrayList<Long> lostResolutions = new ArrayList<>();
      for (int i = 0; i < numResolutions; i++) {
        ensureRemaining(Long.BYTES, b, channel);
        long resolver = b.getLong();
        lostResolutions.add(resolver);
      }

      long numMsgs = b.getLong();
      ArrayList<Long> lostMessages = new ArrayList<>();
      for (int i = 0; i < numMsgs; i++) {
        ensureRemaining(Long.BYTES, b, channel);
        long entryLoc = b.getLong();
        lostMessages.add(entryLoc);
      }

      long numEMsgs = b.getLong();
      ArrayList<Long> lostErrorMessages = new ArrayList<>();
      for (int i = 0; i < numEMsgs; i++) {
        ensureRemaining(Long.BYTES, b, channel);
        long entryLoc = b.getLong();
        lostErrorMessages.add(entryLoc);
      }

      long numChains = b.getLong();
      ArrayList<Long> lostChains = new ArrayList<>();
      for (int i = 0; i < numChains; i++) {
        ensureRemaining(Long.BYTES, b, channel);
        long entryLoc = b.getLong();
        lostChains.add(entryLoc);
      }

      ensureRemaining(Long.BYTES * 2, b, channel);
      long resultPromiseLocation = b.getLong();
      long numHeaps = b.getLong();
      for (int i = 0; i < numHeaps; i++) {
        ensureRemaining(Long.BYTES * 2, b, channel);
        long threadId = b.getLong();
        long offset = b.getLong();
        heapOffsets.put(threadId, offset);
      }

      // At this point we now have read all of the metadata and can begin the process of
      // inflating the snapshot.

      // make sure all the actors exist, we resuse the mapping in ReplayActor
      for (long id : messageLocations.getKeys()) {
        SnapshotBackend.lookupActor(id);
      }

      db = new FileDeserializationBuffer(channel);
      ArrayList<PromiseMessage> messagesNeedingFixup = new ArrayList<>();

      // now let's go through the message list actor by actor, deserialize each message, and
      // add it to the actors mailbox.
      for (long id : messageLocations.getKeys()) {
        LinkedList<Long> locations = messageLocations.get(id);
        Long ml = locations.poll();

        while (ml != null) {
          // Deserialilze message
          currentActor = ReplayActor.getActorWithId(id);
          EventualMessage em = (EventualMessage) db.deserializeWithoutContext(ml);
          db.doUnserialized();

          // Output.println(currentActor + " " + em + " " + em.getMessageId());
          if (em instanceof PromiseMessage) {
            if (em.getArgs()[0] instanceof SPromise) {
              STracingPromise prom = (STracingPromise) ((PromiseMessage) em).getPromise();
              if (prom.isCompleted()) {
                ((PromiseMessage) em).resolve(prom.getValueForSnapshot(), currentActor,
                    SnapshotBackend.lookupActor(prom.getResolvingActor()));
              } else {
                messagesNeedingFixup.add((PromiseMessage) em);
              }
            } else if (em instanceof PromiseCallbackMessage) {
              // if (em.getArgs()[1] instanceof SPromise) {
              STracingPromise prom = (STracingPromise) ((PromiseMessage) em).getPromise();
              if (prom.isCompleted()) {
                ((PromiseCallbackMessage) em).resolve(prom.getValueForSnapshot(),
                    currentActor,
                    SnapshotBackend.lookupActor(prom.getResolvingActor()));
              } else {
                messagesNeedingFixup.add((PromiseMessage) em);
              }
              // }
            } else if (((PromiseMessage) em).getPromise()
                                            .getValueForSnapshot() != em.getArgs()[0]) {
              // non promise deviating from promise result
              Output.println("PROBLEM " + ((PromiseMessage) em).getPromise()
                  + " " + em.getArgs()[0]);
            }
          }

          if (em.getResolver() != null && em.getResolver().getPromise().isCompleted()
              && em.getArgs()[0] instanceof SClass) {
            // Constructor Message
            SPromise cp = em.getResolver().getPromise();
            // need to unresolve this promise...
            cp.unresolveFromSnapshot(Resolution.UNRESOLVED);
          }

          // Output.println(currentActor + " " + em + " " + em.getMessageId());
          currentActor.sendSnapshotMessage(em);

          ml = locations.poll();
        }
      }

      for (long entry : lostMessages) {
        db.position(entry);
        long messageLoc = db.getLong();
        long promiseLoc = db.getLong();
        long version = db.getLong();

        PromiseMessage pm = (PromiseMessage) db.deserializeWithoutContext(messageLoc);
        SReplayPromise prom = (SReplayPromise) db.deserializeWithoutContext(promiseLoc);
        pm.setIdSnapshot(version);

        if (prom.isCompleted()) {
          // Output.println("unresolving: " + prom);
          prom.unresolveFromSnapshot(Resolution.UNRESOLVED);
        }
        // Output.println(
        // "attaching msg " + pm + " to " + prom + System.identityHashCode(prom) + " " +
        // pm.getMessageId());

        prom.registerOnResolvedSnapshot(pm, prom.isUnresolved());
      }

      for (long entry : lostErrorMessages) {
        db.position(entry);
        long messageLoc = db.getLong();
        long promiseLoc = db.getLong();
        long version = db.getLong();

        PromiseMessage pm = (PromiseMessage) db.deserializeWithoutContext(messageLoc);
        SReplayPromise prom = (SReplayPromise) db.deserializeWithoutContext(promiseLoc);
        pm.setIdSnapshot(version);

        if (prom.isCompleted()) {
          prom.unresolveFromSnapshot(Resolution.UNRESOLVED);
        }
        // Output.println(
        // "attaching msg " + pm + " to " + prom + System.identityHashCode(prom) + " " +
        // pm.getMessageId());

        prom.registerOnErrorSnapshot(pm, prom.isUnresolved());
      }

      for (long entry : lostChains) {
        db.position(entry);
        long chainedLoc = db.getLong();
        long promiseLoc = db.getLong();
        long prio = db.getLong();

        SReplayPromise chained = (SReplayPromise) db.deserializeWithoutContext(chainedLoc);
        SReplayPromise prom = (SReplayPromise) db.deserializeWithoutContext(promiseLoc);
        chained.setPriority(prio);

        if (chained.isCompleted()) {
          chained.unresolveFromSnapshot(Resolution.UNRESOLVED);
        }

        // Output.println("LOST CHAIN " + entry);
        // Output.println("chaining " + chained + System.identityHashCode(chained) + " to "
        // + prom + System.identityHashCode(prom));
        if (prom.isCompleted()) {
          chained.resolveFromSnapshot(prom.getValueForSnapshot(),
              prom.getResolutionStateUnsync(),
              SnapshotBackend.lookupActor(prom.getResolvingActor()), prom.isUnresolved());
        }

        prom.registerChainedPromiseSnapshot(chained,
            true);
      }

      for (long entry : lostResolutions) {
        db.position(entry);
        long resolverLoc = db.getLong();
        long resultLoc = db.getLong();

        long resolvingActor = db.getLong();
        byte resolutionState = db.get();

        SResolver resolver = (SResolver) db.deserializeWithoutContext(resolverLoc);
        Object result = db.deserializeWithoutContext(resultLoc);

        STracingPromise prom = (STracingPromise) resolver.getPromise();
        if (!prom.isCompleted()) {
          // Output.println("LOST RESOLUTION " + System.identityHashCode(prom) + " " + result
          // + " by " + resolvingActor);
          prom.resolveFromSnapshot(result, Resolution.values()[resolutionState],
              SnapshotBackend.lookupActor(resolvingActor), true);
          prom.setResolvingActorForSnapshot(resolvingActor);
        } else {
          // Output.println(System.identityHashCode(prom) +
          // " allready resolved with " + prom.getValueForSnapshot() + " vs " + result);
          // Output.println(System.identityHashCode(prom.getValueForSnapshot()) + " vs "
          // + System.identityHashCode(result));
        }
      }

      for (PromiseMessage em : messagesNeedingFixup) {
        STracingPromise prom = (STracingPromise) em.getPromise();
        if (em.getArgs()[0] instanceof SPromise) {
          Output.println("fixed up message" + em);
          em.resolve(prom.getValueForSnapshot(), prom.getOwner(),
              SnapshotBackend.lookupActor(prom.getResolvingActor()));
        }
      }

      resultPromise = (SPromise) db.getReference(resultPromiseLocation);
      if (resultPromise == null) {
        resultPromise = (SPromise) db.deserialize(resultPromiseLocation);
      }

      classLocations = null;
      messageLocations = null;

      assert resultPromise != null : "The result promise was not found";
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      // prevent usage after closing
      if (db != null) {
        objectcnt = db.getNumObjects();
      }
      db = null;
    }
  }

  public static boolean addPMsg(final EventualMessage msg) {
    return parser.sentPMsgs.add(msg);
  }

  private static void parseSymbols() {
    File symbolFile = new File(VmSettings.TRACE_FILE + ".sym");
    // create mapping from old to new symbol ids
    try (FileInputStream fis = new FileInputStream(symbolFile);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis))) {
      String line = br.readLine();
      while (line != null) {
        String[] a = line.split(":", 2);
        Symbols.addSymbolFor(a[1], Short.parseShort(a[0]));
        line = br.readLine();
      }
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static long getFileOffset(final long address) {
    long threadId = address >> SnapshotBuffer.THREAD_SHIFT;
    assert parser.heapOffsets.containsKey(
        threadId) : "Probably some actor didn't get to finish it's todo list " + threadId + " "
            + address;
    return parser.heapOffsets.get(threadId);
  }

  public static long getVersionForActor(final long actorId) {
    if (parser == null) {
      return 0;
    }
    return parser.actorVersions.get(actorId, 0L);
  }

  public static SObjectWithClass getOuterForClass(final int identity) {
    SObjectWithClass result;

    if (parser.classLocations.containsKey(identity)) {
      long reference = parser.db.readOuterForClass(parser.classLocations.get(identity));
      Object o = parser.db.getReference(reference);
      if (!parser.db.allreadyDeserialized(reference)) {
        result = (SObjectWithClass) parser.db.deserialize(reference);
      } else if (DeserializationBuffer.needsFixup(o)) {
        result = null;
        parser.db.installFixup(new EnclosingObjectFixup(identity), reference);
        // OuterFixup!!
      } else {
        result = (SObjectWithClass) o;
      }
    } else {
      result = Nil.nilObject;
    }
    return result;
  }

  private void ensureRemaining(final int bytes, final ByteBuffer b, final FileChannel channel)
      throws IOException {
    if (b.remaining() < bytes) {
      // need to refill buffer
      b.compact();
      channel.read(b);
      b.flip();
      assert b.remaining() >= bytes;
    }
  }

  public static boolean isTracedMessage(final long location) {
    return parser.messagel.contains(location);
  }

  public static ReplayActor getCurrentActor() {
    assert parser.currentActor != null;
    return parser.currentActor;
  }

  public static void setCurrentActor(final ReplayActor current) {
    parser.currentActor = current;
  }

  public static SPromise getResultPromise() {
    assert parser.resultPromise != null;
    return parser.resultPromise;
  }

  public static DeserializationBuffer getDeserializationBuffer() {
    return parser.db;
  }

  public static int getObjectCnt() {
    return parser.objectcnt;
  }

  public static class EnclosingObjectFixup extends FixupInformation {

    int classId;

    public EnclosingObjectFixup(final int classId) {
      this.classId = classId;
    }

    @Override
    public void fixUp(final Object o) {
      assert o instanceof SObjectWithClass;
      SnapshotBackend.finishCreateSClass(classId, (SObjectWithClass) o);
    }

  }
}