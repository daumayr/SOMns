package som.primitives;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ForkJoinPool;

import com.oracle.truffle.api.CompilerDirectives.CompilationFinal;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.dsl.GenerateNodeFactory;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.source.SourceSection;

import bd.primitives.Primitive;
import som.compiler.AccessModifier;
import som.interpreter.SomLanguage;
import som.interpreter.actors.Actor;
import som.interpreter.actors.EventualMessage.DirectMessage;
import som.interpreter.actors.EventualMessage.ExternalDirectMessage;
import som.interpreter.actors.ReceivedMessage;
import som.interpreter.actors.SFarReference;
import som.interpreter.nodes.MessageSendNode;
import som.interpreter.nodes.MessageSendNode.AbstractMessageSendNode;
import som.interpreter.nodes.nary.BinaryComplexOperation.BinarySystemOperation;
import som.interpreter.nodes.nary.UnaryExpressionNode.UnarySystemOperation;
import som.vm.Symbols;
import som.vm.VmSettings;
import som.vm.constants.Classes;
import som.vm.constants.Nil;
import som.vmobjects.SBlock;
import som.vmobjects.SInvokable;
import som.vmobjects.SSymbol;
import tools.concurrency.TracingActors.ReplayActor;


public final class TimerPrims {
  @CompilationFinal private static Timer               timer;
  @CompilationFinal private static ForkJoinPool        actorPool;
  @CompilationFinal protected static Actor             timerActor;
  @CompilationFinal protected static RootCallTarget    rct;
  @CompilationFinal private static SomLanguage         language;
  private static final SSymbol                         SELECTOR    =
      Symbols.symbolFor("value");
  private static final HashMap<Integer, SFarReference> targetMap   =
      new HashMap<>();
  @CompilationFinal private static List<Integer>       todolist;
  private static int                                   num         = 0;
  private static final Object                          REPLAY_LOCK = new Object();

  @GenerateNodeFactory
  @Primitive(primitive = "actorSetupTimer:")
  public abstract static class SetupTimerPrim extends UnarySystemOperation {
    @Specialization
    protected final Object setup(final Object o) {
      timer = new Timer();
      actorPool = vm.getActorPool();
      language = vm.getLanguage();
      timerActor = vm.getMainActor();
      SInvokable s =
          (SInvokable) Classes.blockClass.lookupMessage(SELECTOR, AccessModifier.PUBLIC);
      rct = createOnReceiveCallTarget(SELECTOR,
          s.getSourceSection(), language);
      if (VmSettings.REPLAY) {
        ((ReplayActor) vm.getMainActor()).setDataSource(TimerPrims::requestExternalMessage);
        todolist = new ArrayList<>();
      }
      return Nil.nilObject;
    }

    protected static RootCallTarget createOnReceiveCallTarget(final SSymbol selector,
        final SourceSection source, final SomLanguage lang) {
      AbstractMessageSendNode invoke = MessageSendNode.createGeneric(selector, null, source);
      ReceivedMessage receivedMsg = new ReceivedMessage(invoke, selector, lang);
      return Truffle.getRuntime().createCallTarget(receivedMsg);
    }
  }

  @GenerateNodeFactory
  @Primitive(primitive = "actorDo:after:")
  public abstract static class TimerPrim extends BinarySystemOperation {

    @Specialization
    @TruffleBoundary
    public final Object doResolveAfter(final SBlock target,
        final long timeout) {
      return perform(target, vm.getMainActor(), timeout);
    }

    @Specialization
    @TruffleBoundary
    public final Object doResolveAfter(final SFarReference target,
        final long timeout) {
      return perform(target.getValue(), target.getActor(), timeout);
    }

    protected final Object perform(final Object target, final Actor targetActor,
        final long timeout) {
      if (VmSettings.REPLAY) {
        synchronized (REPLAY_LOCK) {
          if (todolist.contains(num)) {
            todolist.remove((Object) num);
            TimerPrims.sendMessage(new SFarReference(targetActor, target));
          } else {
            targetMap.put(num, new SFarReference(targetActor, target));
          }
        }
        num++;
        return true;
      }

      timer.schedule(new TimerTask() {
        int n = num;

        @Override
        public void run() {
          ExternalDirectMessage msg = new ExternalDirectMessage(targetActor, SELECTOR,
              new Object[] {target},
              timerActor, null, rct,
              false, false, (short) 0, n);
          targetActor.send(msg, actorPool);
        }
      }, timeout);
      num++;
      return true;
    }

    public static boolean isTimerThread(final Thread t) {
      // Checkstyle: stop
      return t.getClass().getName() == "java.util.TimerThread";
      // Checkstyle: resume
    }
  }

  public static void requestExternalMessage(final short method,
      final int dataId) {
    assert VmSettings.REPLAY;
    synchronized (REPLAY_LOCK) {
      SFarReference target = targetMap.remove(dataId);
      if (target == null) {
        todolist.add(dataId);
      } else {
        sendMessage(target);
      }
    }
  }

  private static final void sendMessage(final SFarReference target) {
    DirectMessage msg = new DirectMessage(target.getActor(), SELECTOR,
        new Object[] {target.getValue()},
        timerActor, null, rct,
        false, false);
    target.getActor().send(msg, actorPool);
  }
}
