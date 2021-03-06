package som;

import java.lang.Thread.UncaughtExceptionHandler;

import tools.concurrency.TracingActivityThread;


/**
 * In case an actor processing thread terminates, provide some info.
 */
public final class UncaughtExceptions implements UncaughtExceptionHandler {

  @Override
  public void uncaughtException(final Thread t, final Throwable e) {
    if (e instanceof ThreadDeath) {
      // Ignore those, we already signaled an error
      return;
    }
    TracingActivityThread thread = (TracingActivityThread) t;
    VM.errorPrintln("Processing failed for: "
        + thread.getActivity().toString());
    e.printStackTrace();
  }
}
