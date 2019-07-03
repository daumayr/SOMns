package tools.snapshot;

import tools.concurrency.TracingActivityThread;


public class DeferredFarRefSerialization {
  public final Object  target;
  final SnapshotBuffer referer;
  final int            referenceOffset;

  public DeferredFarRefSerialization(final SnapshotBuffer referer, final int referenceOffset,
      final Object target) {
    this.target = target;
    this.referer = referer;
    this.referenceOffset = referenceOffset;
  }

  public void resolve(final long targetOffset) {
    if (referer != null) {
      assert targetOffset != 0;
      referer.putLongAt(referenceOffset, targetOffset);
    }
  }

  public boolean isCurrent() {
    if (referer == null || !(Thread.currentThread() instanceof TracingActivityThread)) {
      return true;
    }
    return referer.parent.getSnapshotVersion() == TracingActivityThread.currentThread()
                                                                       .getSnapshotId();
  }
}
