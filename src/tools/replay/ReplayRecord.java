package tools.replay;

public class ReplayRecord {
  public static class NumberedPassiveRecord extends ReplayRecord {
    public final long        passiveEntityId;
    public final long        eventNo;
    public final TraceRecord type;

    public NumberedPassiveRecord(final long passiveEntityId, final long eventNo,
        final TraceRecord type) {
      this.passiveEntityId = passiveEntityId;
      this.eventNo = eventNo;
      this.type = type;
    }
  }

  public static class IsLockedRecord extends ReplayRecord {
    public final long    lockId;
    public final boolean isLocked;

    public IsLockedRecord(final long lockid, final long isLocked) {
      this.lockId = lockid;
      this.isLocked = isLocked == 1;
    }
  }

  public static class AwaitTimeoutRecord extends ReplayRecord {
    public final boolean isSignaled;

    public AwaitTimeoutRecord(final long isSignaled) {
      this.isSignaled = isSignaled == 1;
    }
  }
}
