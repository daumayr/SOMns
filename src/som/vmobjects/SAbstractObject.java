package som.vmobjects;

import com.oracle.truffle.api.CompilerAsserts;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

import som.interop.SomInteropObject;
import som.vm.constants.Nil;
import tools.snapshot.SnapshotBuffer;


@ExportLibrary(InteropLibrary.class)
public abstract class SAbstractObject implements SomInteropObject {

  public abstract SClass getSOMClass();

  public abstract boolean isValue();

  private long snapshotLocation = -1;
  private byte snapshotVersion;

  @Override
  public String toString() {
    CompilerAsserts.neverPartOfCompilation();
    SClass clazz = getSOMClass();
    if (clazz == null) {
      return "an Object(clazz==null)";
    }
    return "a " + clazz.getName().getString();
  }

  @ExportMessage
  public final boolean isNull() {
    return this == Nil.nilObject;
  }

  public long getSnapshotLocation() {
    return snapshotLocation;
  }

  public long getSnapshotLocationAndUpdate(final SnapshotBuffer sb) {
    if (snapshotLocation == -1 || snapshotVersion != sb.getHeap().getSnapshotVersion()) {
      snapshotVersion = sb.getHeap().getSnapshotVersion();
      snapshotLocation = getSOMClass().serialize(this, sb.getHeap());
    }
    return snapshotLocation;
  }

  public byte getSnapshotVersion() {
    return snapshotVersion;
  }

  public void updateSnapshotLocation(final long snapshotLocation, final byte version) {
    this.snapshotLocation = snapshotLocation;
    this.snapshotVersion = version;
  }
}
