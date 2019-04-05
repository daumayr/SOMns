package tools.snapshot.nodes;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.GenerateNodeFactory;
import com.oracle.truffle.api.dsl.Specialization;

import som.interpreter.SomLanguage;
import som.interpreter.actors.SFarReference;
import som.vm.constants.Classes;
import som.vm.constants.Nil;
import som.vmobjects.SClass;
import som.vmobjects.SInvokable;
import som.vmobjects.SObjectWithClass;
import som.vmobjects.SObjectWithClass.SObjectWithoutFields;
import som.vmobjects.SSymbol;
import tools.concurrency.TracingActors.ReplayActor;
import tools.concurrency.TracingActors.TracingActor;
import tools.snapshot.SnapshotBackend;
import tools.snapshot.SnapshotBuffer;
import tools.snapshot.SnapshotHeap;
import tools.snapshot.deserialization.DeserializationBuffer;
import tools.snapshot.deserialization.FixupInformation;
import tools.snapshot.deserialization.SnapshotParser;


public abstract class PrimitiveSerializationNodes {

  @GenerateNodeFactory
  public abstract static class StringSerializationNode extends AbstractSerializationNode {

    @Specialization
    public long serialize(final Object o, final SnapshotHeap sh) {
      assert o instanceof String;

      long location = getValueLocation(o);
      if (location != -1) {
        return location;
      }

      String s = (String) o;
      byte[] data = s.getBytes(StandardCharsets.UTF_8);
      SnapshotBuffer vb = getBuffer().getBuffer(data.length + 4);
      int start = vb.addValue(o, Classes.stringClass, data.length + 4);
      int base = start;
      vb.putIntAt(base, data.length);
      vb.putBytesAt(base + 4, data);
      return vb.calculateReferenceB(start);
    }

    @Override
    public Object deserialize(final DeserializationBuffer sb) {
      int len = sb.getInt();
      byte[] b = new byte[len];
      sb.get(b);
      String s = new String(b, StandardCharsets.UTF_8);
      return s;
    }
  }

  @GenerateNodeFactory
  public abstract static class IntegerSerializationNode extends AbstractSerializationNode {

    @Specialization
    public long serialize(final Integer o, final SnapshotHeap sh) {

      long location = getValueLocation((long) o);
      if (location != -1) {
        return location;
      }

      long l = o;
      SnapshotBuffer vb = getBuffer().getBuffer(Long.BYTES);
      int base = vb.addValue((long) o, Classes.integerClass, Long.BYTES);
      vb.putLongAt(base, l);
      return vb.calculateReferenceB(base);
    }

    @Specialization
    public long serialize(final Long o, final SnapshotHeap sh) {
      assert o instanceof Long;

      long location = getValueLocation(o);
      if (location != -1) {
        return location;
      }

      long l = o;
      SnapshotBuffer vb = getBuffer().getBuffer(Long.BYTES);
      int base = vb.addValue(o, Classes.integerClass, Long.BYTES);
      vb.putLongAt(base, l);
      return vb.calculateReferenceB(base);
    }

    @Override
    public Object deserialize(final DeserializationBuffer sb) {
      return sb.getLong();
    }
  }

  @GenerateNodeFactory
  public abstract static class DoubleSerializationNode extends AbstractSerializationNode {

    @Specialization
    public long serialize(final Double o, final SnapshotHeap sh) {
      assert o instanceof Double;

      long location = getValueLocation(o);
      if (location != -1) {
        return location;
      }

      double d = o;
      SnapshotBuffer vb = getBuffer().getBufferObject(Double.BYTES);
      int base = vb.addValue(o, Classes.doubleClass, Double.BYTES);
      vb.putDoubleAt(base, d);
      return vb.calculateReferenceB(base);
    }

    @Override
    public Object deserialize(final DeserializationBuffer sb) {
      return sb.getDouble();
    }
  }

  @GenerateNodeFactory
  public abstract static class BooleanSerializationNode extends AbstractSerializationNode {

    @Specialization
    public long serialize(final Boolean o, final SnapshotHeap sh) {

      long location = getValueLocation(o);
      if (location != -1) {
        return location;
      }

      boolean b = o;
      SnapshotBuffer vb = getBuffer().getBufferObject(1);
      int base = vb.addValue(o, Classes.booleanClass, 1);
      vb.putByteAt(base, (byte) (b ? 1 : 0));
      return vb.calculateReferenceB(base);
    }

    @Override
    public Object deserialize(final DeserializationBuffer sb) {
      return sb.get() == 1;
    }
  }

  @GenerateNodeFactory
  public abstract static class TrueSerializationNode extends AbstractSerializationNode {

    @Specialization
    public long serialize(final boolean o, final SnapshotHeap sh) {
      assert o;

      long location = getValueLocation(o);
      if (location != -1) {
        return location;
      }

      SnapshotBuffer vb = getBuffer().getBufferObject(0);
      int base = vb.addValue(o, Classes.trueClass, 0);
      return vb.calculateReferenceB(base);
    }

    @Override
    public Object deserialize(final DeserializationBuffer sb) {
      return true;
    }
  }

  @GenerateNodeFactory
  public abstract static class FalseSerializationNode extends AbstractSerializationNode {

    @Specialization
    public long serialize(final boolean o, final SnapshotHeap sh) {
      assert !o;

      long location = getValueLocation(o);
      if (location != -1) {
        return location;
      }

      SnapshotBuffer vb = getBuffer().getBufferObject(0);
      int base = vb.addValue(o, Classes.falseClass, 0);
      return vb.calculateReferenceB(base);
    }

    @Override
    public Object deserialize(final DeserializationBuffer sb) {
      return false;
    }
  }

  @GenerateNodeFactory
  public abstract static class SymbolSerializationNode extends AbstractSerializationNode {

    @Specialization
    public long serialize(final SSymbol o, final SnapshotHeap sh) {
      assert o instanceof SSymbol;

      long location = getObjectValueLocation(o);
      if (location != -1) {
        return location;
      }

      SSymbol ss = o;
      SnapshotBuffer vb = getBuffer().getBufferObject(2);
      int base = vb.addValueObject(o, Classes.symbolClass, 2);
      vb.putShortAt(base, ss.getSymbolId());
      return vb.calculateReferenceB(base);
    }

    @Override
    public Object deserialize(final DeserializationBuffer sb) {
      short symid = sb.getShort();
      return SnapshotBackend.getSymbolForId(symid);
    }
  }

  @GenerateNodeFactory
  public abstract static class ClassSerializationNode extends AbstractSerializationNode {

    @Specialization(guards = "cls.isValue()")
    protected long doValueClass(final SClass cls, final SnapshotHeap sh) {

      long location = getObjectValueLocation(cls);
      if (location != -1) {
        return location;
      }
      SnapshotBuffer vb = getBuffer().getBufferObject(Integer.BYTES + Long.BYTES);
      int base = vb.addValueObject(cls, Classes.classClass, Integer.BYTES + Long.BYTES);
      vb.putIntAt(base, cls.getIdentity());
      SObjectWithClass outer = cls.getEnclosingObject();
      vb.putLongAt(base + Integer.BYTES,
          outer.getSOMClass().serialize(outer, sh));

      location = vb.calculateReferenceB(base);
      SnapshotBackend.registerValueClassLocation(cls.getIdentity(), location);
      return location;
    }

    protected TracingActor getMain() {
      CompilerDirectives.transferToInterpreter();
      return (TracingActor) SomLanguage.getCurrent().getVM().getMainActor();
    }

    @Specialization(guards = "!cls.isValue()")
    protected long doNotValueClass(final SClass cls, final SnapshotHeap sh,
        @Cached("getMain()") final TracingActor main) {
      SnapshotBuffer sb = sh.getBufferObject(Integer.BYTES + Long.BYTES);
      int base = sb.addObject(cls, Classes.classClass, Integer.BYTES + Long.BYTES);
      sb.putIntAt(base, cls.getIdentity());

      SObjectWithClass outer = cls.getEnclosingObject();
      assert outer != null;
      TracingActor owner = cls.getOwnerOfOuter();
      if (owner == sh.getOwner().getCurrentActor()) {
        sb.putLongAt(base + Integer.BYTES, outer.getSOMClass().serialize(outer, sh));
      } else {
        if (owner == null) {
          owner = main;
        }
        owner.farReference(outer, sb, base + Integer.BYTES);
      }
      long location = sb.calculateReferenceB(base);
      SnapshotBackend.registerClassLocation(cls.getIdentity(), location);
      return location;
    }

    @Override
    public Object deserialize(final DeserializationBuffer sb) {
      int id = sb.getInt();
      // SObjectWithClass outer = (SObjectWithClass) sb.getReference();
      return SnapshotBackend.lookupClass(id, sb.position() - Integer.BYTES - Integer.BYTES);
    }

    public static long readOuterLocation(final DeserializationBuffer sb) {
      sb.getInt();
      return sb.getLong();
    }
  }

  @GenerateNodeFactory
  public abstract static class SInvokableSerializationNode extends AbstractSerializationNode {

    @Specialization
    public long serialize(final SInvokable o, final SnapshotHeap sh) {

      long location = getObjectValueLocation(o);
      if (location != -1) {
        return location;
      }

      SInvokable si = o;
      SnapshotBuffer vb = getBuffer().getBufferObject(Short.BYTES);
      int base = vb.addObject(si, Classes.methodClass, Short.BYTES);
      vb.putShortAt(base, si.getIdentifier().getSymbolId());
      return vb.calculateReferenceB(base);
    }

    @Override
    public Object deserialize(final DeserializationBuffer sb) {
      short id = sb.getShort();
      SSymbol s = SnapshotBackend.getSymbolForId(id);
      return SnapshotBackend.lookupInvokable(s);
    }
  }

  @GenerateNodeFactory
  public abstract static class NilSerializationNode extends AbstractSerializationNode {

    @Specialization
    public long serialize(final SObjectWithoutFields o, final SnapshotHeap sh) {

      long location = getObjectValueLocation(o);
      if (location != -1) {
        return location;
      }

      SnapshotBuffer vb = getBuffer().getBufferObject(0);
      long base = vb.addValueObject(o, Classes.nilClass, 0);
      return vb.calculateReferenceB(base);
    }

    @Override
    public Object deserialize(final DeserializationBuffer sb) {
      return Nil.nilObject;
    }
  }

  @GenerateNodeFactory
  public abstract static class FarRefSerializationNode extends AbstractSerializationNode {

    @Specialization
    public long serialize(final SFarReference o, final SnapshotHeap sh) {

      long location = getObjectValueLocation(o);
      if (location != -1) {
        return location;
      }

      SnapshotBuffer vb = getBuffer().getBufferObject(Integer.BYTES + Long.BYTES);
      int base =
          vb.addValueObject(o, SFarReference.getFarRefClass(), Integer.BYTES + Long.BYTES);
      TracingActor other = (TracingActor) o.getActor();
      vb.putIntAt(base, other.getActorId());

      // writing the reference is done through this method.
      // actual writing may happen at a later point in time if the object wasn't serialized
      // yetD
      other.farReference(o.getValue(), vb, base + Integer.BYTES);
      return vb.calculateReferenceB(base);
    }

    @Override
    public Object deserialize(final DeserializationBuffer sb) {
      int actorId = sb.getInt();
      TracingActor other = (TracingActor) SnapshotBackend.lookupActor(actorId);
      if (other == null) {
        // no messages recorded for this actor, need to create it here.
        other = ReplayActor.getActorWithId(actorId);
      }

      TracingActor current = SnapshotBackend.getCurrentActor();
      SnapshotParser.setCurrentActor((ReplayActor) other);
      Object value = sb.getReference();
      SnapshotParser.setCurrentActor((ReplayActor) current);

      SFarReference result = new SFarReference(other, value);

      if (DeserializationBuffer.needsFixup(value)) {
        sb.installFixup(new FarRefFixupInformation(result));
      }

      return result;
    }

    private static final class FarRefFixupInformation extends FixupInformation {
      SFarReference ref;

      FarRefFixupInformation(final SFarReference ref) {
        this.ref = ref;
      }

      @Override
      public void fixUp(final Object o) {
        // This may be an alternative to making final fields non-final.
        // Only replay executions would be affected by this.
        try {
          Field field = SFarReference.class.getField("value");
          Field modifiersField = Field.class.getDeclaredField("modifiers");
          modifiersField.setAccessible(true);
          modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
          field.set(ref, o);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
