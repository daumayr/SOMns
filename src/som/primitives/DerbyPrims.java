package som.primitives;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.ForkJoinPool;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.dsl.GenerateNodeFactory;
import com.oracle.truffle.api.dsl.ImportStatic;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.source.SourceSection;

import bd.primitives.Primitive;
import som.compiler.AccessModifier;
import som.interpreter.SomLanguage;
import som.interpreter.actors.Actor;
import som.interpreter.actors.Actor.ActorProcessingThread;
import som.interpreter.actors.EventualMessage;
import som.interpreter.actors.EventualMessage.DirectMessage;
import som.interpreter.actors.EventualMessage.ExternalDirectMessage;
import som.interpreter.actors.EventualSendNode;
import som.interpreter.actors.ReceivedMessage;
import som.interpreter.actors.SFarReference;
import som.interpreter.nodes.MessageSendNode;
import som.interpreter.nodes.MessageSendNode.AbstractMessageSendNode;
import som.interpreter.nodes.dispatch.BlockDispatchNode;
import som.interpreter.nodes.dispatch.BlockDispatchNodeGen;
import som.interpreter.nodes.nary.BinaryComplexOperation.BinarySystemOperation;
import som.interpreter.nodes.nary.QuaternaryExpressionNode;
import som.interpreter.nodes.nary.TernaryExpressionNode;
import som.interpreter.nodes.nary.UnaryExpressionNode;
import som.primitives.arrays.AtPrim;
import som.primitives.arrays.AtPrimFactory;
import som.vm.Symbols;
import som.vm.VmSettings;
import som.vm.constants.Classes;
import som.vm.constants.Nil;
import som.vmobjects.SArray;
import som.vmobjects.SArray.SImmutableArray;
import som.vmobjects.SBlock;
import som.vmobjects.SClass;
import som.vmobjects.SDerbyConnection;
import som.vmobjects.SDerbyConnection.SDerbyPreparedStatement;
import som.vmobjects.SInvokable;
import som.vmobjects.SSymbol;
import tools.concurrency.ActorExecutionTrace;
import tools.concurrency.ActorExecutionTrace.TwoDArrayWrapper;
import tools.concurrency.TracingActors.TracingActor;


public final class DerbyPrims {
  private static final String  DRIVER              = "org.apache.derby.jdbc.EmbeddedDriver";
  private static final short   METHOD_EXEC_PREP_UC = 0;
  private static final short   METHOD_EXEC_PREP_RS = 1;
  private static final short   METHOD_EXEC_UC      = 2;
  private static final short   METHOD_EXEC_RS      = 3;
  private static final SSymbol SELECTOR            = Symbols.symbolFor("value:");

  @GenerateNodeFactory
  @ImportStatic(DerbyPrims.class)
  @Primitive(primitive = "derbyConnectionClass:")
  public abstract static class SetDerbyConnectionClassPrim extends UnaryExpressionNode {
    @Specialization
    public final SClass setClass(final SClass value) {
      SDerbyConnection.setSOMClass(value);
      return value;
    }
  }

  @GenerateNodeFactory
  @ImportStatic(DerbyPrims.class)
  @Primitive(primitive = "derbyPrepStatementClass:")
  public abstract static class SetDerbyPrepStatementClassPrim extends UnaryExpressionNode {
    @Specialization
    public final SClass setClass(final SClass value) {
      SDerbyPreparedStatement.setSOMClass(value);
      return value;
    }
  }

  @GenerateNodeFactory
  @Primitive(primitive = "derbyStart:")
  public abstract static class StartDerbyPrim extends UnaryExpressionNode {
    @Specialization
    @TruffleBoundary
    public final Object doStart(final Object o) {
      if (VmSettings.REPLAY) {
        return o;
      }
      try {
        Class.forName(DRIVER);
      } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      System.out.println("Derby system start");
      return o;
    }
  }

  @GenerateNodeFactory
  @Primitive(primitive = "derbyStop:")
  public abstract static class StopDerbyPrim extends UnaryExpressionNode {
    @Specialization
    @TruffleBoundary
    public final Object doStop(final Object o) {
      if (VmSettings.REPLAY) {
        return o;
      }
      try {
        DriverManager.getConnection("jdbc:derby:;shutdown=true");
      } catch (SQLException e) {
        System.out.println("Derby system shutdown");
      }

      return o;
    }
  }

  @GenerateNodeFactory
  @Primitive(primitive = "derbyGetConnection:ifFail:")
  public abstract static class DerbyGetConnectionPrim extends BinarySystemOperation {
    private static final String        PROTOCOL        = "jdbc:derby:derby/";
    @Child protected BlockDispatchNode dispatchHandler = BlockDispatchNodeGen.create();

    @Specialization
    @TruffleBoundary
    public final Object getConnection(final String dbName, final SBlock fail) {
      if (VmSettings.REPLAY) {
        return new SDerbyConnection(null, vm.getActorPool(), vm.getLanguage());
      }

      try {
        Connection conn =
            DriverManager.getConnection(PROTOCOL + dbName + ";create=true", null);

        return new SDerbyConnection(conn, vm.getActorPool(), vm.getLanguage());
      } catch (SQLException e) {
        return dispatchHandler.executeDispatch(new Object[] {fail,
            Symbols.symbolFor("SQLException" + e.getErrorCode()), e.getMessage()});
      }
    }

    @Specialization
    @TruffleBoundary
    public final Object getConnection(final String dbName, final SFarReference fail) {
      if (VmSettings.REPLAY) {
        return new SDerbyConnection(null, vm.getActorPool(), vm.getLanguage());
      }

      try {
        Connection conn =
            DriverManager.getConnection(PROTOCOL + dbName + ";create=true", null);

        return new SDerbyConnection(conn, vm.getActorPool(), vm.getLanguage());
      } catch (SQLException e) {
        return dispatchHandler.executeDispatch(new Object[] {fail.getValue(),
            Symbols.symbolFor("SQLException" + e.getErrorCode()), e.getMessage()});
      }
    }
  }

  @GenerateNodeFactory
  @ImportStatic(DerbyPrims.class)
  @Primitive(primitive = "derby:prepareStatement:ifFail:")
  public abstract static class DerbyPrepareStatementPrim extends TernaryExpressionNode {
    @Child protected BlockDispatchNode dispatchHandler = BlockDispatchNodeGen.create();

    @Specialization
    @TruffleBoundary
    public final Object getConnection(final SDerbyConnection conn, final String query,
        final SBlock fail) {
      if (VmSettings.REPLAY) {
        return new SFarReference(conn.getDerbyActor(),
            new SDerbyPreparedStatement(null, conn, 0));
      }

      PreparedStatement ps;
      try {
        ps = conn.getConnection().prepareStatement(query);
        return new SFarReference(conn.getDerbyActor(),
            new SDerbyPreparedStatement(ps, conn,
                ps.getParameterMetaData().getParameterCount()));
      } catch (SQLException e) {
        return dispatchHandler.executeDispatch(new Object[] {fail,
            Symbols.symbolFor("SQLException" + e.getErrorCode()), e.getMessage()});
      }
    }
  }

  @TruffleBoundary
  protected static SImmutableArray processResults(final Statement ps)
      throws SQLException {
    ResultSet rs = ps.getResultSet();
    int cols = rs.getMetaData().getColumnCount();
    ArrayList<SArray> results = new ArrayList<>();
    ResultSetMetaData rsmd = rs.getMetaData();
    while (rs.next()) {
      Object[] storage = new Object[cols];
      for (int i = 0; i < cols; i++) {
        switch (rsmd.getColumnType(i + 1)) {
          case java.sql.Types.ARRAY:
            storage[i] = new SImmutableArray(rs.getArray(i + 1), Classes.arrayClass);
            break;
          case java.sql.Types.BIGINT:
          case java.sql.Types.INTEGER:
          case java.sql.Types.TINYINT:
          case java.sql.Types.SMALLINT:
            storage[i] = rs.getLong(i + 1);
            break;
          case java.sql.Types.BOOLEAN:
            storage[i] = rs.getBoolean(i + 1);
            break;
          case java.sql.Types.BLOB:
            storage[i] = rs.getBlob(i + 1);
            break;
          case java.sql.Types.DOUBLE:
          case java.sql.Types.FLOAT:
            storage[i] = rs.getDouble(i + 1);
            break;
          case java.sql.Types.NVARCHAR:
            storage[i] = rs.getNString(i + 1);
            break;
          case java.sql.Types.VARCHAR:
            storage[i] = rs.getString(i + 1);
            break;
          case java.sql.Types.DATE:
            storage[i] = rs.getDate(i + 1).getTime();
            break;
          case java.sql.Types.TIMESTAMP:
            storage[i] = rs.getTimestamp(i + 1).getTime();
            break;
          case java.sql.Types.NULL:
            storage[i] = Nil.nilObject;
            break;
          default:
            storage[i] = rs.getObject(i + 1);
        }
      }
      results.add(new SImmutableArray(storage, Classes.arrayClass));
    }
    return new SImmutableArray(results.toArray(new Object[0]), Classes.arrayClass);
  }

  @TruffleBoundary
  protected static SImmutableArray processResultsandSerialize(final Statement ps,
      final JsonArray jsonArray)
      throws SQLException {
    ResultSet rs = ps.getResultSet();
    int cols = rs.getMetaData().getColumnCount();
    ArrayList<SArray> results = new ArrayList<>();
    ResultSetMetaData rsmd = rs.getMetaData();
    while (rs.next()) {
      Object[] storage = new Object[cols];
      JsonArray subArray = new JsonArray();
      for (int i = 0; i < cols; i++) {
        JsonElement jel;

        Gson gson = new Gson();
        switch (rsmd.getColumnType(i + 1)) {
          case java.sql.Types.ARRAY:
            subArray.add(gson.toJson(rs.getArray(i + 1)));
            storage[i] = new SImmutableArray(rs.getArray(i + 1), Classes.arrayClass);
            break;
          case java.sql.Types.BIGINT:
          case java.sql.Types.INTEGER:
          case java.sql.Types.TINYINT:
          case java.sql.Types.SMALLINT:
            subArray.add(gson.toJson(rs.getLong(i + 1)));
            storage[i] = rs.getLong(i + 1);
            break;
          case java.sql.Types.BOOLEAN:
            subArray.add(gson.toJson(rs.getBoolean(i + 1)));
            storage[i] = rs.getBoolean(i + 1);
            break;
          case java.sql.Types.BLOB:
            subArray.add(gson.toJson(rs.getBlob(i + 1)));
            storage[i] = rs.getBlob(i + 1);
            break;
          case java.sql.Types.DOUBLE:
          case java.sql.Types.FLOAT:
            subArray.add(gson.toJson(rs.getDouble(i + 1)));
            storage[i] = rs.getDouble(i + 1);
            break;
          case java.sql.Types.NVARCHAR:
            subArray.add(gson.toJson(rs.getNString(i + 1)));
            storage[i] = rs.getNString(i + 1);
            break;
          case java.sql.Types.VARCHAR:
            subArray.add(gson.toJson(rs.getString(i + 1)));
            storage[i] = rs.getString(i + 1);
            break;
          case java.sql.Types.DATE:
            subArray.add(gson.toJson(rs.getDate(i + 1).getTime()));
            storage[i] = rs.getDate(i + 1).getTime();
            break;
          case java.sql.Types.TIMESTAMP:
            subArray.add(gson.toJson(rs.getTimestamp(i + 1).getTime()));
            storage[i] = rs.getTimestamp(i + 1).getTime();
            break;
          case java.sql.Types.NULL:
            storage[i] = Nil.nilObject;
            break;
          default:
            subArray.add(gson.toJson(rs.getObject(i + 1)));
            storage[i] = rs.getObject(i + 1);
        }
      }
      jsonArray.add(subArray);
      results.add(new SImmutableArray(storage, Classes.arrayClass));
    }
    return new SImmutableArray(results.toArray(new Object[0]), Classes.arrayClass);
  }

  @TruffleBoundary
  private static RootCallTarget createOnReceiveCallTarget(final SSymbol selector,
      final SourceSection source, final SomLanguage lang) {
    AbstractMessageSendNode invoke = MessageSendNode.createGeneric(SELECTOR, null, source);
    ReceivedMessage receivedMsg = new ReceivedMessage(invoke, SELECTOR, lang);
    return Truffle.getRuntime().createCallTarget(receivedMsg);
  }

  @TruffleBoundary
  protected static final int getUpdateCount(final Statement s) throws SQLException {
    return s.getUpdateCount();
  }

  @TruffleBoundary
  protected static final String jarrToString(final JsonArray jarr) {
    return jarr.toString();
  }

  protected static final void sendExternalMessageRS(final Statement statement,
      final TracingActor actor, final TracingActor targetActor, final SBlock callback,
      final RootCallTarget rct, final ForkJoinPool pool,
      final short method)
      throws SQLException {
    int dataId = actor.getDataId();
    SImmutableArray arg = processResults(statement);
    TwoDArrayWrapper taw = new TwoDArrayWrapper(arg, actor.getActorId(), dataId);
    // String s = jarrToString(jarr);

    // StringWrapper sw = new StringWrapper(s, actor.getActorId(), dataId);
    ((ActorProcessingThread) Thread.currentThread()).addExternalData(taw);

    ExternalDirectMessage msg = new ExternalDirectMessage(targetActor, SELECTOR,
        new Object[] {callback, arg},
        actor, null, rct,
        false, false, method, dataId);
    targetActor.send(msg, pool);
  }

  protected static final void sendExternalMessageUC(final Statement statement,
      final TracingActor actor, final TracingActor targetActor, final SBlock callback,
      final RootCallTarget rct, final ForkJoinPool pool,
      final short method)
      throws SQLException {
    int dataId = actor.getDataId();
    int uc = getUpdateCount(statement);

    byte[] b =
        ActorExecutionTrace.getExtDataByteBuffer(
            actor.getActorId(), dataId,
            4);

    b[12] = (byte) (uc >> 24);
    b[13] = (byte) ((uc >> 16) & 0xFF);
    b[14] = (byte) ((uc >> 8) & 0xFF);
    b[15] = (byte) (uc & 0xFF);

    ((ActorProcessingThread) Thread.currentThread()).addExternalData(b);

    ExternalDirectMessage msg = new ExternalDirectMessage(targetActor, SELECTOR,
        new Object[] {callback, (long) uc},
        actor, null, rct,
        false, false, method, dataId);
    targetActor.send(msg, pool);
  }

  @GenerateNodeFactory
  @ImportStatic(DerbyPrims.class)
  @Primitive(primitive = "derby:executePreparedStatement:callback:ifFail:")
  public abstract static class DerbyExecutePrepareStatementPrim
      extends QuaternaryExpressionNode {
    @Child protected BlockDispatchNode dispatchHandler = BlockDispatchNodeGen.create();
    @Child protected EventualSendNode  msn;
    @Child protected AtPrim            arrayAt         =
        AtPrimFactory.create(null, null);

    @Specialization
    public final Object execute(final SDerbyPreparedStatement statement,
        final SArray parameters,
        final SBlock callback,
        final SBlock fail) {
      return perform(statement, parameters, callback, fail,
          EventualMessage.getActorCurrentMessageIsExecutionOn());
    }

    @Specialization
    public final Object execute(final SDerbyPreparedStatement statement,
        final SFarReference parameters,
        final SFarReference callback,
        final SFarReference fail) {
      return perform(statement, (SArray) parameters.getValue(), (SBlock) callback.getValue(),
          (SBlock) fail.getValue(),
          callback.getActor());
    }

    @Specialization
    public final Object execute(final SDerbyPreparedStatement statement,
        final SFarReference parameters,
        final SFarReference callback,
        final SBlock fail) {
      return perform(statement, (SArray) parameters.getValue(), (SBlock) callback.getValue(),
          fail,
          callback.getActor());
    }

    @TruffleBoundary
    protected final boolean executeStatement(final PreparedStatement ps,
        final SArray parameters,
        final int numParameters) throws SQLException {
      ps.clearParameters();

      for (int i = 1; i <= numParameters; i++) {
        Object o = arrayAt.execute(null, parameters, i);
        ps.setObject(i, o);
      }

      return ps.execute();
    }

    public final Object perform(final SDerbyPreparedStatement statement,
        final SArray parameters,
        final SBlock callback,
        final SBlock fail,
        final Actor targetActor) {
      try {
        PreparedStatement prep = statement.getStatement();
        boolean hasResultSet =
            executeStatement(prep, parameters,
                statement.getNumParameters());

        SInvokable s =
            (SInvokable) Classes.blockClass.lookupMessage(SELECTOR, AccessModifier.PUBLIC);

        RootCallTarget rct = createOnReceiveCallTarget(SELECTOR,
            s.getSourceSection(), statement.getSomLangauge());

        if (VmSettings.ACTOR_TRACING) {
          if (hasResultSet) {
            sendExternalMessageRS(prep, (TracingActor) statement.getDerbyActor(),
                (TracingActor) targetActor, callback, rct,
                statement.getActorPool(), METHOD_EXEC_PREP_RS);
          } else {
            sendExternalMessageUC(prep, (TracingActor) statement.getDerbyActor(),
                (TracingActor) targetActor, callback, rct,
                statement.getActorPool(), METHOD_EXEC_PREP_UC);
          }
        } else {
          if (hasResultSet) {
            SImmutableArray arg = processResults(prep);
            EventualMessage msg = new DirectMessage(targetActor, SELECTOR,
                new Object[] {callback, arg},
                statement.getDerbyActor(), null, rct,
                false, false);
            targetActor.send(msg, statement.getActorPool());
          } else {
            long arg = getUpdateCount(prep);
            EventualMessage msg = new DirectMessage(targetActor, SELECTOR,
                new Object[] {callback, arg},
                statement.getDerbyActor(), null, rct,
                false, false);
            targetActor.send(msg, statement.getActorPool());
          }
        }
      } catch (SQLException e) {
        dispatchHandler.executeDispatch(new Object[] {fail,
            Symbols.symbolFor("SQLException" + e.getErrorCode()), e.getMessage()});
      }
      return statement;
    }
  }

  @GenerateNodeFactory
  @ImportStatic(DerbyPrims.class)
  @Primitive(primitive = "derby:executeStatement:callback:ifFail:")
  public abstract static class DerbyExecuteStatementPrim extends QuaternaryExpressionNode {
    @Child protected BlockDispatchNode dispatchHandler = BlockDispatchNodeGen.create();

    @Specialization
    public final Object execute(final SDerbyConnection connection,
        final String query,
        final SBlock callback,
        final SBlock fail) {
      return perform(connection, query, callback, fail,
          EventualMessage.getActorCurrentMessageIsExecutionOn());
    }

    @Specialization
    public final Object execute(final SDerbyConnection connection,
        final String query,
        final SFarReference callback,
        final SFarReference fail) {
      return perform(connection, query, (SBlock) callback.getValue(), (SBlock) fail.getValue(),
          callback.getActor());
    }

    public final Object perform(final SDerbyConnection connection,
        final String query,
        final SBlock callback,
        final SBlock fail,
        final Actor targetActor) {
      try {
        Statement statement = connection.getConnection().createStatement();
        boolean hasResultSet = statement.execute(query);

        SInvokable s =
            (SInvokable) Classes.blockClass.lookupMessage(SELECTOR, AccessModifier.PUBLIC);
        RootCallTarget rct = createOnReceiveCallTarget(SELECTOR, s.getSourceSection(),
            connection.getLanguage());

        EventualMessage msg;

        if (VmSettings.ACTOR_TRACING) {
          if (hasResultSet) {
            sendExternalMessageRS(statement, (TracingActor) connection.getDerbyActor(),
                (TracingActor) targetActor, callback,
                rct, connection.getActorPool(), METHOD_EXEC_RS);
          } else {
            sendExternalMessageRS(statement, (TracingActor) connection.getDerbyActor(),
                (TracingActor) targetActor, callback,
                rct, connection.getActorPool(), METHOD_EXEC_UC);
          }
        } else {
          if (hasResultSet) {
            SImmutableArray arg = processResults(statement);
            msg = new DirectMessage(targetActor, SELECTOR,
                new Object[] {callback, arg},
                connection.getDerbyActor(), null, rct,
                false, false);
            targetActor.send(msg, connection.getActorPool());
          } else {
            long arg = getUpdateCount(statement);
            msg = new DirectMessage(targetActor, SELECTOR,
                new Object[] {callback, arg},
                connection.getDerbyActor(), null, rct,
                false, false);
            targetActor.send(msg, connection.getActorPool());
          }
        }
      } catch (SQLException e) {
        dispatchHandler.executeDispatch(new Object[] {fail,
            Symbols.symbolFor("SQLException" + e.getErrorCode()), e.getMessage()});
      }
      return connection;
    }
  }

}
