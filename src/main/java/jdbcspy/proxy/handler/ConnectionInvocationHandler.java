package jdbcspy.proxy.handler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jdbcspy.ClientProperties;
import jdbcspy.proxy.ConnectionStatistics;
import jdbcspy.proxy.ProxyStatement;
import jdbcspy.proxy.StatementFactory;
import jdbcspy.proxy.Statistics;
import jdbcspy.proxy.exception.ProxyException;
import jdbcspy.proxy.listener.ConnectionEvent;
import jdbcspy.proxy.listener.ConnectionListener;
import jdbcspy.proxy.listener.ExecutionListener;
import jdbcspy.proxy.listener.ResourceEvent;
import jdbcspy.proxy.util.Utils;

/**
 * The connection handler.
 */
public class ConnectionInvocationHandler implements InvocationHandler, ConnectionStatistics {

    /**
     * the logger object for tracing
     */
    private static final Logger mTrace = LogManager.getLogger("jdbcspy.connection");

    /**
     * max statement count
     */
    private static final int MAX_STMT_COUNT = 100;

    /**
     * the underlying connection
     */
    private final Connection uConnection;

    /**
     * all generated statements
     */
    private final List<ProxyStatement> mStatements = new LinkedList<>();
    /**
     * the connection listener list
     */
    private final List<ConnectionListener> mConnectionListener;
    /**
     * the caller
     */
    private final String mCaller;
    private int itemCount;
    /**
     * is closed
     */
    private int mDeletedStmts;

    private int isolationLevel;
    private String url;

    /**
     * The Constructor.
     *
     * @param theConn the original connection
     */
    public ConnectionInvocationHandler(final Connection theConn) {
        uConnection = theConn;

        try {
            isolationLevel = uConnection.getTransactionIsolation();

            if (uConnection.getMetaData() != null) {
                url = uConnection.getMetaData().getURL();
            }
        }
        catch (final SQLException e) {
            mTrace.error("failed", e);
        }

        mConnectionListener = new LinkedList<>();
        mCaller = Utils.getExecClass(this);
    }

    /**
     * @see InvocationHandler
     */
    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {

        try {
            // implement the toString method
            if (method.getName().equals("toString")) {
                return toString();
            }

            mTrace.trace("call {}.{}", uConnection::getClass, () -> Utils.getMethodSignature(method, args));

            if ("close".equals(method.getName())) {
                return handleClose(proxy, method, args, true);
            }
            else if ("getItemCount".equals(method.getName())) {
                return getItemCount();
            }
            else if ("getDuration".equals(method.getName())) {
                return getDuration();
            }
            else if ("getSize".equals(method.getName())) {
                return getSize();
            }
            else if ("getCaller".equals(method.getName())) {
                return getCaller();
            }
            else if ("getStatements".equals(method.getName())) {
                return getStatements();
            }
            else if ("setTransactionIsolation".equals(method.getName())) {
                isolationLevel = (Integer) args[0];
            }
            else if ("dump".equals(method.getName())) {
                return dump();
            }
            else if (method.getName().startsWith("prepare")) {
                return handlePrepare(proxy, method, args);
            }
            else if (method.getName().startsWith("create")) {
                return handleCreate(proxy, method, args);
            }
            else if (method.getName().equals("endTx")) {
                synchronized (mStatements) {
                    mTrace.trace("now closing {} statements", mStatements.size());

                    for (final ProxyStatement s : mStatements) {
                        try {
                            mTrace.trace("endtx {}", s);
                            s.endTx();
                        }
                        catch (final Exception e) {
                            mTrace.atWarn().withThrowable(e).log("fail");
                        }
                    }
                }

                handleClose(proxy, null, null, false);
                return null;
            }
            else if (method.getName().equals("getUnderlyingConnection")) {
                return uConnection;
            }

            return method.invoke(uConnection, args);
        }
        catch (final InvocationTargetException e) {
            mTrace.atError().withThrowable(e.getCause()).log("{} failed for {}", () -> Utils.getMethodSignature(method, args), () -> this);
            throw e.getCause();
        }
        catch (final ProxyException e) {
            final ResourceEvent event = new ResourceEvent(e, e.getOpenMethod(), Utils.getExecClass(proxy));

            for (final ExecutionListener listener : ClientProperties.getListener()) {
                listener.resourceFailure(event);
            }
            if (ClientProperties.Field.DB_THROW_WARNINGS.getBooleanValue()) {
                throw e;
            }
            return null;
        }
        catch (final Exception e) {
            mTrace.atError().withThrowable(e).log("unknown error in {}.{} failed for {}", uConnection.getClass(), method.getName(), this);
            throw new RuntimeException("failed " + e, e);
        }
    }

    /**
     * Handle the prepare method.
     *
     * @param proxy  Object
     * @param method Method
     * @param args   Object[]
     * @return Object
     * @throws Throwable on error
     */
    private Object handlePrepare(final Object proxy, final Method method, final Object[] args) throws Throwable {
        final String sql = (ClientProperties.Field.DB_REMOVE_HINTS.getBooleanValue() ? Utils.removeHints(args[0].toString()) : args[0].toString());

        final Object ob = method.invoke(uConnection, args);
        if (ob instanceof Statement) {
            final Statement proxyStmt = StatementFactory.getInstance().getStatement((Statement) ob, sql, Utils.getExecClass(proxy));

            if (proxyStmt instanceof ProxyStatement) {
                addStatement((ProxyStatement) proxyStmt);
            }

            return proxyStmt;
        }
        else {
            mTrace.error("method failed {};{}", ob, ob.getClass().getName());
        }

        return ob;
    }

    /**
     * Handle Create method
     *
     * @param proxy  Object
     * @param method Method
     * @param args   Object[]
     * @return Object
     * @throws Throwable on error
     */
    private Object handleCreate(final Object proxy, final Method method, final Object[] args) throws Throwable {
        final Object ob = method.invoke(uConnection, args);

        if (ob instanceof Statement) {
            final Statement proxyStmt = StatementFactory.getInstance().getStatement((Statement) ob, null, Utils.getExecClass(proxy));

            if (proxyStmt instanceof ProxyStatement) {
                addStatement((ProxyStatement) proxyStmt);
            }
            return proxyStmt;
        }
        else {
            mTrace.error("method failed {};{}", ob, ob.getClass().getName());
        }

        return ob;
    }

    /**
     * Add a statement.
     *
     * @param stmt Checkable
     */
    private void addStatement(final ProxyStatement stmt) {
        synchronized (mStatements) {
            int x = mStatements.size() - MAX_STMT_COUNT;

            if (x > 10) {
                final Iterator<ProxyStatement> it = mStatements.iterator();

                while (it.hasNext()) {
                    final ProxyStatement c = it.next();

                    if (c.isClosed()) {
                        it.remove();
                        x--;
                        mDeletedStmts++;

                        if (x <= 0) {
                            break;
                        }
                    }
                }
            }

            mStatements.add(stmt);
            itemCount = mStatements.size();
        }
    }

    /**
     * Handle the close method.
     *
     * @param proxy  the proxy Object
     * @param method the method
     * @param args   the arguments
     * @return the return object
     * @throws Throwable on error
     */
    private Object handleClose(final Object proxy, final Method method, final Object[] args, final boolean checkClosed) throws Throwable {
        Object ret = null;

        try {
            final ConnectionEvent event = new ConnectionEvent(this);

            for (final ConnectionListener listener : mConnectionListener) {
                listener.closeConnection(event);
            }

            if (method != null) {
                ret = method.invoke(uConnection, args);
            }

            long duration = 0;
            long size = 0;

            synchronized (mStatements) {
                for (final ProxyStatement mStatement : mStatements) {
                    final Statistics c = (Statistics) mStatement;
                    duration += c.getDuration();
                    size += c.getSize();
                }

                if (checkClosed) {
                    for (final ProxyStatement c : mStatements) {
                        c.checkClosed();
                    }
                }
            }

            // print out
            final boolean displayTime = duration >= ClientProperties.Field.DB_CONN_TOTAL_TIME_THRESHOLD.getLongValue();
            final boolean displaySize = size >= ClientProperties.Field.DB_CONN_TOTAL_SIZE_THRESHOLD.getLongValue();
            final boolean verbose = ClientProperties.Field.VERBOSE.getBooleanValue();

            if (displayTime || displaySize) {
                final Level l = mStatements.isEmpty() ? Level.TRACE : Level.INFO;

                if (verbose) {
                    mTrace.log(l, "{}closed connection\n{}", () -> method == null ? "implicitly " : "", this::dump);
                }
                else {
                    mTrace.log(l, "{}closed connection {} in {}", () -> method == null ? "implicitly " : "", () -> this, () -> Utils.getExecClass(proxy));
                }
            }
        }
        finally {
            synchronized (mStatements) {
                itemCount = mStatements.size();
                mStatements.clear();
            }
        }

        return ret;
    }

    /**
     * Add a connection listener.
     *
     * @param listener ConnectionListener
     */
    public void addConnectionListener(final ConnectionListener listener) {
        mConnectionListener.add(listener);
    }

    /**
     * Remove a connection listener.
     *
     * @param listener ConnectionListener
     */
    public void removeConnectionListener(final ConnectionListener listener) {
        mConnectionListener.remove(listener);
    }

    /**
     * Get the total duration.
     *
     * @return long
     */
    @Override
    public long getDuration() {
        long dur = 0;
        synchronized (mStatements) {
            for (final ProxyStatement mStatement : mStatements) {
                final Statistics c = (Statistics) mStatement;
                dur += c.getDuration();
            }
        }
        return dur;
    }

    /**
     * Get the size.
     *
     * @return long
     */
    @Override
    public long getSize() {
        long size = 0;
        synchronized (mStatements) {
            for (final ProxyStatement mStatement : mStatements) {
                final Statistics c = (Statistics) mStatement;
                size += c.getSize();
            }
        }
        return size;
    }

    /**
     * Get the item count.
     *
     * @return int
     */
    @Override
    public int getItemCount() {
        return itemCount + mDeletedStmts;
    }

    /**
     * Get the statements.
     *
     * @return the statements.
     */
    @Override
    public List<ProxyStatement> getStatements() {
        return new ArrayList<>(mStatements);
    }

    /**
     * The caller of the connection.
     *
     * @return String
     */
    @Override
    public String getCaller() {
        return mCaller;
    }

    /**
     * Get the underlying connection.
     *
     * @return Connection
     */
    public Object getUnderlyingConnection() {
        return uConnection;
    }

    /**
     * Dump the connection.
     *
     * @return String
     */
    public String dump() {
        final StringBuilder strb = new StringBuilder();
        strb.append(this).append(" {").append("\n");

        int i = mDeletedStmts + 1;
        if (mDeletedStmts > 0) {
            strb.append("1 .. ").append(mDeletedStmts).append(": ...\n");
        }
        synchronized (mStatements) {
            for (final ProxyStatement s : mStatements) {
                strb.append(i).append(": ");
                strb.append(s.toString());
                strb.append("\n");
                i++;
            }
        }
        strb.append("}");

        return strb.toString();
    }

    /**
     * Dump the connection.
     *
     * @return String
     */
    @Override
    public String toString() {
        synchronized (mStatements) {
            final StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(
                    "Connection[#stmt=" + getItemCount() + "; duration=" + Utils.getTimeString(getDuration()) + "; isolation=" + Utils.getIsolationLevel(
                            isolationLevel));
            if (url != null) {
                stringBuilder.append("; url=" + url);
            }
            stringBuilder.append((getSize() > 0 ? "; size=" + Utils.getSizeString(getSize()) : "") + ", opened in " + getCaller() + "]");
            return stringBuilder.toString();
        }
    }

}
