package jdbcspy.proxy.handler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jdbcspy.ClientProperties;
import jdbcspy.proxy.ProxyResultSet;
import jdbcspy.proxy.ResultSetMonitor;
import jdbcspy.proxy.ResultSetStatistics;
import jdbcspy.proxy.StatementStatistics;
import jdbcspy.proxy.Statistics;
import jdbcspy.proxy.exception.ProxyException;
import jdbcspy.proxy.exception.ResourceNotClosedException;
import jdbcspy.proxy.listener.CloseEvent;
import jdbcspy.proxy.listener.ExecutionEvent;
import jdbcspy.proxy.listener.ExecutionFailedEvent;
import jdbcspy.proxy.listener.ExecutionFailedListener;
import jdbcspy.proxy.listener.ExecutionListener;
import jdbcspy.proxy.listener.ResourceEvent;
import jdbcspy.proxy.util.Utils;

/**
 * The statement handler.
 */
public abstract class AbstractStatementInvocationHandler implements InvocationHandler, StatementStatistics {

    /**
     * open state
     */
    protected static final int OPEN = 1;
    /**
     * executing state
     */
    protected static final int EXECUTING = 2;
    /**
     * executed state
     */
    protected static final int EXECUTED = 3;
    /**
     * closed state
     */
    protected static final int CLOSED = 4;
    /**
     * the logger object for tracing
     */
    protected final Logger mTrace = LogManager.getLogger("jdbcspy.stmt");
    /**
     * the prepared statement
     */
    private final Statement uStatement;
    /**
     * the sql command
     */
    private final String mSql;
    /**
     * the generated result sets
     */
    private final Set<Object> mResultSets = new HashSet<>();

    /**
     * the open method
     */
    private final String mOpenMethod;
    private final Utils utils = new Utils();
    /**
     * result set item count
     */
    protected int mResultSetItemCount;
    /**
     * the sql command that is passed to Statement.executeQuery
     */
    private String mDirectSql;
    /**
     * is the statement closed
     */
    private int mState;
    /**
     * just the execute time
     */
    private long mExecTime;
    /**
     * total duration of iteration
     */
    private long mDuration;
    /**
     * total size of iteration
     */
    private long mSize;
    /**
     * the execution listener
     */
    private List<ExecutionListener> mExecListeners;
    /**
     * the execution failed listener
     */
    private List<ExecutionFailedListener> mExecFailedListeners;
    /**
     * get execute caller
     */
    private String mExecCaller;
    /**
     * the execution start time
     */
    private long mExecStartTime;

    /**
     * Constructor.
     *
     * @param theStmt the original statement
     * @param theSql  the sql string
     * @param method  the method
     */
    public AbstractStatementInvocationHandler(final Statement theStmt, final String theSql, final String method) {
        uStatement = theStmt;
        mSql = theSql;
        mOpenMethod = method;
        mState = OPEN;
    }

    public void setExecutionListener(final List<ExecutionListener> listener) {
        mExecListeners = listener;
    }

    public void setExecutionFailedListener(final List<ExecutionFailedListener> listener) {
        mExecFailedListeners = listener;
    }

    /**
     * @see InvocationHandler
     */
    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {

        try {
            if ("toString".equals(method.getName())) {
                return toString();
            }

            mTrace.trace("call method: {}.{}", uStatement::getClass, () -> Utils.getMethodSignature(method, args));

            if ("close".equals(method.getName())) {
                return handleClose(proxy, method, args, true);
            }
            else if ("checkClosed".equals(method.getName())) {
                handleCheckClosed(proxy);
                return null;
            }
            else if ("isClosed".equals(method.getName())) {
                return mState == CLOSED;
            }
            else if ("getExecutionStartTime".equals(method.getName())) {
                return getExecutionStartTime();
            }
            else if ("getExecutionTime".equals(method.getName())) {
                return getExecutionTime();
            }
            else if ("getExecuteCaller".equals(method.getName())) {
                return getExecuteCaller();
            }
            else if ("getSQL".equals(method.getName())) {
                return getSQL();
            }
            else if ("getDuration".equals(method.getName())) {
                return getDuration();
            }
            else if ("getSize".equals(method.getName())) {
                return getSize();
            }
            else if ("getItemCount".equals(method.getName())) {
                return getItemCount();
            }
            else if ("endTx".equals(method.getName())) {
                handleClose(proxy, null, null, false);
                return true;
            }
            else {
                handle(method, args);
            }

            final boolean exe = method.getName().startsWith("execute");
            if (exe || method.getName().startsWith("getResult")) {
                if (exe && args != null && args.length == 1 && ((String) args[0]).startsWith("dbproxy ")) {
                    return handleDbProxy(method.getName(), ((String) args[0]).substring(8));
                }
                else {
                    return handleTimedMethod(proxy, method, args);
                }
            }

            // all other calls
            return method.invoke(uStatement, args);
        }
        catch (final InvocationTargetException e) {
            mTrace.atError().withThrowable(e)
                    .log("execution {}{} failed for {} in method {}", method.getName(), getArgs(args), getSQL(), Utils.getExecClass(proxy));

            final ExecutionFailedEvent event = new ExecutionFailedEvent(toString(), e.getCause());

            for (final ExecutionFailedListener listener : mExecFailedListeners) {
                listener.executionFailed(event);
            }

            throw e.getCause();
        }
        catch (final ProxyException e) {

            final ResourceEvent event = new ResourceEvent(e, e.getOpenMethod(), Utils.getExecClass(proxy));

            for (final ExecutionListener listener : mExecListeners) {
                listener.resourceFailure(event);
            }
            if (ClientProperties.Field.DB_THROW_WARNINGS.getBooleanValue()) {
                throw e;
            }
            return null;
        }
        catch (final Exception e) {
            mTrace.atError().withThrowable(e).log("statement access failed for {}{}", method.getName(), getArgs(args));

            final ExecutionFailedEvent event = new ExecutionFailedEvent(toString(), e);

            for (final ExecutionFailedListener listener : mExecFailedListeners) {
                listener.executionFailed(event);
            }

            throw e;
        }
    }

    private Object handleDbProxy(final String method, String cmd) {
        mTrace.info("execute dbproxy command '{}'", cmd);

        try {
            if (cmd.startsWith("get ")) {
                final String key = cmd.substring(4);
                final Object value = ClientProperties.getProperty(key);
                mTrace.info("Proxy property {}={}", key, value);

                if (value == null) {
                    return Boolean.FALSE;
                }
            }
            else if (cmd.startsWith("set ")) {
                cmd = cmd.substring(4);
                final int pos = cmd.indexOf(" ");
                final String key = cmd.substring(0, pos);
                final String value = cmd.substring(pos + 1);

                if (ClientProperties.getBooleanKeys().contains(key)) {
                    ClientProperties.setProperty(key, Boolean.valueOf(value));
                }
                else if (ClientProperties.getIntKeys().contains(key)) {
                    ClientProperties.setProperty(key, Integer.valueOf(value));
                }
                else if (ClientProperties.getLongKeys().contains(key)) {
                    ClientProperties.setProperty(key, Integer.valueOf(value));
                }
                else if (ClientProperties.getListKeys().contains(key)) {
                    ClientProperties.setProperty(key, value);
                }
                else {
                    mTrace.info("key {} does not exist.", key);
                    return Boolean.FALSE;
                }
            }

            if (method.equals("executeQuery")) {
                return null;
            }
            else {
                return Boolean.TRUE;
            }
        }
        catch (final Exception e) {
            if (method.equals("executeQuery")) {
                return null;
            }
            else {
                return Boolean.FALSE;
            }
        }
    }

    protected void handle(final Method method, final Object[] args) throws SQLException {
    }

    /**
     * Handle the close method.
     *
     * @param proxy  the proxy
     * @param method the method
     * @param args   the arguments
     * @return the return value
     * @throws Throwable on error
     */
    protected Object handleClose(final Object proxy, final Method method, final Object[] args, final boolean checkClosed) throws Throwable {

        if (mState == CLOSED) {
            return false;
        }

        mState = CLOSED;
        Object ret = null;

        try {
            if (method != null) {
                ret = method.invoke(uStatement, args);
            }

            synchronized (mResultSets) {
                if (checkClosed) {
                    for (final Object o : mResultSets) {
                        final ProxyResultSet c = (ProxyResultSet) o;
                        c.checkClosed();
                    }
                }

                for (final Object o : mResultSets) {
                    final Statistics c = (Statistics) o;
                    mDuration += c.getDuration();
                    mSize += c.getSize();
                    mResultSetItemCount += c.getItemCount();
                }
            }

        }
        finally {
            synchronized (mResultSets) {
                mResultSets.clear();
            }

            final CloseEvent event = new CloseEvent(this);

            for (final ExecutionListener listener : mExecListeners) {
                listener.closeStatement(event);
            }
        }

        final boolean displayStmt = mDuration >= ClientProperties.Field.DB_STMT_TOTAL_TIME_THRESHOLD.getLongValue()
                || mSize >= ClientProperties.Field.DB_STMT_TOTAL_SIZE_THRESHOLD.getLongValue();

        if (displayStmt) {
            mTrace.info("{}closed statement {} in {}", method == null ? "implicitly " : "", this, Utils.getExecClass(proxy));
        }

        return ret;
    }

    /**
     * Handle the execute method.
     *
     * @param proxy  the proxy
     * @param method the method
     * @param args   the arguments
     * @return Object the return object
     * @throws Throwable on error
     */
    private Object handleTimedMethod(final Object proxy, final Method method, final Object[] args) throws Throwable {
        Object result;
        final long start;
        ExecutionEvent event = null;

        try {
            if (method.getName().startsWith("execute") && args != null && args.length > 0) {
                args[0] = (ClientProperties.Field.DB_REMOVE_HINTS.getBooleanValue() ? Utils.removeHints(args[0].toString()) : args[0].toString());
                mDirectSql = (String) args[0];
            }

            mExecCaller = Utils.getExecClass(proxy);
            mExecStartTime = System.currentTimeMillis();
            mState = EXECUTING;

            event = new ExecutionEvent(this);

            for (final ExecutionListener listener : mExecListeners) {
                listener.startExecution(event);
            }
        }
        catch (final RuntimeException e) {
            mTrace.error("failed ", e);
        }

        Object retObject;
        start = System.currentTimeMillis();
        long dur = 0;

        try {
            result = method.invoke(uStatement, args);

            dur = (System.currentTimeMillis() - start);
            mState = EXECUTED;
            retObject = result;

            if (result instanceof ResultSet) {
                final ResultSet proxyRs = getResultSetProxy((ResultSet) result, getSQL(), Utils.getExecClass(proxy));

                if (proxyRs instanceof ProxyResultSet) {
                    synchronized (mResultSets) {
                        mResultSets.add(proxyRs);
                    }

                    ResultSetMonitor.registerResultSet((ProxyResultSet) proxyRs, getSQL());
                }

                retObject = proxyRs;
            }
            else if ("executeUpdate".equals(method.getName())) {
                final Integer upd = (Integer) result;
                mResultSetItemCount += upd;
            }
        }
        finally {
            mState = EXECUTED;

            mDuration += dur;
            mExecTime += dur;

            for (final ExecutionListener listener : mExecListeners) {
                listener.endExecution(event);
            }
        }

        boolean infoLevel = dur >= ClientProperties.Field.DB_STMT_EXECUTE_TIME_THRESHOLD.getLongValue();

        if (!infoLevel) {
            infoLevel = (Utils.isTrace(getSQL()) != null);
        }

        if (infoLevel) {
            mTrace.info(getPrintString(method.getName(), result, dur, mExecCaller));
        }
        else if (mTrace.isTraceEnabled()) {
            mTrace.trace(getPrintString(method.getName(), result, dur, mExecCaller));
        }

        return retObject;
    }

    /**
     * Get the print string.
     *
     * @param method     Method
     * @param result     Object
     * @param dur        long
     * @param methodCall the method call
     * @return String
     */
    private String getPrintString(final String method, final Object result, final long dur, final String methodCall) {
        final StringBuilder txt = new StringBuilder("finished " + method + " in " + Utils.getTimeString(dur) + " (" + getSQL() + ")");

        if (result instanceof Boolean) {
            txt.append(": ");
            txt.append(result);
        }
        else if (result instanceof Number) {
            txt.append(": #ret=");
            txt.append(result);
        }
        else if (result instanceof int[]) {
            final int[] res = (int[]) result;
            txt.append(": [");
            if (res.length == 1) {
                txt.append(res[0]);
            }
            else if (res.length > 1) {
                boolean allSame = true;
                final int r = res[0];
                for (int i = 1; i < res.length; i++) {
                    if (res[i] != r) {
                        allSame = false;
                        break;
                    }
                }

                if (!allSame) {
                    for (int i = 0; i < res.length; i++) {
                        if (i != 0) {
                            txt.append(", ");
                        }
                        txt.append(res[i]);
                    }
                }
                else {
                    txt.append(r).append(",... #=").append(res.length);
                }
            }
            txt.append("]");
        }

        txt.append(" in method ").append(methodCall);

        return txt.toString();
    }

    /**
     * Get a ResultSet proxy.
     *
     * @param rs         the original ResultSet
     * @param sql        the sql command
     * @param openMethod the open method
     * @return ResultSet
     */
    private ResultSet getResultSetProxy(final ResultSet rs, final String sql, final String openMethod) {
        final InvocationHandler handler = new ResultSetInvocationHandler(rs, sql, openMethod);

        return (ResultSet) Proxy.newProxyInstance(
                ProxyResultSet.class.getClassLoader(),
                new Class[] { ResultSet.class, ProxyResultSet.class, ResultSetStatistics.class }, handler
        );
    }

    /**
     * Handle the toString method.
     *
     * @return String
     */
    @Override
    public String toString() {
        return "\"" + getSQL() + "\"" + (mState != OPEN
                ? " (" + Utils.getTimeString(getExecutionTime()) + (mState != EXECUTING ? " + " + Utils.getTimeString(getDuration() - getExecutionTime()) : "")
                + "; #=" + getItemCount() + (getSize() > 0 ? "; size=" + Utils.getSizeString(getSize()) : "") + ") "
                : " ") + (mState != OPEN ? (mState == EXECUTING ? "executing" : "executed") + " since " + utils.MILLI_TIME_FORMATTER.format(
                new Date(getExecutionStartTime())) + " in " + getExecuteCaller() : " not executed");
    }

    /**
     * Handle checkClosed method.
     *
     * @param proxy the proxy
     * @throws ProxyException if a resource was not closed or double closed
     */
    private void handleCheckClosed(final Object proxy) throws ProxyException {

        if (!ClientProperties.Field.DB_IGNORE_NOT_CLOSED_OBJECTS.getBooleanValue() && mState != CLOSED) {

            final String txt =
                    "The statement \"" + getSQL() + "\" opened in " + mOpenMethod + " (connection closed in " + Utils.getExecClass(proxy) + ") was not closed.";
            final ResourceNotClosedException proxyExc = new ResourceNotClosedException(txt);

            proxyExc.setOpenMethod(mOpenMethod);
            throw proxyExc;
        }
    }

    /**
     * return the execute caller.
     *
     * @return String
     */
    @Override
    public String getExecuteCaller() {
        return mExecCaller;
    }

    /**
     * Get the execution start time.
     *
     * @return long
     */
    @Override
    public long getExecutionStartTime() {
        return mExecStartTime;
    }

    /**
     * Get the execution time.
     *
     * @return long
     */
    @Override
    public long getExecutionTime() {
        if (mState == EXECUTING) {
            return (System.currentTimeMillis() - mExecStartTime);
        }
        else {
            return mExecTime;
        }
    }

    /**
     * Get the SQL Code.
     *
     * @return the sql code
     */
    @Override
    public String getSQL() {
        String sql;
        if (mSql == null) {
            sql = (mDirectSql != null ? mDirectSql : "");
        }
        else {
            sql = mSql;
        }

        final int maxLen = ClientProperties.Field.DB_DISPLAY_SQL_STRING_MAXLEN.getIntValue();
        if (maxLen > 0 && sql.length() > maxLen) {
            sql = sql.substring(0, maxLen) + "...";
        }
        return sql;
    }

    /**
     * Get the total duration.
     *
     * @return long
     */
    @Override
    public long getDuration() {
        if (mState == EXECUTING) {
            return (System.currentTimeMillis() - mExecStartTime);
        }
        else if (mState == EXECUTED) {
            long l = 0;
            synchronized (mResultSets) {
                for (final Object mResultSet : mResultSets) {
                    final Statistics c = (Statistics) mResultSet;
                    l += c.getDuration();
                }
            }
            return l + mDuration;
        }

        return mDuration;
    }

    /**
     * Get the size.
     *
     * @return long
     */
    @Override
    public long getSize() {
        if (mState != CLOSED) {
            long l = 0;
            synchronized (mResultSets) {
                for (final Object mResultSet : mResultSets) {
                    final Statistics c = (Statistics) mResultSet;
                    l += c.getSize();
                }
            }
            return l + mSize;
        }
        return mSize;
    }

    /**
     * Get the item count.
     *
     * @return int
     */
    @Override
    public int getItemCount() {
        if (mState != CLOSED) {
            int l = 0;
            synchronized (mResultSets) {
                for (final Object mResultSet : mResultSets) {
                    final Statistics c = (Statistics) mResultSet;
                    l += c.getItemCount();
                }
            }
            return l;
        }
        return mResultSetItemCount;
    }

    protected int getState() {
        return mState;
    }

    /**
     * Get arguments.
     *
     * @param args Object[]
     * @return String
     */
    private String getArgs(final Object[] args) {
        final StringBuilder strb = new StringBuilder("(");
        for (int i = 0; args != null && i < args.length; i++) {
            if (i > 0) {
                strb.append(",");
            }
            strb.append(args[i]);
        }
        strb.append(")");
        return strb.toString();
    }

    public String dump() {
        return toString();
    }

}
