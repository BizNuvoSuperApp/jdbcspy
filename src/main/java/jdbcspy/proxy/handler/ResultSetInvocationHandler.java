package jdbcspy.proxy.handler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jdbcspy.ClientProperties;
import jdbcspy.proxy.ResultSetMonitor;
import jdbcspy.proxy.ResultSetStatistics;
import jdbcspy.proxy.exception.ProxyException;
import jdbcspy.proxy.exception.ResourceNotClosedException;
import jdbcspy.proxy.listener.ExecutionFailedEvent;
import jdbcspy.proxy.listener.ExecutionFailedListener;
import jdbcspy.proxy.listener.ExecutionListener;
import jdbcspy.proxy.listener.ResourceEvent;
import jdbcspy.proxy.util.Utils;

/**
 * The result set handler.
 */
public class ResultSetInvocationHandler implements InvocationHandler, ResultSetStatistics {

    /**
     * the logger object for tracing
     */
    private static final Logger mTrace = LogManager.getLogger("jdbcspy.rs");
    /**
     * the original result set
     */
    private final ResultSet uResultSet;
    /**
     * the sql statement
     */
    private final String mSql;

    /**
     * the open method
     */
    private final String mOpenMethod;

    /**
     * is closed
     */
    private boolean mIsClosed;
    /**
     * total duration of iteration
     */
    private long mDuration;
    /**
     * total length
     */
    private long mSize;
    /**
     * the item count
     */
    private int mItemCount;

    private ResultSetMonitor.ResultSetTimer resultSetTimer;

    /**
     * Constructor.
     *
     * @param rs         ResultSet
     * @param sql        String
     * @param openMethod the method
     */
    public ResultSetInvocationHandler(final ResultSet rs, final String sql, final String openMethod) {
        uResultSet = rs;
        mSql = sql;
        mOpenMethod = openMethod;
    }

    /**
     * @see InvocationHandler
     */
    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {

        final String methodName = method.getName();

        try {
            if (methodName.startsWith("get") && args != null && args.length > 0) {
                return handleGet(method, args);
            }

            if ("next".equals(methodName)) {
                return handleNext(method, args);
            }

            if ("close".equals(methodName)) {
                handleClose(proxy);
            }
            // Checkable Interface implementation
            else if ("checkClosed".equals(methodName)) {
                handleCheckClosed(proxy);
                return null;
            }
            else if ("isClosed".equals(method.getName())) {
                return mIsClosed;
            }

            // ResultSetStatitics Interface implementation
            else if ("getItemCount".equals(methodName)) {
                return mItemCount;
            }
            else if ("getDuration".equals(methodName)) {
                return mDuration;
            }
            else if ("getSize".equals(methodName)) {
                return mSize;
            }

            // remaining calls
            return method.invoke(uResultSet, args);
        }
        catch (final InvocationTargetException e) {
            mTrace.atError().withThrowable(e.getCause()).log("result set access failed for {} in {}", () -> mSql, () -> Utils.getMethodSignature(method, args));

            final ExecutionFailedEvent event = new ExecutionFailedEvent(toString(), e.getCause());

            for (final ExecutionFailedListener listener : ClientProperties.getFailedListener()) {
                listener.executionFailed(event);
            }

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
            mTrace.atError().withThrowable(e).log("result set access failed for {} in {}", () -> mSql, () -> Utils.getMethodSignature(method, args));

            final ExecutionFailedEvent event = new ExecutionFailedEvent(toString(), e);

            for (final ExecutionFailedListener listener : ClientProperties.getFailedListener()) {
                listener.executionFailed(event);
            }

            throw e;
        }
    }

    /**
     * Handle the close method.
     *
     * @param proxy the proxy
     */
    private void handleClose(final Object proxy) {
        // may be null if next hasn't been called
        final boolean displayTime = mDuration >= ClientProperties.Field.DB_RESULTSET_TOTAL_TIME_THRESHOLD.getLongValue();
        final boolean displaySize = mSize >= ClientProperties.Field.DB_RESULTSET_TOTAL_SIZE_THRESHOLD.getLongValue();

        if (displayTime || displaySize) {
            mTrace.info(
                    "iteration of resultset closed in {} took {}. {}", () -> Utils.getExecClass(proxy), () -> Utils.getTimeString(mDuration),
                    () -> mSize > 0 ? "(" + Utils.getSizeString(mSize) + ")" : ""
            );
        }

        if (resultSetTimer != null) {
            ResultSetMonitor.unregisterResultSet(resultSetTimer);
            resultSetTimer = null;
        }

        mIsClosed = true;
    }

    /**
     * Handle check closed.
     *
     * @param proxy the proxy
     * @throws ProxyException if a resource was not closed or double closed
     */
    private void handleCheckClosed(final Object proxy) throws ProxyException {
        if (!mIsClosed && !ClientProperties.Field.DB_IGNORE_NOT_CLOSED_OBJECTS.getBooleanValue()) {

            final String txt = "The ResultSet opened in " + mOpenMethod + " was not closed in " + Utils.getExecClass(proxy) + ".";

            final ResourceNotClosedException proxyExc = new ResourceNotClosedException(txt);
            proxyExc.setOpenMethod(mOpenMethod);
            throw proxyExc;
        }
    }

    /**
     * Handle the next method.
     *
     * @param method the method
     * @param args   the arguments
     * @return Object the result object
     * @throws Throwable on error
     */
    private Object handleNext(final Method method, final Object[] args) throws Throwable {

        final long startTime = System.currentTimeMillis();
        try {

            final Boolean b = (Boolean) method.invoke(uResultSet, args);
            if (b) {
                mItemCount++;
            }
            return b;
        }
        finally {
            final long dur = (System.currentTimeMillis() - startTime);

            mDuration += dur;
            if (dur > ClientProperties.Field.DB_RESULTSET_NEXT_TIME_THRESHOLD.getLongValue()) {
                mTrace.info("finished next in {}ms. (loop {})", dur, mItemCount);
            }
        }
    }

    /**
     * Handle the get method.
     *
     * @param method the method
     * @param args   the arguments
     * @return Object the result object
     * @throws Throwable on error
     */
    private Object handleGet(final Method method, final Object[] args) throws Throwable {

        final Object ret;
        try {
            ret = method.invoke(uResultSet, args);
            if (ClientProperties.Field.DB_ENABLE_SIZE_EVALUATION.getBooleanValue()) {
                if (ret instanceof String) {
                    mSize += 2L * ((String) ret).length();
                }
                else if (ret instanceof Integer || ret instanceof Float) {
                    mSize += 4;
                }
                else if (ret instanceof Boolean || ret instanceof Byte) {
                    mSize += 1;
                }
                else if (ret instanceof Long || ret instanceof Date || ret instanceof Time || ret instanceof Double || ret instanceof Timestamp) {
                    mSize += 8;
                }
                else if (ret instanceof Short) {
                    mSize += 2;
                }
                else if (ret instanceof byte[]) {
                    mSize += ((byte[]) ret).length;
                }
                else if (ret != null) {
                    mTrace.atError().withThrowable(new RuntimeException()).log("unknown return type: {}", ret.getClass());
                }
            }

            return ret;
        }
        catch (final InvocationTargetException e) {
            throw e.getCause();
        }
    }

    /**
     * Get the total duration.
     *
     * @return long
     */
    @Override
    public long getDuration() {
        return mDuration;
    }

    /**
     * Get the size.
     *
     * @return long
     */
    @Override
    public long getSize() {
        return mSize;
    }

    /**
     * Get the item count.
     *
     * @return int
     */
    @Override
    public int getItemCount() {
        return mItemCount;
    }

    public String dump() {
        return toString();
    }

    public void setMonitor(final ResultSetMonitor.ResultSetTimer resultSetTimer) {
        this.resultSetTimer = resultSetTimer;
    }

}
