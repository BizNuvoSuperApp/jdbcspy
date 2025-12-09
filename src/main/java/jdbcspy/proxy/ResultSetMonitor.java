package jdbcspy.proxy;

import java.lang.ref.WeakReference;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jdbcspy.ClientProperties;
import jdbcspy.proxy.handler.ResultSetInvocationHandler;

public final class ResultSetMonitor {

    private static final Logger mTrace = LogManager.getLogger("jdbcspy.monitor");

    private static final Set<ResultSetTimer> resultSetTimers = Collections.synchronizedSet(new HashSet<>());

    static {
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(
                1, r -> {
                    final var t = new Thread(r);
                    // Set as daemon thread
                    t.setDaemon(true);
                    return t;
                }
        );
        final var checkFrequency = ClientProperties.Field.DB_MONITOR_RESULTSET_FREQUENCY.getLongValue();
        scheduler.scheduleWithFixedDelay(new MonitorRunnable(resultSetTimers), checkFrequency, checkFrequency, TimeUnit.MILLISECONDS);
    }

    private ResultSetMonitor() {
    }

    public static void registerResultSet(final ProxyResultSet proxyRs, final String sql) {
        final var resultSetTimer = new ResultSetTimer(proxyRs, sql);
        ((ResultSetInvocationHandler) Proxy.getInvocationHandler(proxyRs)).setMonitor(resultSetTimer);
        resultSetTimers.add(resultSetTimer);
        mTrace.trace("Registered ResultSet: {}", resultSetTimer);
    }

    public static void unregisterResultSet(final ResultSetTimer resultSetTimer) {
        resultSetTimers.remove(resultSetTimer);
    }

    public static class ResultSetTimer {

        private static final AtomicLong ID_GENERATOR = new AtomicLong();

        private final long id = ID_GENERATOR.getAndIncrement();
        private final WeakReference<ProxyResultSet> proxy;
        private final String sql;
        private final long startTime = System.currentTimeMillis();

        ResultSetTimer(final ProxyResultSet proxy, final String sql) {
            this.proxy = new WeakReference<>(proxy);
            this.sql = sql;
        }

        long getDuration() {
            return System.currentTimeMillis() - startTime;
        }

        ProxyResultSet getProxy() {
            return proxy.get();
        }

        long getId() {
            return id;
        }

        String getSql() {
            return sql;
        }

        @Override
        public boolean equals(final Object o) {
            if (o == null || getClass() != o.getClass())
                return false;

            final ResultSetTimer that = (ResultSetTimer) o;
            return proxy.equals(that.proxy);
        }

        @Override
        public int hashCode() {
            return proxy.hashCode();
        }

        @Override
        public String toString() {
            return String.format("ResultSetTimer[id=%s, proxy=%s, sql=%s]", id, proxy, sql);
        }

    }

    private static class MonitorRunnable implements Runnable {

        private final Set<ResultSetTimer> resultSetTimers;
        private final long timeThresholdValue;
        private final boolean logLeaks;

        MonitorRunnable(final Set<ResultSetTimer> resultSetTimers) {
            this.resultSetTimers = resultSetTimers;
            this.timeThresholdValue = ClientProperties.Field.DB_MONITOR_RESULTSET_TIME_THRESHOLD.getLongValue();
            this.logLeaks = ClientProperties.Field.DB_MONITOR_RESULTSET_LEAK_LOG_ALWAYS.getBooleanValue();
        }

        @Override
        public void run() {
            try {
                final var logMesg = new StringBuilder();
                final var lineSeparator = System.lineSeparator();

                for (final Iterator<ResultSetTimer> iterator = resultSetTimers.iterator(); iterator.hasNext(); ) {
                    final ResultSetTimer timer = iterator.next();
                    final var proxy = timer.getProxy();

                    if (proxy != null) {
                        if (proxy.isClosed()) {
                            iterator.remove();
                        }
                        else if (timer.getDuration() > timeThresholdValue) {
                            final var sql = timer.getSql().trim();
                            final var endOfLine = sql.endsWith("\n") || sql.endsWith("\r") ? "" : lineSeparator;
                            logMesg.append("RS - ").append(timer.id).append(" -- duration: ").append(timer.getDuration()).append("ms, sql: ").append(sql)
                                    .append(endOfLine);
                        }
                    }
                }

                if (!logMesg.isEmpty()) {
                    mTrace.warn("LEAK: Potential Result Set Leaks [[\n{}]]", logMesg);
                }
                else if (mTrace.isTraceEnabled()) {
                    mTrace.trace("LEAK: Open Result Sets [[\n{}]]", logMesg);
                }
                else if (logLeaks) {
                    mTrace.info("LEAK: Open Result Sets [[\n{}]]", logMesg);
                }
            }
            catch (final ConcurrentModificationException t) {
                mTrace.warn("ConcurrentModificationException while monitoring result sets.  Skipping iteration check.");
            }
        }

    }

}
