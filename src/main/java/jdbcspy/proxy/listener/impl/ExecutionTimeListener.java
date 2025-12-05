package jdbcspy.proxy.listener.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jdbcspy.proxy.StatementStatistics;
import jdbcspy.proxy.listener.CloseEvent;
import jdbcspy.proxy.listener.ExecutionAdapter;
import jdbcspy.proxy.listener.ExecutionEvent;
import jdbcspy.proxy.listener.ExecutionListener;
import jdbcspy.proxy.util.Utils;

/**
 * The Execution checker.
 */
public class ExecutionTimeListener extends ExecutionAdapter {

    /**
     * the logger object for tracing
     */
    private static final Logger mTrace = LogManager.getLogger(ExecutionTimeListener.class.getName());

    /**
     * max history
     */
    private static final int MAX_HISTORY = 15;

    /**
     * the time map
     */
    private final Map<StatementStatistics, Integer> mRunningStmts = new HashMap<>();

    /**
     * the statements that are executed but not closed
     */
    private final Map<StatementStatistics, Integer> mWaiting = new HashMap<>();

    private final Utils utils = new Utils();

    /**
     * the history map
     */
    private final SortedSet<Entry> mHistorySet = new TreeSet<>((l1, l2) -> {
        long c = (l2.duration - l1.duration);
        if (c > 0) {
            return 1;
        }
        if (c < 0) {
            return -1;
        }
        return l1.stmt.compareTo(l2.stmt);
    });

    private Runnable r;
    private int longExecutionThreshold;

    public int getLongExecutionThreshold() {
        return longExecutionThreshold;
    }

    public void setLongExecutionThreshold(final int longExecutionThreshold) {
        this.longExecutionThreshold = longExecutionThreshold;
    }

    /**
     * @see ExecutionListener#startExecution
     */
    @Override
    public void startExecution(final ExecutionEvent event) {
        if (r == null) {
            r = () -> {
                final Set<Map.Entry<StatementStatistics, Integer>> loopSet = new TreeSet<>((e1, e2) -> {
                    final StatementStatistics stmt1 = e1.getKey();
                    final StatementStatistics stmt2 = e2.getKey();

                    return (int) (stmt2.getExecutionStartTime() - stmt1.getExecutionStartTime());
                });

                while (true) {
                    try {
                        Thread.sleep(60000); // 1 min
                        final Date now = new Date();

                        loopSet.clear();
                        synchronized (mRunningStmts) {
                            loopSet.addAll(mRunningStmts.entrySet());
                        }

                        for (final Map.Entry<StatementStatistics, Integer> entry : loopSet) {

                            final StatementStatistics stmt = entry.getKey();
                            final int loop = entry.getValue();

                            final int wait;
                            switch (loop) {
                                case 1:
                                    wait = 1;
                                    break;
                                case 2:
                                    wait = 2;
                                    break;
                                case 3:
                                    wait = 3;
                                    break;
                                case 4:
                                    wait = 5;
                                    break;
                                case 5:
                                    wait = 10;
                                    break;
                                case 6:
                                    wait = 15;
                                    break;
                                case 7:
                                    wait = 20;
                                    break;
                                case 8:
                                    wait = 25;
                                    break;
                                case 9:
                                    wait = 30;
                                    break;
                                case 10:
                                    wait = 45;
                                    break;
                                default: {
                                    wait = (loop <= 15 ? 60 * (loop - 10) : 120 * (loop - 13));
                                }
                            }

                            final long execTime = (now.getTime() - stmt.getExecutionStartTime());
                            if (execTime > (long) longExecutionThreshold * wait) {
                                entry.setValue(loop + 1);
                                printMessage(stmt.getSQL(), loop, execTime, false, stmt.getExecuteCaller());
                            }
                        }
                    }
                    catch (final Exception e) {
                        mTrace.warn("failed", e);
                    }
                }
            };
            final Thread t = new Thread(r, "CheckLongExecThread");
            t.setDaemon(true);
            t.start();
        }
        synchronized (mRunningStmts) {
            mRunningStmts.put(event.getStatementStatistics(), 1);
        }
    }

    /**
     * @see ExecutionListener#endExecution
     */
    @Override
    public void endExecution(final ExecutionEvent event) {
        final Integer loop;
        synchronized (mRunningStmts) {
            loop = mRunningStmts.remove(event.getStatementStatistics());
        }
        synchronized (mWaiting) {
            mWaiting.put(event.getStatementStatistics(), loop);
        }
    }

    /**
     * @see ExecutionListener#closeStatement
     */
    @Override
    public void closeStatement(final CloseEvent event) {

        final StatementStatistics stmt = event.getStatementStatistics();
        final Integer loop;
        synchronized (mWaiting) {
            loop = mWaiting.remove(stmt);
        }
        if (loop != null) {
            final Entry entry = new Entry();

            entry.duration = stmt.getDuration();
            entry.stmt = stmt.toString();

            synchronized (mHistorySet) {
                mHistorySet.add(entry);
                while (mHistorySet.size() > MAX_HISTORY) {
                    mHistorySet.remove(mHistorySet.last());
                }
            }

            if (loop > 1) {
                printMessage(stmt.getSQL(), loop, entry.duration, true, stmt.getExecuteCaller());
            }
        }
    }

    /**
     * @see ExecutionListener#clearStatistics
     */
    @Override
    public void clearStatistics() {
        synchronized (mRunningStmts) {
            mRunningStmts.clear();
        }
        synchronized (mWaiting) {
            mWaiting.clear();
        }
        synchronized (mHistorySet) {
            mHistorySet.clear();
        }
    }

    /**
     * Print the message.
     *
     * @param sql      String
     * @param loop     int
     * @param execTime long
     * @param finished boolean
     * @param method   the method
     */
    private void printMessage(String sql, final int loop, final long execTime, final boolean finished, final String method) {
        if (loop > 1 && sql.length() > 25) {
            sql = sql.substring(0, 25) + "...";
        }
        mTrace.warn(
                "Statement {} called in {}{}", sql, method, finished
                        ? " finished in " + Utils.getTimeString(execTime)
                        : " executing for " + Utils.getTimeString(execTime) + (loop > 1 ? " (loop " + loop + ")" : "")
        );
    }

    /**
     * @see java.lang.Object#toString
     */
    @Override
    public String toString() {
        final StringBuilder strb = new StringBuilder("[ExecutionTimeListener[\n");
        synchronized (mRunningStmts) {
            if (!mRunningStmts.isEmpty()) {
                strb.append("  currently executing:\n");
                int i = 1;
                for (final StatementStatistics stmt : mRunningStmts.keySet()) {

                    strb.append("    ").append(i).append(": ");
                    strb.append(stmt);
                    strb.append("\n");
                    i++;
                }
            }
        }

        synchronized (mWaiting) {
            if (!mWaiting.isEmpty()) {
                strb.append("\n  executed but waiting to be closed:\n");
                int i = 1;
                for (final StatementStatistics stmt : mWaiting.keySet()) {
                    strb.append("    ").append(i).append(": ");
                    strb.append(stmt.toString());
                    strb.append("\n");
                    i++;
                }
            }
        }

        strb.append("\n  long running history (execTime + iterTime):\n");
        synchronized (mHistorySet) {
            if (!mHistorySet.isEmpty()) {
                int i = 1;
                for (final Entry entry : mHistorySet) {
                    strb.append("    ").append(i).append(": ");
                    strb.append(entry.stmt);
                    strb.append("\n");
                    i++;
                }
            }
        }
        strb.append("]]\n");
        return strb.toString();
    }

    private static class Entry {

        long duration;
        String stmt;

    }

}
