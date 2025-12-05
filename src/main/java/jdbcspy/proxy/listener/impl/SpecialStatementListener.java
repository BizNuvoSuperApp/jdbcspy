package jdbcspy.proxy.listener.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jdbcspy.proxy.StatementStatistics;
import jdbcspy.proxy.listener.CloseEvent;
import jdbcspy.proxy.listener.ExecutionAdapter;
import jdbcspy.proxy.listener.ExecutionEvent;
import jdbcspy.proxy.listener.ExecutionListener;
import jdbcspy.proxy.util.Utils;

/**
 * The Special statement listener.
 */
public class SpecialStatementListener extends ExecutionAdapter {

    /**
     * the logger object for tracing
     */
    private static final Logger mTrace = LogManager.getLogger(SpecialStatementListener.class.getName());

    /**
     * the time map
     */
    private final Map<StatementStatistics, String> mRunningStmts = new HashMap<>();

    /**
     * the history map
     */
    private final Map<String, List<String>> mHistoryMap = new HashMap<>();

    /**
     * @see ExecutionListener#startExecution
     */
    @Override
    public void startExecution(final ExecutionEvent event) {
        final StatementStatistics stmt = event.getStatementStatistics();
        final String regExp = Utils.isHistoryTrace(stmt.getSQL());
        if (regExp != null) {
            synchronized (mRunningStmts) {
                mRunningStmts.put(stmt, regExp);
            }
        }
    }

    /**
     * @see ExecutionListener#closeStatement
     */
    @Override
    public void closeStatement(final CloseEvent event) {
        final String regExp;
        synchronized (mRunningStmts) {
            regExp = mRunningStmts.remove(event.getStatementStatistics());
        }
        if (regExp != null) {
            final StatementStatistics stmt = event.getStatementStatistics();

            mTrace.info(stmt.toString());

            synchronized (mHistoryMap) {
                final List<String> l = mHistoryMap.computeIfAbsent(regExp, k -> new ArrayList<>());
                l.add(stmt.toString());
                if (l.size() > 200) {
                    l.remove(0);
                }
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
        synchronized (mHistoryMap) {
            mHistoryMap.clear();
        }
    }

    /**
     * @see java.lang.Object#toString
     */
    @Override
    public String toString() {
        if (mHistoryMap.isEmpty()) {
            return null;
        }

        final StringBuilder strb = new StringBuilder("[SpecialStatementListener[");
        synchronized (mHistoryMap) {

            for (final Map.Entry<String, List<String>> e : mHistoryMap.entrySet()) {
                final List<String> stmts = e.getValue();

                strb.append("\n  history list for (").append(e.getKey()).append("):\n");

                int i = 1;
                for (final String s : stmts) {
                    strb.append("  ").append(i).append(": ");
                    strb.append(s);
                    strb.append("\n");
                    i++;
                }
            }
            strb.append("]]\n");
        }

        strb.append("]\n");
        return strb.toString();
    }

}
