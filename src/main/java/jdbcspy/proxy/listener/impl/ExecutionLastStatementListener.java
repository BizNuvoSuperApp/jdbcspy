package jdbcspy.proxy.listener.impl;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jdbcspy.proxy.StatementStatistics;
import jdbcspy.proxy.listener.ExecutionAdapter;
import jdbcspy.proxy.listener.ExecutionEvent;
import jdbcspy.proxy.listener.ExecutionListener;

/**
 * The Execution Repeat checker.
 */
public class ExecutionLastStatementListener extends ExecutionAdapter {

    /**
     * the logger object for tracing
     */
    private static final Logger mTrace = LogManager.getLogger(ExecutionLastStatementListener.class.getName());

    private int lastStatementMaxHistory;
    private int lastStatementExecutionThreshold;

    /**
     * the time map
     */
    private Entry[] mEntries;

    /**
     * the current position
     */
    private int mCurrentPos;

    public void setLastStatementMaxHistory(final int l) {
        lastStatementMaxHistory = l;
        mEntries = new Entry[l];
    }

    public void setLastStatementExecutionThreshold(final int l) {
        lastStatementExecutionThreshold = l;
    }

    /**
     * @see ExecutionListener#startExecution
     */
    @Override
    public void startExecution(final ExecutionEvent event) {
        if (lastStatementMaxHistory == 0) {
            return;
        }

        final StatementStatistics stmt = event.getStatementStatistics();

        for (int i = 0; i < lastStatementMaxHistory; i++) {
            // check the statement at mCurrentPos -i
            final Entry e = mEntries[(2 * lastStatementMaxHistory + mCurrentPos - i - 1) % lastStatementMaxHistory];

            if (e != null && stmt.getSQL().equals(e.event)) {
                e.count++;
                e.totalCount++;

                if (e.count >= lastStatementExecutionThreshold) {
                    e.count = 0;
                    mTrace.warn("Statement {} called in method {} was executed {} times in a row.", stmt, stmt.getExecuteCaller(), e.totalCount);
                }

                return;
            }
        }

        // event not found, so store it
        final Entry newEntry = new Entry();
        newEntry.count = 1;
        newEntry.totalCount = 1;
        newEntry.event = stmt.getSQL();
        newEntry.method = stmt.getExecuteCaller();
        mEntries[mCurrentPos] = newEntry;
        mCurrentPos = (mCurrentPos + 1) % lastStatementMaxHistory;
    }

    /**
     * @see ExecutionListener#clearStatistics
     */
    @Override
    public void clearStatistics() {
        if (lastStatementMaxHistory == 0) {
            return;
        }

        mCurrentPos = 0;
        Arrays.fill(mEntries, null);
    }

    /**
     * @see java.lang.Object#toString
     */
    @Override
    public String toString() {
        if (lastStatementMaxHistory == 0) {
            return null;
        }

        final StringBuilder strb = new StringBuilder("[ExecutionLastStatementListener[\n");
        for (int i = 0; i < lastStatementMaxHistory; i++) {
            final Entry e = mEntries[(2 * lastStatementMaxHistory + mCurrentPos - i - 1) % lastStatementMaxHistory];
            if (e != null) {
                strb.append("  ").append(i).append(": \"").append(e.event).append("\" (#=").append(e.count).append(", ").append(e.method).append(")\n");
            }
        }
        strb.append("]]\n");
        return strb.toString();
    }

    /**
     * The entry class.
     */
    private static class Entry {

        /**
         * the event
         */
        String event;
        /**
         * the count
         */
        int count;
        /**
         * the total count
         */
        int totalCount;
        /**
         * the method
         */
        String method;

    }

}
