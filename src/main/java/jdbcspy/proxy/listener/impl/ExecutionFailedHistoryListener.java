package jdbcspy.proxy.listener.impl;

import java.util.ArrayList;
import java.util.List;

import jdbcspy.proxy.listener.ExecutionFailedEvent;
import jdbcspy.proxy.listener.ExecutionFailedListener;

/**
 * The Execution Failed Listener.
 */
public class ExecutionFailedHistoryListener implements ExecutionFailedListener {

    /**
     * the list
     */
    private final List<ExecutionFailedEvent> mList = new ArrayList<>();

    /**
     * @see ExecutionFailedListener#executionFailed
     */
    @Override
    public void executionFailed(final ExecutionFailedEvent event) {
        synchronized (mList) {
            mList.add(event);
        }
    }

    /**
     * @see ExecutionFailedListener#clearStatistics
     */
    @Override
    public void clearStatistics() {
        synchronized (mList) {
            mList.clear();
        }
    }

    /**
     * @see java.lang.Object#toString
     */
    @Override
    public String toString() {
        if (mList.isEmpty()) {
            return null;
        }

        final StringBuilder strb = new StringBuilder("[ExecutionFailedHistoryListener[\n");
        synchronized (mList) {
            int i = 1;
            for (final ExecutionFailedEvent ev : mList) {
                strb.append("  ").append(i).append(": ");
                strb.append(ev.getStatement()).append(" failed, cause: ").append(ev.getCause());
                strb.append("\n");
                i++;
            }
        }
        strb.append("]");
        return strb.toString();
    }

}
