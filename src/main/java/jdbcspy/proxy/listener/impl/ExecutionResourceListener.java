package jdbcspy.proxy.listener.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jdbcspy.proxy.listener.ExecutionAdapter;
import jdbcspy.proxy.listener.ExecutionListener;
import jdbcspy.proxy.listener.ResourceEvent;

/**
 * The Execution resource listener.
 */
public class ExecutionResourceListener extends ExecutionAdapter {

    /**
     * the logger object for tracing
     */
    private static final Logger mTrace = LogManager.getLogger(ExecutionResourceListener.class.getName());

    /**
     * the resource map String-&gt; Integer
     */
    private final Map<String, Entry> mResource;
    private boolean throwWarnings;

    /**
     * Constructor.
     */
    public ExecutionResourceListener() {
        mResource = new LinkedHashMap<>();
    }

    public boolean isThrowWarnings() {
        return throwWarnings;
    }

    public void setThrowWarnings(final boolean throwWarnings) {
        this.throwWarnings = throwWarnings;
    }

    /**
     * @see ExecutionListener#resourceFailure
     */
    @Override
    public void resourceFailure(final ResourceEvent event) {
        synchronized (mResource) {
            Entry entry = mResource.get(event.getOpenMethod());
            if (entry == null) {
                entry = new Entry();
                entry.count = 1;
                entry.cause = event.getCause();

                mResource.put(event.getOpenMethod(), entry);

                if (!throwWarnings) {
                    mTrace.atError().withThrowable(event.getCause()).log("resource failure in {}", event.getMethod());
                }
            }
            else {
                entry.count++;
                if (entry.count % 10 == 0) {
                    mTrace.warn("resource failure in {} occurred {} times", event.getMethod(), entry.count);
                }
            }
        }
    }

    /**
     * @see ExecutionListener#clearStatistics
     */
    @Override
    public void clearStatistics() {
        synchronized (mResource) {
            mResource.clear();
        }
    }

    /**
     * @see java.lang.Object#toString
     */
    @Override
    public String toString() {
        if (mResource.isEmpty()) {
            return null;
        }

        final StringBuilder strb = new StringBuilder("[ExecutionResourceListener[");
        int i = 0;
        synchronized (mResource) {
            for (final Map.Entry<String, Entry> entry : mResource.entrySet()) {
                if (i == 0) {
                    strb.append("\n");
                }
                final Entry e = entry.getValue();
                strb.append("  ").append(i).append(": ").append(e.cause.getMessage()).append(" (#=").append(e.count).append(")\n");
                i++;
            }

        }
        strb.append("]]\n");
        return strb.toString();
    }

    /**
     * The entry class
     */
    private static class Entry {

        /**
         * the count
         */
        private int count;
        /**
         * the cause exception
         */
        private Exception cause;

    }

}
