package jdbcspy.proxy.listener.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jdbcspy.proxy.listener.ExecutionAdapter;
import jdbcspy.proxy.listener.ExecutionEvent;
import jdbcspy.proxy.listener.ExecutionListener;

/**
 * The Execution Repeat checker.
 */
public class ExecutionRepeatCountListener extends ExecutionAdapter {

    /**
     * the logger object for tracing
     */
    private static final Logger mTrace = LogManager.getLogger(ExecutionRepeatCountListener.class.getName());
    /**
     * max print size
     */
    private static final int MAX_PRINT_SIZE = 10;
    /**
     * the time map
     */
    private final Map<String, Integer> mMap;
    private int repeatCountStmtSize;
    private int repeatCountThreshold;

    /**
     * Constructor.
     */
    public ExecutionRepeatCountListener() {
        mMap = new HashMap<>();
    }

    /**
     * @see ExecutionListener#startExecution
     */
    @Override
    public void startExecution(final ExecutionEvent event) {
        if (repeatCountStmtSize == 0) {
            return;
        }

        final String stmt = event.getStatementStatistics().getSQL();

        final Integer count;
        final int size;
        synchronized (mMap) {
            count = mMap.get(stmt);

            if (count == null) {
                mMap.put(stmt, 1);
            }
            else {
                mMap.put(stmt, count + 1);

                if ((count + 1) % repeatCountThreshold == 0) {
                    mTrace.warn(
                            "The statement {} in method {} has been executed {} times ", stmt, event.getStatementStatistics().getExecuteCaller(), count + 1);
                }
            }

            size = mMap.size();
        }

        if (size > repeatCountStmtSize) {
            // clear some entries
            int smallest2n = Integer.MAX_VALUE;
            int smallest = Integer.MAX_VALUE;
            int cnt = 0;

            synchronized (mMap) {
                final Collection<Integer> c = mMap.values();

                for (final Integer i : c) {
                    if (i < smallest) {
                        smallest = i;
                        cnt = 1;
                    }
                    else if (i == smallest) {
                        cnt++;
                    }
                    else if (i < smallest2n) {
                        smallest2n = i;
                    }
                }

                // delete at least 15% and maximum 50% of all entries
                if (cnt < 0.85f * mMap.size()) {
                    smallest = smallest2n;
                }

                int maxDelete = mMap.size() / 2;
                for (final Iterator it = mMap.entrySet().iterator(); it.hasNext(); ) {
                    final Map.Entry entry = (Map.Entry) it.next();
                    cnt = (Integer) entry.getValue();
                    if (cnt == smallest) {
                        it.remove();
                        maxDelete--;
                        if (maxDelete == 0) {
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * @see ExecutionListener#clearStatistics
     */
    @Override
    public void clearStatistics() {
        if (repeatCountStmtSize == 0) {
            return;
        }

        synchronized (mMap) {
            mMap.clear();
        }
    }

    /**
     * @see java.lang.Object#toString
     */
    @Override
    public String toString() {
        if (repeatCountStmtSize == 0) {
            return null;
        }

        final StringBuilder strb = new StringBuilder("[ExecutionRepeatCountListener[\n");
        final Set<Map.Entry<String, Integer>> s = new TreeSet<>((e1, e2) -> {
            final int c = e2.getValue() - e1.getValue();
            if (c != 0) {
                return c;
            }
            return e2.getKey().compareTo(e1.getKey());
        });

        synchronized (mMap) {
            s.addAll(mMap.entrySet());
        }

        int count = MAX_PRINT_SIZE;
        for (final Map.Entry<String, Integer> e : s) {
            strb.append("  ").append(MAX_PRINT_SIZE - count + 1).append(": #=").append(e.getValue()).append(": \"").append(e.getKey()).append("\"\n");
            count--;
            if (count <= 0) {
                break;
            }
        }
        strb.append("]]\n");
        return strb.toString();
    }

}
