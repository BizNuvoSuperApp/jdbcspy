package jdbcspy.proxy.listener;

import jdbcspy.proxy.StatementStatistics;

/**
 * The Close Event class.
 */
public class CloseEvent {

    /**
     * the event source
     */
    private final StatementStatistics mSource;

    /**
     * Constructor.
     *
     * @param source Object
     */
    public CloseEvent(final StatementStatistics source) {
        mSource = source;
    }

    /**
     * Get the source event.
     *
     * @return Object
     */
    public StatementStatistics getStatementStatistics() {
        return mSource;
    }

}
