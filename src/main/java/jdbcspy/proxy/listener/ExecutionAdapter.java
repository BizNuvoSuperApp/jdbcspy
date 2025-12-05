package jdbcspy.proxy.listener;

/**
 * The ExecutionAdapter.
 */
public class ExecutionAdapter implements ExecutionListener {

    /**
     * @see ExecutionListener#startExecution
     */
    @Override
    public void startExecution(final ExecutionEvent event) {
    }

    /**
     * @see ExecutionListener#endExecution
     */
    @Override
    public void endExecution(final ExecutionEvent event) {
    }

    /**
     * @see ExecutionListener#closeStatement
     */
    @Override
    public void closeStatement(final CloseEvent event) {
    }

    /**
     * @see ExecutionListener#resourceFailure
     */
    @Override
    public void resourceFailure(final ResourceEvent event) {
    }

    /**
     * @see ExecutionListener#clearStatistics
     */
    @Override
    public void clearStatistics() {
    }

}
