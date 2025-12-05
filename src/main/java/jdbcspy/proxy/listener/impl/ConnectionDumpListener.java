package jdbcspy.proxy.listener.impl;

import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jdbcspy.proxy.handler.ConnectionInvocationHandler;
import jdbcspy.proxy.listener.ConnectionEvent;
import jdbcspy.proxy.listener.ConnectionListener;
import jdbcspy.proxy.util.Utils;

/**
 * The ConnectionStatisticListener.
 */
public class ConnectionDumpListener implements ConnectionListener {

    /**
     * the logger object for tracing
     */
    private static final Logger mTrace = LogManager.getLogger(ConnectionDumpListener.class.getName());
    private String dbConnDumpClassExp;

    /**
     * @see ConnectionListener#openConnection
     */
    @Override
    public void openConnection(final ConnectionEvent event) {
    }

    /**
     * @see ConnectionListener#openConnection
     */
    @Override
    public void closeConnection(final ConnectionEvent event) {
        final List<String> debug = Arrays.asList(dbConnDumpClassExp.split(","));
        final String regExp = Utils.isTraceClass(debug);

        if (regExp != null) {
            final ConnectionInvocationHandler handler = (ConnectionInvocationHandler) event.getConnectionStatistics();
            mTrace.info("closed connection: {}", handler::dump);
        }
    }

    /**
     * @see ConnectionListener#clearStatistics
     */
    @Override
    public void clearStatistics() {
    }

    public void setConnDumpCloseClassExp(final String exp) {
        dbConnDumpClassExp = exp;
    }

    /**
     * @see java.lang.Object#toString
     */
    @Override
    public String toString() {
        return dbConnDumpClassExp;
    }

}
