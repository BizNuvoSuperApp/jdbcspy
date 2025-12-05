package jdbcspy.proxy;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jdbcspy.ClientProperties;
import jdbcspy.proxy.handler.ConnectionInvocationHandler;
import jdbcspy.proxy.handler.XAConnectionInvocationHandler;
import jdbcspy.proxy.handler.XAResourceInvocationHandler;
import jdbcspy.proxy.listener.ConnectionEvent;
import jdbcspy.proxy.listener.ConnectionListener;
import jdbcspy.proxy.listener.ExecutionFailedListener;
import jdbcspy.proxy.listener.ExecutionListener;

/**
 * Title: ConnectionFactory
 */
public class ConnectionFactory {

    /**
     * A Logger.
     */
    private static final Logger mTrace = LogManager.getLogger("jdbcspy.connectionfactory");

    private static final AtomicBoolean dumpAfterShutdownThread = new AtomicBoolean(false);
    private static final AtomicBoolean dumpIntervalThread = new AtomicBoolean(false);
    /**
     * shall the proxy be enabled
     */
    private boolean mEnableProxy;

    /**
     * Constructor.
     */
    public ConnectionFactory() {
        /**
         * shall the proxy be enabled
         */
        final boolean mInitiallyEnableProxy = ClientProperties.isInitiallyEnabled();

        if (mInitiallyEnableProxy) {
            mEnableProxy = true;
        }
        else {
            mTrace.info("Disable the ProxyConnectionFactory initially. Using standard connection.");
        }

        if (ClientProperties.Field.DB_DUMP_AFTER_SHUTDOWN.getBooleanValue()) {
            synchronized (dumpAfterShutdownThread) {
                if (!dumpAfterShutdownThread.getAndSet(true)) {
                    final Thread t = new Thread(() -> System.out.println(dumpStatistics()));
                    t.setDaemon(true);
                    Runtime.getRuntime().addShutdownHook(t);
                }
            }
        }

        if (ClientProperties.Field.DB_DUMP_INTERVAL.getLongValue() > 0) {
            synchronized (dumpIntervalThread) {
                if (!dumpIntervalThread.getAndSet(true)) {
                    final Thread t = new Thread(() -> {
                        try {
                            while (true) {
                                Thread.sleep(ClientProperties.Field.DB_DUMP_INTERVAL.getLongValue());
                                System.out.println(dumpStatistics());
                            }
                        }
                        catch (final Exception e) {
                            e.printStackTrace();
                        }
                    });
                    t.setDaemon(true);
                    t.start();
                }
            }
        }
    }

    /**
     * Dump the statistics.
     *
     * @return String
     */
    public static String dumpStatistics() {
        final StringBuilder strb = new StringBuilder();
        for (final ExecutionListener obj : ClientProperties.getListener()) {
            if (obj.toString() != null) {
                strb.append(obj);
                strb.append("\n");
            }
        }
        for (final ConnectionListener obj : ClientProperties.getConnectionListener()) {
            if (obj.toString() != null) {
                strb.append(obj);
                strb.append("\n");
            }
        }
        for (final ExecutionFailedListener obj : ClientProperties.getFailedListener()) {
            if (obj.toString() != null) {
                strb.append(obj);
                strb.append("\n");
            }
        }
        return strb.toString();
    }

    /**
     * Get the connection.
     *
     * @param conn the original connection
     * @return a proxy connection
     */
    public final Connection getProxyConnection(final Connection conn) {

        if (!mEnableProxy) {
            // get standard connection
            return conn;
        }

        final ConnectionInvocationHandler connHandler = new ConnectionInvocationHandler(conn);

        for (final ConnectionListener listener : ClientProperties.getConnectionListener()) {
            connHandler.addConnectionListener(listener);
        }

        final Connection c = (ProxyConnection) Proxy.newProxyInstance(
                ProxyConnection.class.getClassLoader(), new Class[] { ProxyConnection.class }, connHandler);

        final ConnectionEvent event = new ConnectionEvent(connHandler);
        for (final ConnectionListener listener : ClientProperties.getConnectionListener()) {
            listener.openConnection(event);
        }

        return c;
    }

    /**
     * Get the connection.
     *
     * @param conn the original connection
     * @return a proxy connection
     */
    public final XAConnection getProxyXAConnection(final XAConnection conn) {

        if (!mEnableProxy) {
            // get standard connection
            return conn;
        }

        final XAConnectionInvocationHandler connHandler = new XAConnectionInvocationHandler(conn, this);

        return (XAConnection) Proxy.newProxyInstance(ProxyConnection.class.getClassLoader(), new Class[] { ProxyConnection.class }, connHandler);
    }

    /**
     * Get the connection.
     *
     * @param xaResource the original connection
     * @return a proxy connection
     */
    public final XAResource getProxyXAResource(final XAResource xaResource, final XAConnectionInvocationHandler handler) {

        if (!mEnableProxy) {
            // get standard connection
            return xaResource;
        }

        final XAResourceInvocationHandler xaHandler = new XAResourceInvocationHandler(xaResource, handler);

        return (XAResource) Proxy.newProxyInstance(XAResource.class.getClassLoader(), new Class[] { XAResource.class }, xaHandler);
    }

}
