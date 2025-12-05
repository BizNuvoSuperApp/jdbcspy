package jdbcspy.proxy;

import java.sql.Connection;

import javax.sql.XAConnection;

/**
 * The proxy connection interface provides a method to access the underlying connection.
 */
public interface ProxyConnection extends Connection, XAConnection, ConnectionStatistics {

    /**
     * Get the underlying connection.
     *
     * @return Connection the connection
     */
    Connection getUnderlyingConnection();

    /**
     * Dump the connection.
     *
     * @return String
     */
    String dump();

    /**
     * end tx.
     */
    void endTx();

}
