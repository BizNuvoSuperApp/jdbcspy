package jdbcspy.proxy.handler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jdbcspy.proxy.ConnectionFactory;
import jdbcspy.proxy.ConnectionStatistics;
import jdbcspy.proxy.ProxyConnection;
import jdbcspy.proxy.ProxyStatement;
import jdbcspy.proxy.util.Utils;

/**
 * The connection handler.
 */
public class XAConnectionInvocationHandler implements InvocationHandler, ConnectionStatistics {

    /**
     * the logger object for tracing
     */
    private static final Logger mTrace = LogManager.getLogger("jdbcspy.xaconnection");

    /**
     * the underlying connection
     */
    private final XAConnection mConn;

    private final List<ProxyConnection> mConnections = new ArrayList<>();

    private final ConnectionFactory connFac;

    /**
     * The Constructor.
     *
     * @param theConn the original connection
     */
    public XAConnectionInvocationHandler(final XAConnection theConn, final ConnectionFactory connFac) {
        mConn = theConn;
        this.connFac = connFac;
    }

    /**
     * @see InvocationHandler
     */
    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {

        try {
            // implement the toString method
            if (method.getName().equals("toString")) {
                return toString();
            }

            mTrace.trace("call {}.{}", mConn::getClass, () -> Utils.getMethodSignature(method, args));

            switch (method.getName()) {
                case "close" -> {
                    return handleClose(proxy, method, args);
                }
                case "getConnection" -> {
                    final Connection c = (Connection) method.invoke(mConn, args);
                    final ProxyConnection pc = (ProxyConnection) connFac.getProxyConnection(c);

                    mConnections.add(pc);

                    return pc;
                }
                case "getXAResource" -> {
                    final XAResource xa = (XAResource) method.invoke(mConn, args);
                    final XAResource pc = connFac.getProxyXAResource(xa, this);

                    return pc;
                }
                case "dump" -> {
                    return dump();
                }
            }

            return method.invoke(mConn, args);
        }
        catch (final Exception e) {
            mTrace.atError().withThrowable(e).log("unknown error in {}.{} failed for {}", mConn.getClass(), method.getName(), this);
            throw e.getCause();
        }
    }

    public void endTx() {
        for (final ProxyConnection c : mConnections) {
            c.endTx();
        }
    }

    /**
     * Handle the close method.
     *
     * @param proxy  the proxy Object
     * @param method the method
     * @param args   the arguments
     * @return the return object
     * @throws Throwable on error
     */
    private Object handleClose(final Object proxy, final Method method, final Object[] args) throws Throwable {
        mConn.close();
        return null;
    }

    @Override
    public long getDuration() {
        return 0;
    }

    @Override
    public long getSize() {
        return 0;
    }

    @Override
    public int getItemCount() {
        return 0;
    }

    @Override
    public List<ProxyStatement> getStatements() {
        return null;
    }

    @Override
    public String getCaller() {
        return null;
    }

    public String dump() {
        return toString();
    }

    /**
     * Dump the connection.
     *
     * @return String
     */
    @Override
    public String toString() {
        synchronized (mConnections) {
            final StringBuilder strb = new StringBuilder();
            strb.append("XAConnection[\n");
            int i = 0;
            for (final ProxyConnection c : mConnections) {
                if (i++ > 0) {
                    strb.append(",\n");
                }
                strb.append(c.toString());
            }
            strb.append("\n]");

            return strb.toString();
        }
    }

}
