package jdbcspy.proxy;

import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.Statement;

import jdbcspy.ClientProperties;
import jdbcspy.proxy.handler.PreparedStatementInvocationHandler;
import jdbcspy.proxy.handler.StatementInvocationHandler;

/**
 * The statement factory.
 */
public class StatementFactory {

    /**
     * the instance
     */
    private static StatementFactory instance;

    /**
     * Constructor.
     */
    private StatementFactory() {
    }

    /**
     * Get an instance.
     *
     * @return an instance
     */
    public static StatementFactory getInstance() {
        if (instance == null) {
            instance = new StatementFactory();
        }
        return instance;
    }

    /**
     * Get a statement.
     *
     * @param ps  the original statement
     * @param sql the sql code
     * @return Statement the proxy statement
     */
    public Statement getStatement(final Statement ps, final String sql, final String method) {
        final StatementFactory factory = getInstance();
        if (ps instanceof CallableStatement) {
            return factory.getCallableStatementProxy((CallableStatement) ps, sql, method);
        }
        else if (ps instanceof PreparedStatement) {
            return factory.getPreparedStatementProxy((PreparedStatement) ps, sql, method);
        }
        else {
            return factory.getStatementProxy(ps, sql, method);
        }
    }

    /**
     * Get a statement proxy.
     *
     * @param ps  the original statement
     * @param sql the sql code
     * @return the proxy statement
     */
    private Statement getStatementProxy(final Statement ps, final String sql, final String method) {

        final StatementInvocationHandler handler = new StatementInvocationHandler(ps, sql, method);

        handler.setExecutionFailedListener(ClientProperties.getFailedListener());
        handler.setExecutionListener(ClientProperties.getListener());

        return (Statement) Proxy.newProxyInstance(
                ProxyStatement.class.getClassLoader(),
                new Class[] { Statement.class, ProxyStatement.class, StatementStatistics.class }, handler
        );
    }

    /**
     * Get a statement proxy.
     *
     * @param ps  the original statement
     * @param sql the sql code
     * @return the proxy statement
     */
    private PreparedStatement getPreparedStatementProxy(final PreparedStatement ps, final String sql, final String method) {

        final PreparedStatementInvocationHandler handler = new PreparedStatementInvocationHandler(ps, sql, method);

        handler.setExecutionFailedListener(ClientProperties.getFailedListener());
        handler.setExecutionListener(ClientProperties.getListener());

        return (PreparedStatement) Proxy.newProxyInstance(
                ProxyStatement.class.getClassLoader(),
                new Class[] { PreparedStatement.class, ProxyStatement.class, StatementStatistics.class }, handler
        );
    }

    /**
     * Get a statement proxy.
     *
     * @param ps  the original statement
     * @param sql the sql code
     * @return the proxy statement
     */
    private CallableStatement getCallableStatementProxy(final CallableStatement ps, final String sql, final String method) {
        final PreparedStatementInvocationHandler handler = new PreparedStatementInvocationHandler(ps, sql, method);

        handler.setExecutionFailedListener(ClientProperties.getFailedListener());
        handler.setExecutionListener(ClientProperties.getListener());

        return (CallableStatement) Proxy.newProxyInstance(
                ProxyStatement.class.getClassLoader(),
                new Class[] { CallableStatement.class, ProxyStatement.class, StatementStatistics.class }, handler
        );
    }

}
