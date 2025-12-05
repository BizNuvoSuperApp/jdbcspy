package jdbcspy.proxy.handler;

import java.sql.Statement;

/**
 * The statement handler.
 */
public class StatementInvocationHandler extends AbstractStatementInvocationHandler {

    /**
     * Constructor.
     *
     * @param theStmt the original statement
     * @param theSql  the sql string
     * @param method  the method
     */
    public StatementInvocationHandler(final Statement theStmt, final String theSql, final String method) {
        super(theStmt, theSql, method);
    }

}
