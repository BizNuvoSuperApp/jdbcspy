package jdbcspy.proxy.handler;

import java.lang.reflect.Method;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import jdbcspy.ClientProperties;
import jdbcspy.proxy.util.Utils;

/**
 * The statement handler.
 */
public class PreparedStatementInvocationHandler extends AbstractStatementInvocationHandler {

    /**
     * the bind variables
     */
    private final Map<Object, Object> mBindVariables = new HashMap<>();

    /**
     * the batch bind variables
     */
    private final Map<Object, Object> mBatchBindVariables = new HashMap<>();

    private final String mSql;
    /**
     * the batched element size
     */
    private int mBatchedSize;

    /**
     * Constructor.
     *
     * @param theStmt the original statement
     * @param theSql  the sql string
     * @param method  the method
     */
    public PreparedStatementInvocationHandler(final Statement theStmt, final String theSql, final String method) {
        super(theStmt, theSql, method);
        mSql = theSql;
    }

    @Override
    protected void handle(final Method method, final Object[] args) throws SQLException {
        if (method.getName().startsWith("registerOutParameter") && args.length >= 2) {
            mBindVariables.put(args[0], Utils.getTypeName((Number) args[1]));

        }
        else if (method.getName().startsWith("set") && args.length >= 2) {
            handleSet(method, args);
        }
        else if (method.getName().equals("addBatch")) {
            handleAddBatch();
        }
    }

    /**
     * Handle the close method.
     *
     * @param proxy  the proxy
     * @param method the method
     * @param args   the arguments
     * @return the return value
     * @throws Throwable on error
     */
    @Override
    protected Object handleClose(final Object proxy, final Method method, final Object[] args, final boolean checkClosed) throws Throwable {

        final Object obj = super.handleClose(proxy, method, args, checkClosed);
        mResultSetItemCount += mBatchedSize;
        return obj;
    }

    /**
     * Get the SQL Code.
     *
     * @return the sql code
     */
    @Override
    public String getSQL() {

        final String[] s = mSql.split("\\?");
        final StringBuilder result = new StringBuilder();
        int i;
        for (i = 0; i < s.length; i++) {
            if (i > 0) {
                final Object obj = getBindValue(i);
                if (obj != null) {
                    result.append(obj);
                }
            }
            result.append(s[i]);
        }

        final Object obj = getBindValue(i);
        if (obj != null) {
            result.append(obj);
        }
        String sql = result.toString();

        final int maxLen = ClientProperties.Field.DB_DISPLAY_SQL_STRING_MAXLEN.getIntValue();
        if (maxLen > 0 && sql.length() > maxLen) {
            sql = sql.substring(0, maxLen) + "...";
        }
        return sql;
    }

    /**
     * Get the item count.
     *
     * @return int
     */
    @Override
    public int getItemCount() {
        final int x = super.getItemCount();
        if (getState() != CLOSED) {
            return x + mBatchedSize;
        }
        return x;
    }

    /**
     * Handle the addBatch method
     */
    protected void handleAddBatch() {
        mBatchedSize++;
        if (mBatchedSize > 100) {
            // heuristic: do not gather more that 100 elements
            return;
        }
        for (final Map.Entry entry : mBindVariables.entrySet()) {
            List l = (List) mBatchBindVariables.get(entry.getKey());
            if (l == null) {
                l = new ArrayList();
                mBatchBindVariables.put(entry.getKey(), l);
            }
            l.add(entry.getValue());
        }
    }

    /**
     * Get the bind value.
     *
     * @param i int
     * @return Object
     */
    protected Object getBindValue(final int i) {
        if (mBatchedSize == 0) {
            return mBindVariables.get(i);
        }
        else {
            final StringBuilder strb = new StringBuilder("{");
            final List l = (List) mBatchBindVariables.get(i);
            if (l == null) {
                return null;
            }

            int c = 0;
            for (final Iterator it = l.iterator(); c < 10 && it.hasNext(); c++) {
                if (c > 0) {
                    strb.append(", ");
                }
                strb.append(it.next());
            }

            if (l.size() > 10) {
                strb.append(", ... #=").append(mBatchedSize);
            }
            strb.append("}");
            return strb.toString();
        }
    }

    /**
     * Handle the setXXX method.
     *
     * @param method Method
     * @param args   Object[]
     * @throws SQLException on sql exception
     */
    private void handleSet(final Method method, final Object[] args) throws SQLException {
        mBindVariables.put(args[0], getArgName(method, args[1]));
    }

    /**
     * Get the argument name.
     *
     * @param method the method
     * @param arg    the argument
     * @return the printable name
     * @throws SQLException on error
     */
    private String getArgName(final Method method, final Object arg) throws SQLException {

        if ("setNull".equals(method.getName())) {
            return "NULL";
        }
        return getArgName(arg);
    }

    /**
     * Get the argument name.
     *
     * @param arg Object
     * @return String
     * @throws SQLException if the access to the array object fails
     */
    private String getArgName(final Object arg) throws SQLException {

        if (arg == null) {
            return "NULL";
        }
        else if (arg instanceof String) {
            return "'" + arg + "'";
        }
        else if (arg instanceof byte[]) {
            final byte[] b = (byte[]) arg;
            final StringBuilder strb = new StringBuilder("'");
            for (int i = 0; i < b.length; i++) {
                strb.append(Integer.toHexString(b[i] < 0 ? b[i] + 256 : b[i]));
                // heuristic to shortcut arrays
                if (i >= 100 && b.length > 120) {
                    strb.append(", ... #=").append(b.length);
                    break;
                }
            }
            strb.append("'");
            return strb.toString();
        }
        else if (arg instanceof Array) {
            final Array a = (Array) arg;
            final Object[] o = (Object[]) a.getArray();
            final StringBuilder strb = new StringBuilder("[");
            for (int i = 0; i < o.length; i++) {
                if (i > 0) {
                    strb.append(", ");
                }
                strb.append(getArgName(o[i]));
                // heuristic to shortcut arrays
                if (i >= 100 && o.length > 120) {
                    strb.append(", ... #=").append(o.length);
                    break;
                }
            }
            strb.append("]");
            return strb.toString();
        }
        else {
            return arg.toString();
        }
    }

}
