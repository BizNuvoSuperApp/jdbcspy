package jdbcspy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import jdbcspy.proxy.listener.ConnectionListener;
import jdbcspy.proxy.listener.ExecutionFailedListener;
import jdbcspy.proxy.listener.ExecutionListener;
import jdbcspy.proxy.util.Utils;

/**
 * The Properties class.
 */
public final class ClientProperties {

    /**
     * A Logger.
     */
    private static final Logger mTrace = LogManager.getLogger("jdbcspy.properties");

    /**
     * the db init file
     */
    private static final String DBINIT_FILE = "/jdbcspy/dbinit.xml";
    /**
     * all int values
     */
    private static final List<String> mIntValues = Arrays.stream(Field.values()).filter(it -> Integer.class.isAssignableFrom(it.getFieldClass()))
            .map(Field::getFieldName).toList();
    /**
     * all int values
     */
    private static final List<String> mLongValues = Arrays.stream(Field.values()).filter(it -> Long.class.isAssignableFrom(it.getFieldClass()))
            .map(Field::getFieldName).toList();
    /**
     * all boolean values
     */
    private static final List<String> mBoolValues = Arrays.stream(Field.values()).filter(it -> Boolean.class.isAssignableFrom(it.getFieldClass()))
            .map(Field::getFieldName).toList();
    /**
     * all string values
     */
    private static final List<String> mStringValues = Arrays.stream(Field.values()).filter(it -> String.class.isAssignableFrom(it.getFieldClass()))
            .map(Field::getFieldName).toList();
    /**
     * all list values
     */
    private static final List<String> mListValues = Arrays.stream(Field.values()).filter(it -> List.class.isAssignableFrom(it.getFieldClass()))
            .map(Field::getFieldName).toList();
    /**
     * the instance
     */
    private static ClientProperties instance;

    /**
     * the values
     */
    private final Map<String, Object> values;
    /**
     * the listener list
     */
    private final List<ExecutionListener> mListener = new ArrayList<>();
    /**
     * the listener list
     */
    private final List<ExecutionFailedListener> mFailedListener = new ArrayList<>();
    /**
     * the connection listener list
     */
    private final List<ConnectionListener> mConnectionListener = new ArrayList<>();
    /**
     * The XML handler.
     */
    private final DefaultHandler mHandler = new DefaultHandler() {
        private ConnectionListener connectionListener;
        private ExecutionListener executionListener;
        private ExecutionFailedListener executionFailedListener;

        @Override
        public void startElement(final String uri, final String localName, final String qName, final Attributes attributes) {
            if ("property".equals(qName)) {
                final String name = attributes.getValue("name").trim();
                final String value = attributes.getValue("value").trim();

                if (connectionListener == null && executionFailedListener == null && executionListener == null) {
                    boolean found = false;
                    if (mBoolValues.contains(name)) {
                        values.put(name, Boolean.parseBoolean(value));
                        found = true;
                    }
                    else if (mLongValues.contains(name)) {
                        values.put(name, Long.parseLong(value));
                        found = true;
                    }
                    else if (mIntValues.contains(name)) {
                        values.put(name, Integer.parseInt(value));
                        found = true;
                    }
                    else if (mStringValues.contains(name)) {
                        values.put(name, value);
                        found = true;
                    }
                    else if (mListValues.contains(name)) {
                        final String[] s = value.split(",");
                        final List<String> l = new ArrayList<>(Arrays.asList(s));
                        values.put(name, l);
                        found = true;
                    }

                    if (!found) {
                        mTrace.warn("Could not find the property {}.", name);
                    }
                }
                else {
                    // this must be a value assigned by a listener
                    if (connectionListener != null) {
                        Utils.setProperty(connectionListener, name, value);
                    }
                    else if (executionFailedListener != null) {
                        Utils.setProperty(executionFailedListener, name, value);
                    }
                    else {
                        Utils.setProperty(executionListener, name, value);
                    }
                }
            }
            else if (qName.endsWith("listener")) {
                final String classname = attributes.getValue("class");
                try {
                    final Class<?> c = Class.forName(classname);
                    final Object cl = c.getDeclaredConstructor().newInstance();

                    switch (qName) {
                        case "connectionlistener":
                            mConnectionListener.add((ConnectionListener) cl);
                            connectionListener = (ConnectionListener) cl;
                            executionFailedListener = null;
                            executionListener = null;
                            break;
                        case "executionfailedlistener":
                            mFailedListener.add((ExecutionFailedListener) cl);
                            executionFailedListener = (ExecutionFailedListener) cl;
                            connectionListener = null;
                            executionListener = null;
                            break;
                        case "executionlistener":
                            mListener.add((ExecutionListener) cl);
                            executionListener = (ExecutionListener) cl;
                            connectionListener = null;
                            executionFailedListener = null;
                            break;
                        default:
                            throw new IllegalArgumentException("The listener " + qName + " does not exist.");
                    }

                }
                catch (final Exception e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void endElement(final String uri, final String localName, final String qName) {

            if (qName.endsWith("listener")) {

                connectionListener = null;
                executionFailedListener = null;
                executionListener = null;
            }
        }
    };

    /**
     * Constructor.
     */
    private ClientProperties() {
        values = new LinkedHashMap<>();

        final InputStream input = ClientProperties.class.getResourceAsStream(DBINIT_FILE);

        try {
            final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            parser.parse(input, mHandler);
            input.close();
        }
        catch (final Exception ex) {
            throw new IllegalStateException("something's wrong here with dbinit.xml");
        }

        mTrace.info("initialized {}", values);
    }

    /**
     * Get the instance.
     *
     * @return instance
     */
    private static ClientProperties getInstance() {
        if (instance == null) {
            instance = new ClientProperties();
            instance.init();
        }
        return instance;
    }

    /**
     * Get the property.
     *
     * @param flag the flag
     * @return the value
     */
    public static Object getProperty(final String flag) {
        return getInstance().values.get(flag);
    }

    /**
     * Get the int keys.
     *
     * @return String[]
     */
    public static List<String> getIntKeys() {
        getInstance();
        return mIntValues;
    }

    /**
     * Get the int keys.
     *
     * @return String[]
     */
    public static List<String> getLongKeys() {
        getInstance();
        return mLongValues;
    }

    /**
     * Get the boolean keys.
     *
     * @return String[]
     */
    public static List<String> getBooleanKeys() {
        getInstance();
        return mBoolValues;
    }

    /**
     * Get the list keys.
     *
     * @return String[]
     */
    public static List<String> getListKeys() {
        getInstance();
        return mListValues;
    }

    /**
     * Is the proxy enabled?
     *
     * @return boolean
     */
    public static boolean isInitiallyEnabled() {
        return (Boolean) getInstance().values.get(Field.DB_ENABLE_PROXY_INITIALLY.getFieldName());
    }

    /**
     * Set am integer property.
     *
     * @param property String
     * @param value    the value
     */
    public static void setProperty(final String property, final Object value) {
        if (value instanceof Boolean) {
            if (!mBoolValues.contains(property)) {
                throw new IllegalArgumentException("the boolean property " + property + " does not exist.");
            }
            getInstance().values.put(property, value);
            return;
        }
        if (value instanceof Long) {
            if (!mLongValues.contains(property)) {
                throw new IllegalArgumentException("the long property " + property + " does not exist.");
            }
            getInstance().values.put(property, value);
            return;
        }
        if (value instanceof Integer) {
            if (!mIntValues.contains(property)) {
                throw new IllegalArgumentException("the int property " + property + " does not exist.");
            }
            getInstance().values.put(property, value);
            return;
        }
        if (value instanceof String) {
            if (!mStringValues.contains(property)) {
                throw new IllegalArgumentException("the string property " + property + " does not exist.");
            }
            getInstance().values.put(property, value);
            return;
        }
        if (value instanceof List) {
            if (!mListValues.contains(property)) {
                throw new IllegalArgumentException("the list property " + property + " does not exist.");
            }
            getInstance().values.put(property, value);
            return;
        }

        throw new IllegalArgumentException("the argument " + value + " is illegal");
    }

    public static List<ExecutionListener> getListener() {
        return getInstance().mListener;
    }

    public static List<ExecutionFailedListener> getFailedListener() {
        return getInstance().mFailedListener;
    }

    public static List<ConnectionListener> getConnectionListener() {
        return getInstance().mConnectionListener;
    }

    /**
     * Init the client properties.
     */
    private void init() {
        boolean init = false;

        final InputStream input;
        try {
            input = ClientProperties.class.getResourceAsStream("/dbproxy.xml");
            if (input != null) {
                mTrace.info("reading dbproxy.xml configuration from classpath.");
                final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
                parser.parse(input, mHandler);
                input.close();
                init = true;
            }
        }
        catch (final Exception ex) {
            throw new IllegalArgumentException("Parsing the dbinit file dbproxy.xml failed.", ex);
        }

        final File f = new File(System.getProperty("user.home") + "/dbproxy.xml");
        if (f.exists()) {
            mTrace.info("reading properties from {}", f);

            try {
                final SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
                parser.parse(f, mHandler);
                init = true;
            }
            catch (final FileNotFoundException ex) {
                mTrace.info("The dbproxy configuration file {} does not exist.", f);
            }
            catch (final Exception ex) {
                mTrace.atError().withThrowable(ex).log("parsing the file {} failed", f);
            }
        }

        if (!init) {
            mTrace.warn(
                    "neither a file {} nor a file dbproxy.xml in the classpath exists. You can download an example of the file here https://github.com/lbaeumer/jdbcspy/blob/master/src/test/resources/dbproxy.xml",
                    f
            );
        }

        readSystemProperties();
    }

    /**
     * @see java.lang.Object
     */
    @Override
    public String toString() {
        return values.toString();
    }

    private void readSystemProperties() {
        final Properties p = System.getProperties();
        for (final String key : mIntValues) {
            final Object obj = p.get(key);
            if (obj != null) {
                try {
                    values.put(key, Integer.valueOf((String) obj));
                }
                catch (final Exception e) {
                    mTrace.warn("property for {}={} is not convertable to type int", key, obj);
                }
            }
        }
        for (final String key : mLongValues) {
            final Object obj = p.get(key);
            if (obj != null) {
                try {
                    values.put(key, Long.valueOf((String) obj));
                }
                catch (final Exception e) {
                    mTrace.warn("property for {}={} is not convertable to type long", key, obj);
                }
            }
        }
        for (final String key : mBoolValues) {
            final Object obj = p.get(key);
            if (obj != null) {
                try {
                    values.put(key, Boolean.valueOf((String) obj));
                }
                catch (final Exception e) {
                    mTrace.warn("property for {}={} is not convertable to type boolean", key, obj);
                }
            }
        }
        for (final String key : mStringValues) {
            final Object obj = p.get(key);
            if (obj != null) {
                values.put(key, obj);
            }
        }
    }

    public enum Field {
        /**
         * enable the proxy
         */
        DB_ENABLE_PROXY_INITIALLY("EnableProxyInitially", Boolean.class),
        /**
         * throw warnings
         */
        DB_THROW_WARNINGS("ThrowWarnings", Boolean.class),

        DB_AMAZON_DRIVER_CLASS("Amazon_DriverClass", String.class),
        DB_AMAZONORACLE_DRIVER_CLASS("AmazonOracle_DriverClass", String.class),
        DB_DERBY_DRIVER_CLASS("Derby_DriverClass", String.class),
        DB_DB2_DRIVER_CLASS("DB2_DriverClass", String.class),
        DB_MARIA_DRIVER_CLASS("Maria_DriverClass", String.class),
        DB_MSSQL_DRIVER_CLASS("Mssql_DriverClass", String.class),
        DB_MYSQL_DRIVER_CLASS("Mysql_DriverClass", String.class),
        DB_ORACLE_DRIVER_CLASS("Oracle_DriverClass", String.class),
        DB_POSTGRESQL_DRIVER_CLASS("PostgreSql_DriverClass", String.class),
        DB_REDSHIFT_DRIVER_CLASS("Redshift_DriverClass", String.class),

        /**
         * the threshold for the next method
         */
        DB_RESULTSET_NEXT_TIME_THRESHOLD("ResultSetNextTimeThreshold", Long.class),
        /**
         * the threshold for the resultset iteration
         */
        DB_RESULTSET_TOTAL_TIME_THRESHOLD("ResultSetTotalTimeThreshold", Long.class),
        /**
         * the threshold for the resultset iteration
         */
        DB_RESULTSET_TOTAL_SIZE_THRESHOLD("ResultSetTotalSizeThreshold", Long.class),
        DB_STMT_EXECUTE_TIME_THRESHOLD("StmtExecuteTimeThreshold", Long.class),
        DB_STMT_TOTAL_TIME_THRESHOLD("StmtTotalTimeThreshold", Long.class),
        DB_STMT_TOTAL_SIZE_THRESHOLD("StmtTotalSizeThreshold", Long.class),
        DB_CONN_TOTAL_TIME_THRESHOLD("ConnTotalTimeThreshold", Long.class),
        DB_CONN_TOTAL_SIZE_THRESHOLD("ConnTotalSizeThreshold", Long.class),
        /**
         * maximum number of characters to be displayed of sql string
         */
        DB_DISPLAY_SQL_STRING_MAXLEN("DisplaySqlStringMaxlen", Integer.class),
        /**
         * remove hints
         */
        DB_REMOVE_HINTS("RemoveHints", Boolean.class),
        /**
         * ignore not closed objects
         */
        DB_IGNORE_NOT_CLOSED_OBJECTS("IgnoreNotClosedObjects", Boolean.class),
        /**
         * enable size evaluation
         */
        DB_ENABLE_SIZE_EVALUATION("EnableSizeEvaluation", Boolean.class),

        /**
         * debug beans
         */
        DB_STMT_DEBUG_CLASS_EXP("StmtDebugClassExp", List.class),
        /**
         * historize statement
         */
        DB_STMT_HISTORIZE_CLASS_EXP("StmtHistorizeClassExp", List.class),
        /**
         * debug beans
         */
        DB_STMT_DEBUG_SQL_EXP("StmtDebugSQLExp", List.class),
        /**
         * historize beans
         */
        DB_STMT_HISTORIZE_SQL_EXP("StmtHistorizeSQLExp", List.class),

        /**
         * the trace depth
         */
        DB_TRACE_DEPTH("TraceDepth", Integer.class),
        DB_TRACE_CLASS_IGNORE_REGEXP("TraceClassIgnoreRegExp", String.class),

        /**
         * dump after shutdown
         */
        DB_DUMP_AFTER_SHUTDOWN("DumpAfterShutdown", Boolean.class),
        /**
         * dump interval in s
         */
        DB_DUMP_INTERVAL("DumpInterval", Long.class),

        DB_MONITOR_RESULTSET_TIME_THRESHOLD("MonitorResultSetTimeThreshold", Long.class),
        DB_MONITOR_RESULTSET_FREQUENCY("MonitorResultSetFrequency", Long.class),
        DB_MONITOR_RESULTSET_LEAK_LOG_ALWAYS("MonitorResultSetLeakLogAlways", Boolean.class),

        /**
         * dump interval in s
         */
        VERBOSE("Verbose", Boolean.class);

        private final String fieldName;
        private final Class<?> fieldClass;

        Field(final String fieldName, final Class<?> fieldClass) {
            this.fieldName = fieldName;
            this.fieldClass = fieldClass;
        }

        public String getFieldName() {
            return fieldName;
        }

        public Class<?> getFieldClass() {
            return fieldClass;
        }

        public String getStringValue() {
            if (fieldClass != String.class) {
                throw new IllegalArgumentException("The field " + fieldName + " is not a String.");
            }

            return (String) getProperty(fieldName);
        }

        public int getIntValue() {
            if (fieldClass != Integer.class) {
                throw new IllegalArgumentException("The field " + fieldName + " is not an Integer.");
            }

            return (int) getProperty(fieldName);
        }

        public long getLongValue() {
            if (fieldClass != Long.class && fieldClass != Integer.class) {
                throw new IllegalArgumentException("The field " + fieldName + " is not an Long or Integer.");
            }

            return (long) getProperty(fieldName);
        }

        public boolean getBooleanValue() {
            if (fieldClass != Boolean.class) {
                throw new IllegalArgumentException("The field " + fieldName + " is not a Boolean.");
            }

            return (boolean) getProperty(fieldName);
        }

        public List<String> getListValues() {
            if (fieldClass != List.class) {
                throw new IllegalArgumentException("The field " + fieldName + " is not a List.");
            }

            return (List<String>) getProperty(fieldName);
        }

        public Object getValue() {
            return getProperty(fieldName);
        }
    }

}







