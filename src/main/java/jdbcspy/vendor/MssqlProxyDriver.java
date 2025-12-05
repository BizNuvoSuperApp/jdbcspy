package jdbcspy.vendor;

import jdbcspy.AbstractProxyDriver;
import jdbcspy.ClientProperties;

public class MssqlProxyDriver extends AbstractProxyDriver {

    public MssqlProxyDriver() {
        super(ClientProperties.Field.DB_MSSQL_DRIVER_CLASS);
    }

}
