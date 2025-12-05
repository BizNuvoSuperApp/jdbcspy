package jdbcspy.vendor;

import jdbcspy.AbstractProxyDriver;
import jdbcspy.ClientProperties;

public class PostgreSqlProxyDriver extends AbstractProxyDriver {

    public PostgreSqlProxyDriver() {
        super(ClientProperties.Field.DB_POSTGRESQL_DRIVER_CLASS);
    }

}
