package jdbcspy.vendor;

import jdbcspy.AbstractProxyDriver;
import jdbcspy.ClientProperties;

public class MysqlProxyDriver extends AbstractProxyDriver {

    public MysqlProxyDriver() {
        super(ClientProperties.Field.DB_MYSQL_DRIVER_CLASS);
    }

}
