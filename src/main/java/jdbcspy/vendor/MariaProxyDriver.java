package jdbcspy.vendor;

import jdbcspy.AbstractProxyDriver;
import jdbcspy.ClientProperties;

public class MariaProxyDriver extends AbstractProxyDriver {

    public MariaProxyDriver() {
        super(ClientProperties.Field.DB_MARIA_DRIVER_CLASS);
    }

}
