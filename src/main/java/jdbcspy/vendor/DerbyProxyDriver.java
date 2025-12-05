package jdbcspy.vendor;

import jdbcspy.AbstractProxyDriver;
import jdbcspy.ClientProperties;

public class DerbyProxyDriver extends AbstractProxyDriver {

    public DerbyProxyDriver() {
        super(ClientProperties.Field.DB_DERBY_DRIVER_CLASS);
    }

}
