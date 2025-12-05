package jdbcspy.vendor;

import jdbcspy.AbstractProxyDriver;
import jdbcspy.ClientProperties;

public class RedshiftProxyDriver extends AbstractProxyDriver {

    public RedshiftProxyDriver() {
        super(ClientProperties.Field.DB_REDSHIFT_DRIVER_CLASS);
    }

}
