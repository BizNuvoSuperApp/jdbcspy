package jdbcspy.vendor;

import jdbcspy.AbstractProxyDriver;
import jdbcspy.ClientProperties;

public class AmazonOracleProxyDriver extends AbstractProxyDriver {

    public AmazonOracleProxyDriver() {
        super(ClientProperties.Field.DB_AMAZONORACLE_DRIVER_CLASS);
    }

}
