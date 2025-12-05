package jdbcspy.vendor;

import jdbcspy.AbstractProxyDriver;
import jdbcspy.ClientProperties;

public class AmazonProxyDriver extends AbstractProxyDriver {

    public AmazonProxyDriver() {
        super(ClientProperties.Field.DB_AMAZON_DRIVER_CLASS);
    }

}
