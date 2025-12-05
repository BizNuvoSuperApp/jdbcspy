package jdbcspy.vendor;

import jdbcspy.AbstractProxyDriver;
import jdbcspy.ClientProperties;

public class OracleProxyDriver extends AbstractProxyDriver {

    public OracleProxyDriver() {
        super(ClientProperties.Field.DB_ORACLE_DRIVER_CLASS);
    }

}
