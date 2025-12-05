package jdbcspy.proxy.exception;

import java.io.Serial;
import java.sql.SQLException;

/**
 * The Proxy Exception class.
 */
public class ProxyException extends SQLException {

    /**
     * the serial version uid
     */
    @Serial private static final long serialVersionUID = -3690118765196760445L;

    /**
     * the method
     */
    private String method;

    /**
     * Constructor.
     *
     * @param txt the text
     */
    public ProxyException(final String txt) {
        super(txt);
    }

    /**
     * Get the open method.
     *
     * @return String
     */
    public String getOpenMethod() {
        return method;
    }

    /**
     * Set the open method.
     *
     * @param m the open method
     */
    public void setOpenMethod(final String m) {
        method = m;
    }

}
