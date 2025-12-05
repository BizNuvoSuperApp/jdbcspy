package jdbcspy.proxy.exception;

import java.io.Serial;

/**
 * The ResourceNotClosedException class.
 */
public class ResourceNotClosedException extends ProxyException {

    /**
     * the serial version uid
     */
    @Serial private static final long serialVersionUID = -7961795386461897591L;

    /**
     * the method
     */
    private String method;

    /**
     * Constructor.
     *
     * @param txt the text
     */
    public ResourceNotClosedException(final String txt) {
        super(txt);
    }

    /**
     * Get the open method.
     *
     * @return String
     */
    @Override
    public String getOpenMethod() {
        return method;
    }

    /**
     * Set the open method.
     *
     * @param m the open method
     */
    @Override
    public void setOpenMethod(final String m) {
        method = m;
    }

}
