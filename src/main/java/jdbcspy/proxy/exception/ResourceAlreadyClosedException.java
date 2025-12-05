package jdbcspy.proxy.exception;

import java.io.Serial;

/**
 * The ResourceAlreadyClosedException class.
 */
public class ResourceAlreadyClosedException extends ProxyException {

    /**
     * the serial version uid
     */
    @Serial private static final long serialVersionUID = -8576761524925278661L;

    /**
     * the method
     */
    private String mMethod;

    /**
     * Constructor.
     *
     * @param txt the text
     */
    public ResourceAlreadyClosedException(final String txt) {
        super(txt);
    }

    /**
     * Get the open method.
     *
     * @return String
     */
    @Override
    public String getOpenMethod() {
        return mMethod;
    }

    /**
     * Set the open method.
     *
     * @param method the open method
     */
    @Override
    public void setOpenMethod(final String method) {
        mMethod = method;
    }

}
