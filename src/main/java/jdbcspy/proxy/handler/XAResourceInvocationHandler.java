package jdbcspy.proxy.handler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import javax.transaction.xa.XAResource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jdbcspy.proxy.util.Utils;

/**
 * The connection handler.
 */
public class XAResourceInvocationHandler implements InvocationHandler {

    /**
     * the logger object for tracing
     */
    private static final Logger mTrace = LogManager.getLogger("jdbcspy.xaresource");

    /**
     * the underlying connection
     */
    private final XAResource uXa;
    private final XAConnectionInvocationHandler xaConnectionInvocationHandler;

    /**
     * The Constructor.
     *
     * @param xa the xa resource
     */
    public XAResourceInvocationHandler(final XAResource xa, final XAConnectionInvocationHandler xaConnectionInvocationHandler) {
        uXa = xa;
        this.xaConnectionInvocationHandler = xaConnectionInvocationHandler;
    }

    /**
     * @see InvocationHandler
     */
    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) {

        try {
            mTrace.trace("call {}.{}", uXa::getClass, () -> Utils.getMethodSignature(method, args));

            if ("end".equals(method.getName())) {
                xaConnectionInvocationHandler.endTx();
            }

            return method.invoke(uXa, args);
        }
        catch (final Exception e) {
            mTrace.atError().withThrowable(e).log("unknown error in {}.{} failed for {}", uXa.getClass(), method.getName(), this);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

}
