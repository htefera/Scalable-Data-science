package de.tuberlin.dima.aim3.exercises.streaming;

import java.lang.reflect.*;

/**
 * A dynamic proxy class for {@link FootballStatistics} interface.
 * The main purpose for this class is to invoke {@link FootballStatistics#initStreamExecEnv()}
 * to initialize {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment}
 * and {@link org.apache.flink.streaming.api.datastream.DataStream} before each streaming operation.
 *
 * @author Imran, Muhammad
 */
public class FootballStatisticsProxy implements InvocationHandler {
    private FootballStatistics statistics;

    /**
     * constructor to create FootballStatisticsProxy instance
     *
     * @param statistics an instance of {@link FootballStatistics} concrete implementation.
     */
    private FootballStatisticsProxy(FootballStatistics statistics) {
        this.statistics = statistics;
    }

    /**
     * create an  instance of {@link FootballStatisticsProxy}
     *
     * @param statistics instance of an implementation of {@link FootballStatistics} type
     * @return {@link FootballStatistics}
     */
    public static FootballStatistics newInstance(FootballStatistics statistics) {
        return (FootballStatistics) Proxy.newProxyInstance(statistics.getClass().getClassLoader(),
                statistics.getClass().getInterfaces(), new FootballStatisticsProxy(statistics));
    }

    /**
     * Processes a method invocation on a proxy instance and returns
     * the result.  This method will be invoked on an invocation handler
     * when a method is invoked on a proxy instance that it is
     * associated with.
     *
     * @param proxy  the proxy instance that the method was invoked on
     * @param method the {@code Method} instance corresponding to
     *               the interface method invoked on the proxy instance.  The declaring
     *               class of the {@code Method} object will be the interface that
     *               the method was declared in, which may be a superinterface of the
     *               proxy interface that the proxy class inherits the method through.
     * @param args   an array of objects containing the values of the
     *               arguments passed in the method invocation on the proxy instance,
     *               or {@code null} if interface method takes no arguments.
     *               Arguments of primitive types are wrapped in instances of the
     *               appropriate primitive wrapper class, such as
     *               {@code java.lang.Integer} or {@code java.lang.Boolean}.
     * @return the value to return from the method invocation on the
     * proxy instance.  If the declared return type of the interface
     * method is a primitive type, then the value returned by
     * this method must be an instance of the corresponding primitive
     * wrapper class; otherwise, it must be a type assignable to the
     * declared return type.  If the value returned by this method is
     * {@code null} and the interface method's return type is
     * primitive, then a {@code NullPointerException} will be
     * thrown by the method invocation on the proxy instance.  If the
     * value returned by this method is otherwise not compatible with
     * the interface method's declared return type as described above,
     * a {@code ClassCastException} will be thrown by the method
     * invocation on the proxy instance.
     * @throws Throwable the exception to throw from the method
     *                   invocation on the proxy instance.  The exception's type must be
     *                   assignable either to any of the exception types declared in the
     *                   {@code throws} clause of the interface method or to the
     *                   unchecked exception types {@code java.lang.RuntimeException}
     *                   or {@code java.lang.Error}.  If a checked exception is
     *                   thrown by this method that is not assignable to any of the
     *                   exception types declared in the {@code throws} clause of
     *                   the interface method, then an
     *                   {@link UndeclaredThrowableException} containing the
     *                   exception that was thrown by this method will be thrown by the
     *                   method invocation on the proxy instance.
     * @see UndeclaredThrowableException
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            if (!method.getName().equals("initStreamExecEnv")) {
                // invoke FootballStatistics#initStreamExecEnv() method before each invocation of the method.
                statistics.initStreamExecEnv();
            }
            method.invoke(statistics, args);
        } catch (InvocationTargetException ex) {
            throw ex.getTargetException();
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected invocation exception: " + ex.getMessage());
        }
        return null;
    }
}
