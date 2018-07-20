package ru.yandex.clickhouse.util;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;

public class LogAndInstrumentationProxy<T> implements InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(LogAndInstrumentationProxy.class);

    private final T object;
    private final Class<T> clazz;
    private final boolean instrument;

    public static <T> T wrap(Class<T> interfaceClass, T object) {
        return wrap(interfaceClass, object, false);
    }

    public static <T> T wrap(Class<T> interfaceClass, T object, boolean instrument) {
        if (log.isTraceEnabled() || instrument) {
            LogAndInstrumentationProxy<T> proxy = new LogAndInstrumentationProxy<T>(interfaceClass, object, instrument);
            return proxy.getProxy();
        }
        return object;
    }

    private LogAndInstrumentationProxy(Class<T> interfaceClass, T object, boolean instrument) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalStateException("Class " + interfaceClass.getName() + " is not an interface");
        }
        clazz = interfaceClass;
        this.object = object;
        this.instrument = instrument;
    }

    @SuppressWarnings("unchecked")
    public T getProxy() {
        //xnoinspection x
        // unchecked
        return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class<?>[]{clazz}, this);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String result = "";
        long start = System.currentTimeMillis();
        try {
            final Object invokeResult = method.invoke(object, args);
            if (log.isTraceEnabled()) {
                result += invokeResult;
            }
            return invokeResult;
        } catch (InvocationTargetException e) {
            if (log.isTraceEnabled()) {
                result += e.getMessage();
            }
            if (instrument) {
                Meter meter = getOrCreateInvocationExceptionMeter(object, method);
                meter.mark();
            }
            throw e.getTargetException();
        } finally {
            long stop = System.currentTimeMillis();
            long durationMs = stop - start;
            if (log.isTraceEnabled()) {
                log.trace("==== ClickHouse JDBC trace begin ====\n" +
                                  "Call class: " + object.getClass().getName() +
                                  "\nMethod: " + method.getName() +
                                  "\nObject: " + object +
                                  "\nArgs: " + Arrays.toString(args) +
                                  "\nDuration: " + durationMs + "ms" +
                                  "\nInvoke result: " + result +
                                  "\n==== ClickHouse JDBC trace end ====");
            }
            if (instrument) {
                Histogram histogram = getOrCreateDurationHistogram(object, method);
                histogram.update(durationMs);
            }
        }
    }

    private Meter getOrCreateInvocationExceptionMeter(T object, Method method) {
        String suffix = "exceptions";
        Map<String, Meter> meters = SharedMetricRegistries.getDefault().getMeters(getFilterForProxy(object, method, suffix));
        return meters.isEmpty() ?
                       SharedMetricRegistries.getDefault().meter(getMetricNameForProxy(object, method, suffix)) :
                       meters.get(((SortedMap<String, Meter>) meters).firstKey());
    }

    private Histogram getOrCreateDurationHistogram(T object, Method method) {
        String suffix = "duration";
        Map<String, Histogram> timers = SharedMetricRegistries.getDefault().getHistograms(getFilterForProxy(object, method, suffix));
        return timers.isEmpty() ?
                       SharedMetricRegistries.getDefault().histogram(getMetricNameForProxy(object, method, suffix)) :
                       timers.get(((SortedMap<String, Histogram>) timers).firstKey());
    }

    private MetricFilter getFilterForProxy(T object, Method method, String suffix) {
        final String metricName = getMetricNameForProxy(object, method, suffix);
        return new MetricFilter() {
            @Override
            public boolean matches(String name, Metric metric) {
                return name.equals(metricName);
            }
        };
    }

    private String getMetricNameForProxy(T object, Method method, String suffix) {
        return MetricRegistry.name(object.getClass(), method.getName(), "invocation", suffix);
    }
}
