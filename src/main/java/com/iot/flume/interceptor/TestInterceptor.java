package com.iot.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by hjl on 2016/9/2.
 */
public class TestInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory
            .getLogger(TestInterceptor.class);

    private TestInterceptor() {

    }

    public void initialize() {
        // no-op
    }

    /**
     * Modifies events in-place.
     */
    public Event intercept(Event event) {
        logger.info("\nbody = " + new String(event.getBody()));
        logger.info("headers = " + event.getHeaders() + "\n");
        return event;
    }

    /**
     * Delegates to {@link #intercept(Event)} in a loop.
     *
     * @param events
     * @return
     */
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    public void close() {
        // no-op
    }

    /**
     * Builder which builds new instance of the StaticInterceptor.
     */
    public static class Builder implements Interceptor.Builder {


        public void configure(Context context) {
        }

        public Interceptor build() {
            return new TestInterceptor();
        }


    }

}
