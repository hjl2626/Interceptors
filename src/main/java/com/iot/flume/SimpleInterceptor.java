package com.iot.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;



/**

 * <code>
 *   agent.sources.r1.channels = c1<p>
 *   agent.sources.r1.type = SEQ<p>
 *   agent.sources.r1.interceptors = i1<p>
 *   agent.sources.r1.interceptors.i1.type = host<p>
 *   agent.sources.r1.interceptors.i1.preserveExisting = true<p>
 *   agent.sources.r1.interceptors.i1.useIP = false<p>
 *   agent.sources.r1.interceptors.i1.hostHeader = hostname<p>
 * </code>
 *
 */
public class SimpleInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory
            .getLogger(SimpleInterceptor.class);

    private static int index = 0;

    private int partitionNum ;

    /**
     * Only {@link SimpleInterceptor.Builder} can build me
     */
    private SimpleInterceptor(int partitionNum) {
        this.partitionNum = partitionNum;
    }


    public void initialize() {
        // no-op
    }

    /**
     * Modifies events in-place.
     */

    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        if(index == partitionNum){
            index = 0;
        }
        headers.put("key", Integer.toString((index++) % partitionNum));
        return event;
    }

    /**
     * Delegates to {@link #intercept(Event)} in a loop.
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
     * Builder which builds new instances of the HostInterceptor.
     */
    public static class Builder implements Interceptor.Builder {

        private int partitionNum;

        public Interceptor build() {
            return new SimpleInterceptor(partitionNum);
        }


        public void configure(Context context) {
            partitionNum = context.getInteger(Constants.PARTITIONNUM, 1);
        }

    }

    public static class Constants {
        public static String PARTITIONNUM = "partitionNum";
    }

}
