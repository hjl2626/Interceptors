package com.iot.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
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

    private static HashMap<String,Integer> index = new HashMap<String, Integer>();
    private String partitionNum;


    private SimpleInterceptor(String partitionNum) {
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
        Integer par;
        String key = headers.get("type");
        if(partitionNum != null){
            int partitionNumInt = Integer.valueOf(partitionNum);
            if(index.containsKey(key)){
                if(index.get(key) >= partitionNumInt){
                    index.put(key,0);
                }
                par = index.get(key) % partitionNumInt;
                index.put(key,par+1);

            }else{
                par = 0;
                index.put(key,1);
            }
            headers.put("partition", par.toString());
        }
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
     * Builder which builds new instance of the StaticInterceptor.
     */
    public static class Builder implements Interceptor.Builder {


        private String partitionNum;

        public void configure(Context context) {
            partitionNum = context.getString(Constants.PARTITIONNUM, Constants.PARTITIONNUM_DEFAULT);

        }

        public Interceptor build() {
            logger.info(String.format(
                    "Creating SimpleInterceptor: partitionNum=%s",
                   partitionNum));
            return new SimpleInterceptor(partitionNum);
        }


    }

    public static class Constants {

        public static final String PARTITIONNUM = "partitionNum";
        public static final String PARTITIONNUM_DEFAULT = "0";

    }
}
