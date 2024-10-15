package com.ibm.trl.serverlessbench;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.ibm.trl.serverlessbench.wrapper.BenchmarkWrapper;

import org.jboss.logging.Logger;

import io.quarkus.funqy.Funq;

public class Sleep {
    private static final double nanosecInSec = 1_000_000_000.0;

    static Map<String, Integer> size_generators = Map.of("test",  1,
                                                         "small", 100,
                                                         "large", 1000);

    private static final Logger log = Logger.getLogger(Sleep.class);
    
    @Funq
    @BenchmarkWrapper
    public Map<String, Object> sleep(Map<String, String> input) {
        Map<String, Object> retVal = new LinkedHashMap<>();
        long sleep_time = 0; // seconds

        if(!input.isEmpty()) {
            Integer st = size_generators.get(input.get("size"));
            if(st != null) {
                sleep_time = st;
            }
        }
        long processTimeBegin = System.nanoTime();

        try {
            TimeUnit.SECONDS.sleep(sleep_time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long processTimeEnd = System.nanoTime();

        retVal.put("measurement", Map.of("compute_time", (processTimeEnd - processTimeBegin) / nanosecInSec));
        retVal.put("sleep_time", sleep_time);
        
        log.debug("retVal.measurement="+retVal.get("measurement"));

        return retVal;
    }
}
