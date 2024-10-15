package com.ibm.trl.serverlessbench;

import java.util.LinkedHashMap;
import java.util.Map;

import com.ibm.trl.serverlessbench.wrapper.BenchmarkWrapper;

import org.jboss.logging.Logger;

import io.quarkus.funqy.Funq;

public class HelloWorld {
    private static final double nanosecInSec = 1_000_000_000.0;

    static Map<String, Integer> size_generators = Map.of("test",  1,
                                                         "small", 100,
                                                         "large", 1000);

    private final Logger log = Logger.getLogger(HelloWorld.class);
    
    @Funq
    @BenchmarkWrapper
    public Map<String, Object> helloworld(Map<String, String> input) {
        Map<String, Object> retVal = new LinkedHashMap<>();

        long hello_count = 0;

        if (!input.isEmpty()) {
            Integer hc = size_generators.get(input.get("size"));
            if (hc != null) {
                hello_count = hc;
            }
        }
        long processTimeBegin = System.nanoTime();

        String hello = "world!";
        for (int i = 0; i<hello_count; i++) {
            hello = "Hello " + hello;
        }
        long processTimeEnd = System.nanoTime();

        retVal.put("measurement", Map.of("compute_time", (processTimeEnd - processTimeBegin) / nanosecInSec));
        retVal.put("hello_count", hello_count);

        log.debug("retVal.measurement="+retVal.get("measurement"));

        return retVal;
    }
}
