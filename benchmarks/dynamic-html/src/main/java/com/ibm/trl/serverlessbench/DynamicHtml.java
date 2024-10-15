package com.ibm.trl.serverlessbench;

import java.sql.Timestamp;

import com.ibm.trl.serverlessbench.wrapper.BenchmarkWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.hubspot.jinjava.Jinjava;


import io.quarkus.funqy.Funq;


/* reference: https://github.com/HubSpot/jinjava */

public class DynamicHtml {
    private static final double nanosecInSec = 1_000_000_000.0;

    private String template = "<!DOCTYPE html>" +
            "<html>" +
            "  <head>" +
            "    <title>Randomly generated data.</title>" +
            "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">" +
            "    <link href=\"http://netdna.bootstrapcdn.com/bootstrap/3.0.0/css/bootstrap.min.css\" rel=\"stylesheet\" media=\"screen\">" +
            "    <style type=\"text/css\">" +
            "      .container {" +
            "        max-width: 500px;" +
            "        padding-top: 100px;" +
            "      }" +
            "    </style>" +
            "  </head>" +
            "  <body>" +
            "    <div class=\"container\">" +
            "      <p>Welcome {{username}}!</p>" +
            "      <p>Data generated at: {{cur_time}}!</p>" +
            "      <p>Requested random numbers:</p>" +
            "      <ul>" +
            "        {% for n in random_numbers %}" +
            "        <li>{{n}}</li>" +
            "        {% endfor %}" +
            "      </ul>" +
            "    </div>" +
            "  </body>" +
            "</html>";

    static Map<String, Integer> size_generators = Map.of("test", 10,
                                                        "tiny", 100,
                                                        "small", 1000,
                                                        "medium", 10000,
                                                        "large", 100000,
                                                        "huge", 1000000,
                                                        "massive", 10000000);

    public static class FunInput {
        public String size;
        public boolean debug;
    }

    @Funq("dynamic-html")
    @BenchmarkWrapper
    public Map<String, Object> dynamicHtml(FunInput input) throws Exception {
        Map<String, Object> retVal = new LinkedHashMap<>();
        if (input == null || input.size == null) {
            retVal.put("message", "ERROR: DynamicHtml unable to run. size needs to be set.");
            return retVal;
        }
        int loadSize = inputSize(input.size);
        Random rand = new Random();

        long startTime = System.nanoTime();

        long initStart = System.nanoTime();
        Jinjava jinjava = new Jinjava();
        long initEnd = System.nanoTime();
        HashMap<String, Object> context = new HashMap<>();

        long setupStart = System.nanoTime();
        List<Integer> integers = new ArrayList<>(loadSize);
        for (int i = 0; i < loadSize; i++) {
            integers.add(rand.nextInt(1000000));
        }
        long setupEnd = System.nanoTime();

        context.put("username", "testname");
        context.put("random_numbers", integers);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        context.put("cur_time", timestamp.toString());

        long renderStart = System.nanoTime();
        String renderedTemplate = jinjava.render(template, context);
        long renderEnd = System.nanoTime();
        long stopTime = renderEnd;

        retVal.put("measurement", Map.of("init_time", (initEnd - initStart) / nanosecInSec,
                                        "setup_time", (setupEnd - setupStart) / nanosecInSec,
                                        "render_time", (renderEnd - renderStart) / nanosecInSec,
                                        "total_run_time", (stopTime - startTime) / nanosecInSec));
        retVal.put("output", Map.of("input_size", input.size,
                                    "converted_size", loadSize,
                                    "rendered_Length", renderedTemplate.length()));
        if (input.debug) {
            retVal.put("rendered_HTML", renderedTemplate);
        }

        return retVal;
    }

    private int inputSize(String size) {
        int retval = 1;

        if (size != null) {
            Integer s = size_generators.get(size);
            if (s != null) {
                retval = s;
            } else if (!size.isEmpty()) {
                retval = Integer.parseUnsignedInt(size);
            }
        }

        return retval;
    }
}
