package com.ibm.trl.serverlessbench.wrapper;

import jakarta.annotation.Priority;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Interceptor
@Priority(0)
@BenchmarkWrapper
public class FunctionInterceptor {

    private static final String coldStartFile = "/tmp/cold_run";
    private static String containerId;

    @AroundInvoke
    public Object logFunctionCall(InvocationContext context) throws Exception {
        Instant begin = Instant.now();
        Object result = context.proceed();
        Instant end = Instant.now();

        File file = new File(coldStartFile);
        boolean isCold = !file.exists();

        if (isCold) {
            try (FileWriter fw = new FileWriter(file)) {
                containerId = UUID.randomUUID().toString().substring(0, 8);
                fw.write(containerId);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            containerId = new String(Files.readAllBytes(Paths.get(coldStartFile)));
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("begin", begin.getEpochSecond() * 1_000_000L + begin.getNano() / 1_000L);
        response.put("end", end.getEpochSecond() * 1_000_000L + end.getNano() / 1_000L);
        response.put("results_time", java.time.Duration.between(begin, end).toNanos() / 1_000_000_000.0);
        response.put("is_cold", isCold);
        response.put("cold_start_var", System.getenv("cold_start_var"));
        response.put("container_uptime", ManagementFactory.getRuntimeMXBean().getUptime() / 1_000.0);
        response.put("container_id", containerId);
        response.put("result", result);

        return response;
    }
}

