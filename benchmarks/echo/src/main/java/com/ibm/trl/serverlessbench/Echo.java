package com.ibm.trl.serverlessbench;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.trl.serverlessbench.wrapper.BenchmarkWrapper;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.jboss.logging.Logger;

import io.quarkus.funqy.Funq;

@RegisterForReflection(targets = { MemoryUsage.class, MemoryMXBean.class, ThreadMXBean.class })
public class Echo {
    private static final Logger log = Logger.getLogger(Echo.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Funq
    @BenchmarkWrapper
    public Map<String, Object> echo(Map<String, Object> input) {
        Map<String, Object> retVal = new LinkedHashMap<>();

        retVal.put("input", input);

        long uptime = ManagementFactory.getRuntimeMXBean().getUptime();
        retVal.put("uptime", uptime);

        retVal.put("env", System.getenv());

        Properties properties = System.getProperties();
        Map<String, String> systemProperties = new LinkedHashMap<>();
        for (String key : properties.stringPropertyNames()) {
            systemProperties.put(key, properties.getProperty(key));
        }
        retVal.put("systemProperties", systemProperties);

        List<Map<String, String>> cpuInfoList = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/cpuinfo"))) {
            Map<String, String> cpuInfo = new LinkedHashMap<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    if (!cpuInfo.isEmpty()) {
                        cpuInfoList.add(cpuInfo);
                        cpuInfo = new LinkedHashMap<>();
                    }
                } else {
                    String[] parts = line.split(":", 2);
                    if (parts.length == 2) {
                        cpuInfo.put(parts[0].trim(), parts[1].trim());
                    }
                }
            }
            if (!cpuInfo.isEmpty()) {
                cpuInfoList.add(cpuInfo);
            }
        } catch (IOException e) {
            log.error("Error reading /proc/cpuinfo", e);
            throw new RuntimeException("Error reading /proc/cpuinfo", e);
        }

        retVal.put("cpuinfo", cpuInfoList);

        return retVal;
    }

}
