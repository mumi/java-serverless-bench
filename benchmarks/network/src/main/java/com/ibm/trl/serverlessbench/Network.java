package com.ibm.trl.serverlessbench;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.SocketTimeoutException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.ibm.trl.serverlessbench.wrapper.BenchmarkStorageUtil;
import com.ibm.trl.serverlessbench.wrapper.BenchmarkWrapper;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;

import io.quarkus.funqy.Funq;
import org.jclouds.blobstore.BlobStore;


public class Network {
    private static final double nanosecInSec = 1_000_000_000.0;

    private static final Logger log = Logger.getLogger(Network.class);
    private static BlobStore blobStore;
    private static String bucket;


    void onStart(@Observes StartupEvent ev) {
        blobStore = BenchmarkStorageUtil.setupStorage();
        bucket = System.getenv("STORAGE_BUCKET");
    }

    public static class FunInput {
        public String request_id;
        public String server_address;
        public int server_port;
        public int repetitions;
        public String bucket;
        public boolean debug;
    }

    @Funq
    @BenchmarkWrapper
    public Map<String, Object> network(FunInput input) {
        Map<String, Object> retVal = new LinkedHashMap<>();
        if (input == null || (input.bucket == null && bucket == null) || input.request_id == null || input.server_address == null || input.server_port == 0 || input.repetitions == 0) {
            retVal.put("message", "ERROR: Network unable to run. request_id, server_address, server_port, repetitions and bucket need to be set.");
            return retVal;
        }
        if (input.bucket == null)
            input.bucket = bucket;

        String key = "filename_tmp";

        String request_id = input.request_id;
        int port = input.server_port;

        long processTimeBegin = System.nanoTime();
        List<Long[]> times = new ArrayList<>();
        int i = 0;

        try {
            DatagramSocket sendSocket = new DatagramSocket(null);
            sendSocket.setSoTimeout(3000);
            sendSocket.setReuseAddress(true);
            sendSocket.bind(new InetSocketAddress("", 0));

            DatagramSocket recvSocket = new DatagramSocket(port);

            byte[] message = new byte[0];
            try {
                message = request_id.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }

            DatagramPacket packet = new DatagramPacket(message, message.length, new InetSocketAddress(input.server_address, input.server_port));
            DatagramPacket packet2 = new DatagramPacket(message, message.length);

            int consecutive_failures = 0;
            long send_begin = 0;
            long recv_end = 0;

            while (i < input.repetitions + 1) {
                try {
                    send_begin = System.nanoTime();
                    sendSocket.send(packet);
                    recvSocket.receive(packet2);
                    recv_end = System.nanoTime();
                } catch (SocketTimeoutException e) {
                    ++i;
                    ++consecutive_failures;
                    if (consecutive_failures == 5) {
                        log.error("Can't setup the connection");
                        break;
                    }
                    continue;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (i > 0)
                    times.add(new Long[]{(long) i, send_begin, recv_end});

                ++i;
                consecutive_failures = 0;
                sendSocket.setSoTimeout(2000);
            }
            sendSocket.close();
            recvSocket.close();

            if (consecutive_failures != 5 && input.debug) {
                File upload_file = new File("/tmp/data.csv");
                try {
                    FileWriter writer = new FileWriter("/tmp/data.csv");
                    String header = String.join(",", "id", "client_send", "client_rcv");
                    writer.append(header);
                    for (Long[] row : times) {
                        String[] strRow = Stream.of(row).map(Object::toString).toArray(String[]::new);
                        writer.append(String.join(",", strRow));
                    }
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                key = String.format("network-benchmark-results-%s.csv", request_id);

                try {
                    blobStore.putBlob(input.bucket, blobStore.blobBuilder(key).payload(upload_file).build());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
        long processTimeEnd = System.nanoTime();

        retVal.put("measurement", Map.of("process_time", (processTimeEnd - processTimeBegin) / nanosecInSec));
        retVal.put("output", key);
        return retVal;
    }
}
