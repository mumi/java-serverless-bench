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

import io.quarkus.funqy.Funq;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;
import org.jclouds.blobstore.BlobStore;

public class ClockSynchronization {
    private static final double nanosecInSec = 1_000_000_000.0;

    private static final Logger log = Logger.getLogger(ClockSynchronization.class);
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

    @Funq("clock-synchronization")
    @BenchmarkWrapper
    public Map<String, Object> clock_synchronization(FunInput input) {
        Map<String, Object> retVal = new LinkedHashMap<>();
        if (input == null || (input.bucket == null && bucket == null) || input.server_address == null || input.server_port == 0) {
            retVal.put("message", "ERROR: Clock Synchronization unable to run. server_address, server_port and bucket need to be set.");
            return retVal;
        }

        String key = "filename_tmp";
        String request_id = input.request_id == null ? "test" : input.request_id;
        String address = input.server_address;
        int port = input.server_port;
        int repetitions = input.repetitions == 0 ? 1 : input.repetitions;

        List<Long[]> times = new ArrayList<>();
        log.info("Starting communication with " + address + ":" + port);
        long i = 0;
        
        long processTimeBegin = System.nanoTime();

        try {
            DatagramSocket sendSocket = new DatagramSocket(null);
            sendSocket.setSoTimeout(4);
            sendSocket.setReuseAddress(true);
            sendSocket.bind(new InetSocketAddress("", 0));

            DatagramSocket recvSocket = new DatagramSocket(port);

            byte[] message = new byte[0];
            try {
                message = request_id.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }

            DatagramPacket packet = new DatagramPacket(message, message.length, new InetSocketAddress(address, port));
            DatagramPacket packet2 = new DatagramPacket(message, message.length);

            int consecutive_failures = 0;
            int measurements_not_smaller = 0;
            long cur_min = 0;

            long send_begin = 0;
            long recv_end = 0;
            while (i < 1000) {
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
                if (i > 0) {
                    times.add( new Long[] {i, send_begin, recv_end} );
                }
                long cur_time = recv_end - send_begin;
                if (cur_time > cur_min && cur_min > 0) {
                    measurements_not_smaller += 1;
                    if (measurements_not_smaller == repetitions) {
                        try {
                            message = "stop".getBytes("UTF-8");
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                        DatagramPacket packet_ = new DatagramPacket(message, message.length, new InetSocketAddress(address, port));
                        try {
                            sendSocket.send(packet_);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                } else {
                    cur_min = cur_time;
                    measurements_not_smaller = 0;
                }
                ++i;
                consecutive_failures = 0;
                sendSocket.setSoTimeout(4000);
            }
            sendSocket.close();
            recvSocket.close();

            if (consecutive_failures != 5 && input.debug) {
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

                key = String.format("clock-synchronization-benchmark-results-%s.csv", request_id);

                try {
                    blobStore.putBlob(input.bucket, blobStore.blobBuilder(key).payload(new File("/tmp/data.csv")).build());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
        long processTimeEnd = System.nanoTime();

        retVal.put("measurement", (processTimeEnd - processTimeBegin) / nanosecInSec);
        retVal.put("output", key);

        log.debug("retVal.measurement="+retVal.get("measurement"));

        return retVal;
    }
}
