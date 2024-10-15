package com.ibm.trl.serverlessbench;

import java.util.LinkedHashMap;
import java.util.Map;

import java.lang.String;

import java.io.IOException;

import java.net.Socket;
import java.net.SocketException;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.util.concurrent.TimeUnit;

import com.ibm.trl.serverlessbench.wrapper.BenchmarkWrapper;

import org.jboss.logging.Logger;

import io.quarkus.funqy.Funq;


public class ServerReply {
    private static final double nanosecInSec = 1_000_000_000.0;

    private static final Logger log = Logger.getLogger(ServerReply.class);

    public static class FunInput {
        public String server_address;
        public int server_port;
    }

    @Funq("server-reply")
    @BenchmarkWrapper
    public Map<String, Object> server_reply(FunInput input) {
        Map<String, Object> retVal = new LinkedHashMap<>();
        if (input == null || input.server_address == null || input.server_port == 0) {
            retVal.put("message", "ERROR: ServerReply unable to run. server_address and server_port need to be set.");
            return retVal;
        }

        String address = input.server_address;
        int port = input.server_port;

        // start echo server: THIS DEPENDS ON NCAT BEING INSTALLED!!!
        Process proc = null;
        try {
            String[] cmd = {"/usr/bin/sh", "-c", "/usr/bin/yes chargenchargenchargen | /usr/bin/ncat -l " + port + " --keep-open --send-only"};
            proc = Runtime.getRuntime().exec(cmd);
            TimeUnit.SECONDS.sleep(5);
        } catch (Exception e) {
            log.error("Server didn't launch: ", e);
        }

        long processTimeBegin = System.nanoTime();

        int readSize = 0;
        String line = "";
        try {
            Socket socket = new Socket(address, port);
            socket.setSoTimeout(100);
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            readSize = in.read(new byte[1024]);
            in.close();
            socket.close();
        } catch (SocketException e) {
            line = "SocketException: " + e;
        } catch (IOException e) {
            line = "IOException: " + e;
        }

        long processTimeEnd = System.nanoTime();

        proc.destroy();

        retVal.put("measurement", Map.of("process_time", (processTimeEnd - processTimeBegin) / nanosecInSec));
        retVal.put("output", Map.of("result", line,
                                    "size", readSize + ""));

        log.info("retVal.measurement=" + retVal.get("measurement"));

        return retVal;
    }
}
