package com.ibm.trl.serverlessbench;

import com.ibm.trl.serverlessbench.wrapper.BenchmarkStorageUtil;
import com.ibm.trl.serverlessbench.wrapper.BenchmarkWrapper;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.Map;

import io.quarkus.funqy.Funq;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;

public class Uploader {
    private static final double nanosecInSec = 1_000_000_000.0;

    private static final Logger log = Logger.getLogger(Uploader.class);

    private static BlobStore blobStore;
    private static String bucket;

    void onStart(@Observes StartupEvent ev) {
        blobStore = BenchmarkStorageUtil.setupStorage();
        bucket = System.getenv("STORAGE_BUCKET");
    }
    
    public static class FunInput {
        public String file;
        public String bucket;
        public boolean debug;
    }

    @Funq
    @BenchmarkWrapper
    public Map<String, Object> uploader(FunInput input) throws Exception {
        Map<String, Object> retVal = new LinkedHashMap<>();
        if (input == null || (input.bucket == null && bucket == null) || input.file == null) {
            retVal.put("message", "ERROR: Uploader unable to run. file and bucket need to be set.");
            return retVal;
        }
        if (input.bucket == null)
            input.bucket = bucket;

        File filePath = new File(String.format("/tmp/uploader-%s-%s", UUID.randomUUID(), input.file));
        long downloadStartTime = System.nanoTime();
        downloadFile(input.bucket, "input/" + input.file, filePath.toString());
        long downloadStopTime = System.nanoTime();
        long downloadSize = filePath.length();

        long uploadStartTime = System.nanoTime();
        uploadFile(input.bucket, "output/" + input.file, filePath.toString());
        long uploadStopTime = System.nanoTime();

        retVal.put("measurement", Map.of("download_time", (downloadStopTime - downloadStartTime) / nanosecInSec,
                                         "upload_time", (uploadStopTime - uploadStartTime) / nanosecInSec,
                                         "download_size", Long.toString(downloadSize)));
        if (input.debug) {
            retVal.put("output", Map.of( "bucket", input.bucket,
                                        "key", "output/" + input.file));
        } else {
            deleteFile(input.bucket, "output/" + input.file);
            filePath.delete();
        }

        return retVal;
    }

    private void downloadFile(String bucket, String key, String filePath) throws Exception {
        log.debug("Downloading " + filePath + " as " + key + " from bucket " + bucket + ".");
        File theFile = new File(filePath);
        File theDir = theFile.getParentFile();
        if (!theDir.exists())
            theDir.mkdirs();

        Blob blob = blobStore.getBlob(bucket, key);
        if (blob == null)
            throw new Exception("ERROR: Bucket or File not found.");

        try (InputStream is = blob.getPayload().openStream();
             OutputStream os = new FileOutputStream(theFile)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
        }
    }

    private void uploadFile(String bucket, String key, String filePath) {
        log.debug("Uploading " + filePath + " as " + key + " to bucket " + bucket + ".");
        blobStore.putBlob(bucket, blobStore.blobBuilder(key).payload(new File(filePath)).build());
    }

    private void deleteFile(String bucket, String key) {
        log.debug("Deleting " + key + " from bucket " + bucket + ".");
        blobStore.removeBlob(bucket, key);
    }
}
