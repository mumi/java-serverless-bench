package com.ibm.trl.serverlessbench;

import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.imageio.ImageIO;

import com.ibm.trl.serverlessbench.wrapper.BenchmarkStorageUtil;
import com.ibm.trl.serverlessbench.wrapper.BenchmarkWrapper;

import io.quarkus.funqy.Funq;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.io.Payloads;
import org.jclouds.io.payloads.InputStreamPayload;

public class Thumbnailer {
    private static final double nanosecInSec = 1_000_000_000.0;

    private static BlobStore blobStore;
    private static String bucket;

    void onStart(@Observes StartupEvent ev) {
        blobStore = BenchmarkStorageUtil.setupStorage();
        bucket = System.getenv("STORAGE_BUCKET");
    }

    public static class FunInput {
        public int height;
        public int width;
        public String file;
        public String bucket;
        public boolean debug;
    }
    
    @Funq
    @BenchmarkWrapper
    public Map<String, Object> thumbnailer(FunInput input) throws Exception {
        Map<String, Object> retVal = new LinkedHashMap<>();
        if (input == null || (input.bucket == null && bucket == null) || input.height == 0 || input.width == 0 || input.file == null) {
            retVal.put("message", "ERROR: Thumbnailer unable to run. file, height, width and bucket need to be set.");
            return retVal;
        }
        if (input.bucket == null)
            input.bucket = bucket;

        String key = "input/" + input.file.replaceAll(" ", "+");

        long download_begin = System.nanoTime();
        InputStream img = download_stream(input.bucket, key);
        long download_end = System.nanoTime();

        BufferedImage bimg = ImageIO.read(img);
        long image_size = len(bimg);
        long process_begin = System.nanoTime();
        BufferedImage resized = resize_image(bimg, input.width, input.height);
        long resized_size = len(resized);
        long process_end = System.nanoTime();

        long upload_begin = System.nanoTime();
        long upload_end   = upload_begin;
        File f = new File(key);
        String out_key = "resized-" + f.getName();
        String key_name = "";
        if(input.debug) {
            key_name = upload_stream(input.bucket, "output/" + out_key, resized, resized_size);
            upload_end = System.nanoTime();
        }

        double download_time = (download_end - download_begin) / nanosecInSec;
        double upload_time   = (upload_end - upload_begin) / nanosecInSec;
        double process_time   = (process_end - process_begin) / nanosecInSec;

        retVal.put("measurement", Map.of("download_time", download_time,
                                    "download_size", image_size,
                                    "upload_time", upload_time,
                                    "upload_size", resized_size,
                                    "compute_time", process_time));
        retVal.put("output", Map.of("bucket", input.bucket,
                                    "key", key_name));
        return retVal;
    }

    public BufferedImage resize_image(BufferedImage bimg, int w, int h) {
        Image thumbnail = bimg.getScaledInstance(w, h, Image.SCALE_DEFAULT);
        BufferedImage ret_image = new BufferedImage(thumbnail.getWidth(null), thumbnail.getHeight(null), BufferedImage.TYPE_INT_RGB);
        ret_image.createGraphics().drawImage(thumbnail, null, null);
        return ret_image;
    }

    private long len(BufferedImage img) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ImageIO.write(img, "jpg", out);
        out.flush();
        byte[] image_bytes = out.toByteArray();
        out.close();

        return image_bytes.length;
    }

    private InputStream download_stream(String bucket, String file) throws IOException {
        return blobStore.getBlob(bucket, file).getPayload().openStream();
    }
    
    private String upload_stream(String bucket, String file, BufferedImage bytes_data, long contentLength) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(bytes_data, "jpg", baos);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        InputStreamPayload payload = Payloads.newInputStreamPayload(bais);
        payload.getContentMetadata().setContentLength(contentLength);
        String key = String.join("/", file);
        blobStore.putBlob(bucket, blobStore.blobBuilder(key).payload(payload).build());

        return key;
    }
}
