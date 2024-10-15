package com.ibm.trl.serverlessbench;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.attribute.PosixFilePermission;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import com.ibm.trl.serverlessbench.wrapper.BenchmarkStorageUtil;
import com.ibm.trl.serverlessbench.wrapper.BenchmarkWrapper;

import io.quarkus.funqy.Funq;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import net.bramp.ffmpeg.FFmpeg;
import net.bramp.ffmpeg.FFmpegExecutor;
import net.bramp.ffmpeg.builder.FFmpegBuilder;
import net.bramp.ffmpeg.builder.FFmpegOutputBuilder;

import org.jboss.logging.Logger;
import org.jclouds.blobstore.BlobStore;


public class VideoProcessing {
    private static final double nanosecInSec = 1_000_000_000.0;

    private static final Logger log = Logger.getLogger(VideoProcessing.class);
    private static final Path watermarkPath = Path.of("/tmp/watermark.png");
    private static final String ffmpegPath = "./ffmpeg";
    private static FFmpeg ffmpeg;
    private static BlobStore blobStore;
    private static String bucket;


    void onStart(@Observes StartupEvent ev) {
        blobStore = BenchmarkStorageUtil.setupStorage();
        bucket = System.getenv("STORAGE_BUCKET");

        // ffmpeg = new FFmpeg(); // initialize FFmpeg from PATH or FFMPEG env var
        try {
            Path path = Path.of(ffmpegPath);
            Set<PosixFilePermission> perms = Files.getPosixFilePermissions(path);
            perms.add(PosixFilePermission.OWNER_EXECUTE);
            Files.setPosixFilePermissions(path, perms);

            ffmpeg = new FFmpeg(ffmpegPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize FFmpeg", e);
        }
    }

    public static class FunInput {
        public String file;
        public int duration;
        public String operation;
        public String bucket;
        public boolean debug;
    }

    @Funq("video-processing")
    @BenchmarkWrapper
    public Map<String, Object> video_processing(FunInput input) throws Exception {
        Map<String, Object> retVal = new LinkedHashMap<>();
        if (input == null || (input.bucket == null && bucket == null) || input.file == null || input.operation == null) {
            retVal.put("message", "ERROR: VideoProcessing unable to run. file, operation and bucket need to be set.");
            return retVal;
        }
        if (input.bucket == null)
            input.bucket = bucket;

        String key = input.file;
        String download_path = String.format("/tmp/%s", key);

        long download_begin = System.nanoTime();
        download(input.bucket, "input/" + key, download_path);
        long download_stop = System.nanoTime();
        double download_size = Files.size(new File(download_path).toPath());

        long process_begin = System.nanoTime();
        String upload_path = operations.get(input.operation).apply(download_path, input.duration);
        long process_end = System.nanoTime();

        String out_key = "";
        double output_size = 0d;
        long upload_begin = 0L;
        long upload_stop = 0L;

        if (upload_path != null) {
            File output_file = new File(upload_path);
            output_size = Files.size(output_file.toPath());

            if (input.debug) {
                File f = new File(key);
                out_key = "output/" + ((f.getParent() != null) ? f.getParent() + "/" : "") + output_file.getName();
                upload_begin = System.nanoTime();
                upload(input.bucket, out_key, upload_path);
                upload_stop = System.nanoTime();
            }
        }

        retVal.put("measurement", Map.of("download_time", (download_stop - download_begin) / nanosecInSec,
                                         "download_size", download_size,
                                         "upload_time", (upload_stop - upload_begin) / nanosecInSec,
                                         "output_size", output_size,
                                         "compute_time", (process_end - process_begin) / nanosecInSec));
        retVal.put("output", Map.of("bucket", input.bucket,
                                    "key", out_key));
        return retVal;
    }

    BiFunction<String, Integer, String> to_gif = (video, duration) -> {
        String output = String.format("/tmp/processed-%s.gif", video.substring(video.lastIndexOf('/') + 1, video.lastIndexOf('.')));
        FFmpegOutputBuilder outBuilder = new FFmpegBuilder()
                .setInput(video)
                .overrideOutputFiles(true)
                .addOutput(output);
        if (duration > 0) {
            outBuilder.setDuration(duration.longValue(), TimeUnit.SECONDS);
        }
        FFmpegBuilder builder = outBuilder
                .setFormat("gif")
                .setVideoFrameRate(10, 1)
                .setVideoResolution(320, 240)
                .done();

        FFmpegExecutor executor = null;
        try {
            executor = new FFmpegExecutor(ffmpeg);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        executor.createJob(builder).run();

        return output;
    };

    private String getWatermark() {
        if (Files.exists(watermarkPath)) {
            return watermarkPath.toString();
        }

        Path tmpPath = null;
        try (InputStream res = getClass().getResourceAsStream("/watermark.png")) {
            tmpPath = Files.createTempFile(Path.of("/tmp"), "watermark", ".png");
            Files.copy(res, tmpPath, StandardCopyOption.REPLACE_EXISTING);
            if (!Files.exists(watermarkPath)) {
                Files.copy(tmpPath, watermarkPath, StandardCopyOption.REPLACE_EXISTING);
            }
            Files.delete(tmpPath);

            return watermarkPath.toString();

        } catch (IOException e) {
            e.printStackTrace();
            if (tmpPath != null && Files.exists(tmpPath)) {
                try {
                    Files.delete(tmpPath);
                } catch (IOException e2) {
                    e2.printStackTrace();
                }
            }
        }
        return null;
    }

    BiFunction<String, Integer, String> watermark = (video, duration) -> {
        String output = String.format("/tmp/processed-%s.mp4", video.substring(video.lastIndexOf('/') + 1, video.lastIndexOf('.')));
        String watermark_file = getWatermark();
        FFmpegOutputBuilder outBuilder = new FFmpegBuilder()
                .setInput(video)
                .addInput(watermark_file)
                .overrideOutputFiles(true)
                .setComplexFilter("overlay=main_w/2-overlay_w/2:main_h/2-overlay_h/2")
                .addOutput(output);
        if (duration > 0) {
            outBuilder.setDuration(duration.longValue(), TimeUnit.SECONDS);
        }
        FFmpegBuilder builder = outBuilder
                .setFormat("mp4")
                .done();

        FFmpegExecutor executor = null;
        try {
            executor = new FFmpegExecutor(ffmpeg);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        executor.createJob(builder).run();

        return output;
    };

    BiFunction<String, Integer, String> transcode_mp3 = (video, duration) -> {
        return null;
    };

    private final Map<String, BiFunction<String, Integer, String>> operations = Map.of("transcode", transcode_mp3,
                                                                                      "extract-gif", to_gif,
                                                                                      "watermark", watermark);

    private void download(String input_bucket, String key, String download_path) throws Exception {
        File theFile = new File(download_path);
        File theDir = theFile.getParentFile();
        if (theDir != null && !theDir.exists())
            theDir.mkdirs();

        try (InputStream is = blobStore.getBlob(input_bucket, key).getPayload().openStream();
             OutputStream os = new FileOutputStream(theFile)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
        }
    }

    private void upload(String output_bucket, String filename, String upload_path) {
        blobStore.putBlob(output_bucket, blobStore.blobBuilder(filename).payload(new File(upload_path)).build());
    }
}
