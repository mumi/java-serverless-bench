package com.ibm.trl.serverlessbench;

import com.ibm.trl.serverlessbench.wrapper.BenchmarkStorageUtil;
import com.ibm.trl.serverlessbench.wrapper.BenchmarkWrapper;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import java.nio.file.Files;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;

import io.quarkus.funqy.Funq;

public class Compress {
    private static final double nanosecInSec = 1_000_000_000.0;

    private static final Logger log = Logger.getLogger(Compress.class);
    private static BlobStore blobStore;
    private static String bucket;

    void onStart(@Observes StartupEvent ev) {
        blobStore = BenchmarkStorageUtil.setupStorage();
        bucket = System.getenv("STORAGE_BUCKET");
    }

    private File downloadPath = null;

    public static class FunInput {
        public String input_key;
        public String bucket;
        public boolean debug;
    }

    @Funq
    @BenchmarkWrapper
    public Map<String, Object> compress(FunInput input) throws Exception {
        Map<String, Object> retVal = new LinkedHashMap<>();
        if (input == null || (input.bucket == null && bucket == null) || input.input_key == null) {
            retVal.put("message", "ERROR: Compress unable to run. input_key and bucket need to be set.");
            return retVal;
        }
        if (input.bucket == null)
            input.bucket = bucket;

        String uuid = UUID.randomUUID().toString().substring(0, 8);

        downloadPath=new File(String.format("/tmp/%s-%s", input.input_key, uuid));
        downloadPath.mkdirs();
        long downloadStartTime = System.nanoTime();
        downloadDirectory(input.bucket, input.input_key, downloadPath.toString());
        long downloadStopTime = System.nanoTime();
        long downloadSize = parseDirectory(new File(downloadPath.getPath() + "/" + input.input_key));

        long compressStartTime = System.nanoTime();
        File destinationFile = new File(String.format("%s/%s-%s.zip", downloadPath.toString(), input.input_key, uuid));
        zipDir(destinationFile, new File(downloadPath.getPath() + "/" + input.input_key));
        long compressStopTime = System.nanoTime();

        long uploadStartTime = System.nanoTime();
        String archiveName = String.format("%s-%s.zip", input.input_key, uuid);
        uploadFile(input.bucket, "output/" + archiveName, destinationFile.toString());
        long uploadStopTime = System.nanoTime();
        long compressSize = destinationFile.length();

        try {
            if (!input.debug)
                deleteFile(input.bucket, "output/" + archiveName);
        } catch (Exception e) {
            log.error("Exception deleting from cloud storage: " + e);
        }

        try {
            deleteLocalDir(downloadPath);
        } catch (Exception e) {
            log.error("Exception deleting from local filesystem: " + e);
        }

        retVal.put("input_key", input.input_key);
        retVal.put("measurement", Map.of("download_time", (downloadStopTime - downloadStartTime) / nanosecInSec,
                                        "compress_time", (compressStopTime - compressStartTime) / nanosecInSec,
                                        "upload_time", (uploadStopTime - uploadStartTime) / nanosecInSec,
                                        "download_size", Long.toString(downloadSize),
                                        "compress_size", Long.toString(compressSize)));
        return retVal;
    }

    private void deleteFile(String bucket, String key) {
        log.debug("Deleting "+key+" from bucket "+bucket+".");
        blobStore.removeBlob(bucket, key);
    }


    private void uploadFile(String bucket, String key, String filePath) {
        log.debug("Uploading "+filePath+" as "+key+" to bucket "+bucket+".");
        blobStore.putBlob(bucket, blobStore.blobBuilder(key).payload(new File(filePath)).build());
    }

    private void downloadFile(String bucket, String key, String filePath) throws Exception {
        log.debug("Downloading "+filePath+" as "+key+" from bucket "+bucket+".");
        File theFile = new File(filePath);
        File theDir = theFile.getParentFile();
        if (!theDir.exists()) {
            theDir.mkdirs();
        }

        Blob blob = blobStore.getBlob(bucket, key);
        if (blob == null) {
            throw new FileNotFoundException("Blob " + key + " not found in container " + bucket);
        }

        try (InputStream is = blob.getPayload().openStream();
             OutputStream os = new FileOutputStream(theFile)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
        }
    }

    private void downloadDirectory(String bucket, String prefix, String dirPath) throws Exception {
        log.debug("Downloading " + dirPath + " with prefix input/" + prefix + " from bucket " + bucket + ".");

        PageSet<? extends StorageMetadata> blobs = blobStore.list(bucket,
                new ListContainerOptions().recursive().prefix("input/" + prefix));

        for (StorageMetadata metadata : blobs) {
            String blobName = metadata.getName();
            String relativePath = blobName.substring(("input/" + prefix).length());
            Path filePath = Paths.get(dirPath, prefix, relativePath);

            downloadFile(bucket, blobName, filePath.toString());
        }
    }

    public long parseDirectory(File dir) {
        return calculateDirectorySize(dir);
    }

    private long calculateDirectorySize(File dir) {
        long size = 0;
        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.isFile()) {
                size += file.length();
            } else if (file.isDirectory()) {
                size += calculateDirectorySize(file);
            }
        }
        return size;
    }

    public void zipDir(File dstFile, File srcDir) throws IOException {
        log.debug("in zipDir(): source directory: " + srcDir + " destination file: " + dstFile);
        try (FileOutputStream fos = new FileOutputStream(dstFile);
             ZipOutputStream zipOut = new ZipOutputStream(fos)) {

            zipFile(srcDir, srcDir.getName(), zipOut, dstFile.getName());
        }
    }

    private void zipFile(File fileToZip, String fileName, ZipOutputStream zipOut, String dstFileName) throws IOException {
        if (fileToZip.isHidden()) {
            return;
        }

        if (fileToZip.isDirectory()) {
            File[] children = fileToZip.listFiles();
            if (children != null) {
                for (File childFile : children) {
                    zipFile(childFile, fileName + "/" + childFile.getName(), zipOut, dstFileName);
                }
            }
            return;
        }

        if (fileToZip.getName().equals(dstFileName)) {
            return;
        }

        try (FileInputStream fis = new FileInputStream(fileToZip)) {
            ZipEntry zipEntry = new ZipEntry(fileName);
            zipOut.putNextEntry(zipEntry);
            byte[] bytes = new byte[16384];
            int length;
            while ((length = fis.read(bytes)) >= 0) {
                zipOut.write(bytes, 0, length);
            }
            zipOut.closeEntry();
        }
    }

    private void deleteLocalDir(File file) {
        log.debug("Deleting from local file system: "+file.toPath());
        File[] flist = file.listFiles();
        if (flist != null)
            for (File f : flist)
                if (!Files.isSymbolicLink(f.toPath()))
                    deleteLocalDir(f);
        file.delete();
    }

    public void finalize() {
        if (downloadPath != null) {
            log.info("Compress: Running finalizer.");
            try {
                deleteLocalDir(downloadPath);
            } catch (Exception e) {
                log.error("Exception deleting from local filesystem: " + e);
            }
        }
    }
}
