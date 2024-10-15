package com.ibm.trl.serverlessbench;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import com.ibm.trl.serverlessbench.wrapper.BenchmarkStorageUtil;
import com.ibm.trl.serverlessbench.wrapper.BenchmarkWrapper;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;

import io.quarkus.funqy.Funq;
import net.sf.jfasta.FASTAFileReader;
import net.sf.jfasta.impl.FASTAElementIterator;
import net.sf.jfasta.impl.FASTAFileReaderImpl;
import org.jclouds.blobstore.BlobStore;

public class DnaVis {
    private static final double nanosecInSec = 1_000_000_000.0;

    private static final Logger log = Logger.getLogger(DnaVis.class);
    private static BlobStore blobStore;
    private static String bucket;

    void onStart(@Observes StartupEvent ev) {
        blobStore = BenchmarkStorageUtil.setupStorage();
        bucket = System.getenv("STORAGE_BUCKET");
    }

    public static class FunInput {
        public String bucket;
        public String file;
        public boolean debug;
    }

    @Funq("dna-visualization")
    @BenchmarkWrapper
    public Map<String, Object> dnavis(FunInput input) {
        Map<String, Object> retVal = new LinkedHashMap<>();
        if (input == null || (input.bucket == null && bucket == null) || input.file == null) {
            retVal.put("message", "ERROR: DnaVis unable to run. file and bucket need to be set.");
            return retVal;
        }
        if (input.bucket == null)
            input.bucket = bucket;

        File inFile = null, outFile = null;

        // Create temporary files
        try {
            inFile  = File.createTempFile("dnavis_input_", ".fasta");
            outFile = File.createTempFile("dnavis_squiggle_", ".json");
            inFile.deleteOnExit();
            outFile.deleteOnExit();
        } catch(IOException e) {
            cleanupAfterException(retVal, log, e, inFile, outFile);
            return retVal;
        }

        // Download input file from object storage
        long download_begin = System.nanoTime();
        try (FileOutputStream fos = new FileOutputStream(inFile);
             BufferedOutputStream os = new BufferedOutputStream(fos)){
            try (InputStream is = blobStore.getBlob(input.bucket, "input/" + input.file).getPayload().openStream()) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                    os.write(buffer, 0, bytesRead);
                }
            }
        } catch (Exception e) {
            cleanupAfterException(retVal, log, e, inFile, outFile);
            return retVal;
        }
        long download_end = System.nanoTime();

        // Transform FASTA to Squiggle
        ArrayList<SquiggleData> plotList = new ArrayList<>();
        double                  process_total = 0.0;
        try (FASTAFileReader fasta = new FASTAFileReaderImpl(inFile)) {
            FASTAElementIterator itr = fasta.getIterator();

            if(!itr.hasNext()) {
                retVal.put("message", "ERROR: No FASTA data");
                cleanFiles(log, inFile, outFile);
                return retVal;
            }

            do {
                process_total += transform(itr.next().getSequence(), plotList);
            } while(itr.hasNext());

        } catch (Exception e) {
            cleanupAfterException(retVal, log, e, inFile, outFile);
            return retVal;
        }

        // Serialize to JSON
        long json_begin = System.nanoTime();
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.writeValue(outFile, plotList.toArray());
        } catch (Exception e) {
            cleanupAfterException(retVal, log, e, inFile, outFile);
            return retVal;
        }
        long json_end = System.nanoTime();

        // Upload Squiggle data (if 'debug' == true)
        long upload_begin = System.nanoTime();
        long upload_end   = upload_begin;
        if(input.debug) {
            try {
                blobStore.putBlob(input.bucket, blobStore.blobBuilder("output/" + input.file).payload(outFile).build());
            } catch (Exception e) {
                cleanupAfterException(retVal, log, e, inFile, outFile);
                return retVal;
            }
            upload_end = System.nanoTime();
        }

        retVal.put("measurement", Map.of("download_time", (download_end - download_begin) / nanosecInSec,
                                        "compute_time", process_total,
                                        "serialize_time", (json_end - json_begin) / nanosecInSec,
                                        "upload_time", (upload_end - upload_begin) / nanosecInSec));
        retVal.put("output", Map.of("bucket", input.bucket,
                                    "key", "output/" + input.file));
        cleanFiles(log, inFile, outFile);
        return retVal;
    }

    private static double transform(String seq, ArrayList<SquiggleData> list) {
        int      len = seq.length();
        double   curX = 0.0;
        double   curY = 0.0;

        double[] x = new double[len*2];
        double[] y = new double[len*2];

        long process_begin = System.nanoTime();
        for(int i = 0; i < len; i++, curX += 1.0) {
            switch(seq.charAt(i)) {
                case 'A': case 'a':  y[i * 2] = curY + 0.5; y[i * 2 + 1] = curY; break;
                case 'C': case 'c':  y[i * 2] = curY - 0.5; y[i * 2 + 1] = curY; break;
                case 'G': case 'g':  y[i * 2] = curY + 0.5; y[i * 2 + 1] = curY + 1.0; curY += 1.0; break;
                case 'T': case 't':  y[i * 2] = curY - 0.5; y[i * 2 + 1] = curY - 1.0; curY -= 1.0; break;
                default:             y[i * 2] = curY;       y[i * 2 + 1] = curY; break;
            }
            x[i * 2]     = curX;
            x[i * 2 + 1] = curX + 0.5;
        }
        long process_end = System.nanoTime();

        list.add(new SquiggleData(x, y));

        return (process_end - process_begin)/nanosecInSec;
    }

    void cleanupAfterException(Map<String, Object> retVal, Logger log, Exception e, File inFile, File outFile) {
        retVal.put("message", e.toString());
        log.info(Arrays.toString(e.getStackTrace()).replace(", ", "\n    "));
        cleanFiles(log, inFile, outFile);
    }

    private static void cleanFiles(Logger log, File... files) {
        for(File file : files) {
            try {
                if(file != null) {
                    Files.delete(file.toPath());
                }
            } catch (Exception e) {
                log.info(Arrays.toString(e.getStackTrace()).replace(", ", "\n    "));
            }
        }
    }

    @JsonAutoDetect(fieldVisibility = Visibility.ANY)
    public static class SquiggleData {
        public double[] x;
        public double[] y;

        SquiggleData(double[] x, double[] y) {
            this.x = x;
            this.y = y;
        }

        public SquiggleData() {
            x = null;
            y = null;
        }
    }
}
