package com.ibm.trl.serverlessbench;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import ai.djl.MalformedModelException;
import ai.djl.inference.Predictor;
import ai.djl.modality.Classifications;
import ai.djl.modality.cv.Image;
import ai.djl.modality.cv.ImageFactory;
import ai.djl.modality.cv.transform.CenterCrop;
import ai.djl.modality.cv.transform.Normalize;
import ai.djl.modality.cv.transform.Resize;
import ai.djl.modality.cv.transform.ToTensor;
import ai.djl.modality.cv.translator.ImageClassificationTranslator;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.Criteria.Builder;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.translate.TranslateException;
import ai.djl.translate.Translator;

import com.ibm.trl.serverlessbench.wrapper.BenchmarkStorageUtil;
import com.ibm.trl.serverlessbench.wrapper.BenchmarkWrapper;

import io.quarkus.funqy.Funq;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import org.jclouds.blobstore.BlobStore;


public class ImageRecognition {
    private static final double nanosecInSec = 1_000_000_000.0;


    private static Builder<Image, Classifications> model;
    private static final Object lock = new Object();
    private static BlobStore blobStore;
    private static String bucket;


    void onStart(@Observes StartupEvent ev) {
        blobStore = BenchmarkStorageUtil.setupStorage();
        bucket = System.getenv("STORAGE_BUCKET");
    }

    public static class FunInput {
        public String file;
        public String model;
        public String synset;
        public String bucket;
    }

    @Funq("image-recognition")
    @BenchmarkWrapper
    public Map<String, Object> image_recognition(FunInput input) throws IOException {
        Map<String, Object> retVal = new LinkedHashMap<>();
        if (input == null || (input.bucket == null && bucket == null) || input.file == null || input.model == null || input.synset == null) {
            retVal.put("message", "ERROR: ImageRecognition unable to run. file, model, synset and bucket need to be set.");
            return retVal;
        }
        if (input.bucket == null)
            input.bucket = bucket;

        String key = "input/" + input.file;
        String key_path = String.format("/tmp/%s-%s", input.file, UUID.randomUUID());

        String model_key = "input/" + input.model;
        String model_key_path = String.join("/", "/tmp", input.model);

        String synset = "input/" + input.synset;
        String synset_path = String.join("/", "/tmp", input.synset);

        long image_download_begin = System.nanoTime();
        try {
            downloadFile(input.bucket, key, key_path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long image_download_end = System.nanoTime();

        long synset_download_begin = 0L;
        long synset_download_end = 0L;
        long model_download_begin = 0L;
        long model_download_end = 0L;
        long model_process_begin = 0L;
        long model_process_end = 0L;
        if (model != null) {
            synset_download_begin = System.nanoTime();
            synset_download_end = synset_download_begin;
            model_download_begin = System.nanoTime();
            model_download_end = model_download_begin;
            model_process_begin = System.nanoTime();
            model_process_end = model_process_begin;
        } else {
            synchronized(lock) {
                if (model != null) {
                    synset_download_begin = System.nanoTime();
                    synset_download_end = synset_download_begin;
                    model_download_begin = System.nanoTime();
                    model_download_end = model_download_begin;
                    model_process_begin = System.nanoTime();
                    model_process_end = model_process_begin;
                } else {
                    try {
                        synset_download_begin = System.nanoTime();
                        downloadFile(input.bucket, synset, synset_path);
                        synset_download_end = System.nanoTime();

                        model_download_begin = System.nanoTime();
                        downloadFile(input.bucket, model_key, model_key_path);
                        model_download_end = System.nanoTime();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    model_process_begin = System.nanoTime();
                    model = Criteria.builder()
                        .setTypes(Image.class, Classifications.class)
                       .optModelPath(Paths.get(model_key_path));
                    model_process_end = System.nanoTime();
               }
            }
        }

        long process_begin = System.nanoTime();
        Image img = ImageFactory.getInstance().fromFile(Paths.get(key_path));
        img.getWrappedImage();

        String ret = "";
        try {
            Translator<Image, Classifications> translator = ImageClassificationTranslator.builder()
                .addTransform(new Resize(256))
                .addTransform(new CenterCrop(224, 224))
                .addTransform(new ToTensor())
                .addTransform(new Normalize(
                        new float[] {0.485f, 0.456f, 0.406f}, /*mean*/
                        new float[] {0.229f, 0.224f, 0.225f}) /*std*/)
                .optApplySoftmax(true)
                .optSynsetUrl("file:" + synset_path)
                .build();

            Criteria<Image, Classifications> criteria = model.optTranslator(translator).build();
            ZooModel<Image, Classifications> zooModel = criteria.loadModel();
            Predictor<Image, Classifications> predictor = zooModel.newPredictor();
            String tokens = predictor.predict(img).best().getClassName();
            ret = tokens.substring(tokens.indexOf(' ') + 1);
            zooModel.getNDManager().close();
        } catch (ModelNotFoundException | MalformedModelException | IOException | TranslateException e) {
            e.printStackTrace();
        }
        long process_end = System.nanoTime();

        double image_download_time = (image_download_end - image_download_begin) / nanosecInSec;
        double model_download_time = (model_download_end - model_download_begin) / nanosecInSec;
        double synset_download_time = (synset_download_end - synset_download_begin) / nanosecInSec;
        double model_process_time = (model_process_end - model_process_begin) / nanosecInSec;
        double process_time = (process_end - process_begin) / nanosecInSec;


        retVal.put("measurement", Map.of("download_time", image_download_time + model_download_time + synset_download_time,
                                         "compute_time", process_time + model_process_time,
                                         "model_time", model_process_time,
                                         "model_download_time", model_download_time));
        retVal.put("output", Map.of(     "class", ret));

        Files.delete(Paths.get(URI.create("file:///" + key_path)));

        return retVal;
    }

    public void downloadFile(String bucket, String key, String filePath) throws Exception {
        File theFile = new File(filePath);
        File theDir = theFile.getParentFile();
        if (!theDir.exists()) {
            theDir.mkdirs();
        }
        try (InputStream is = blobStore.getBlob(bucket, key).getPayload().openStream();
             OutputStream os = new FileOutputStream(theFile)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
        }
    }
}
