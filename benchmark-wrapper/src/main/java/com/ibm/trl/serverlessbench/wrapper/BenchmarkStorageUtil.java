package com.ibm.trl.serverlessbench.wrapper;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jclouds.ContextBuilder;
import org.jclouds.aws.s3.config.AWSS3HttpApiModule;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.rest.ConfiguresHttpApi;
import org.jclouds.s3.S3Client;

import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
public class BenchmarkStorageUtil {

    private static final Logger LOGGER = Logger.getLogger(BenchmarkStorageUtil.class.getName());

    public static BlobStore setupStorage() {
        String gcpClientEmail = System.getenv("GCP_CLIENT_EMAIL") != null ? System.getenv("GCP_CLIENT_EMAIL") :
                ConfigProvider.getConfig().getOptionalValue("serverlessbench.gcp-client-email", String.class).orElse("");
        String gcpPrivateKey = System.getenv("GCP_PRIVATE_KEY") != null ? System.getenv("GCP_PRIVATE_KEY") :
                ConfigProvider.getConfig().getOptionalValue("serverlessbench.gcp-private-key", String.class).orElse("");
        String azureWebJobsStorage = System.getenv("AzureWebJobsStorage") != null ? System.getenv("AzureWebJobsStorage") :
                ConfigProvider.getConfig().getOptionalValue("serverlessbench.azureWebJobsStorage", String.class).orElse("");
        String awsAccessKeyId = System.getenv("S3_ACCESS_KEY_ID") != null ? System.getenv("S3_ACCESS_KEY_ID") :
                ConfigProvider.getConfig().getOptionalValue("serverlessbench.s3-access-key-id", String.class).orElse("minioadmin");
        String awsSecretAccessKey = System.getenv("S3_SECRET_ACCESS_KEY") != null ? System.getenv("S3_SECRET_ACCESS_KEY") :
                ConfigProvider.getConfig().getOptionalValue("serverlessbench.s3-secret-access-key", String.class).orElse("minioadmin");
        String s3Endpoint = System.getenv("S3_ENDPOINT") != null ? System.getenv("S3_ENDPOINT") :
                ConfigProvider.getConfig().getOptionalValue("serverlessbench.s3-endpoint", String.class).orElse("http://localhost:9000");

        s3Endpoint = s3Endpoint.trim();

        if (!(s3Endpoint.startsWith("http://") || s3Endpoint.startsWith("https://"))) {
            LOGGER.warning("[STORAGE] S3_ENDPOINT must start with http:// or https://");
            throw new IllegalArgumentException("S3_ENDPOINT must start with http:// or https://");
        }

        ContextBuilder contextBuilder;
        if (!gcpClientEmail.isEmpty() && !gcpPrivateKey.isEmpty()) {
            LOGGER.warning("[STORAGE] Using Google Cloud Storage");
            contextBuilder = ContextBuilder.newBuilder("google-cloud-storage")
                    .credentials(
                            gcpClientEmail.replace("\"", ""),
                            gcpPrivateKey.replace("\"", "").replace("\\n", "\n"));
        } else if (!azureWebJobsStorage.isEmpty()) {
            LOGGER.warning("[STORAGE] Using Azure Blob Storage");
            String[] parts = azureWebJobsStorage.split(";");
            String azureStorageName = null;
            String azureStorageKey = null;
            for (String part : parts) {
                part = part.trim();
                if (part.startsWith("AccountName=")) {
                    azureStorageName = part.substring("AccountName=".length());
                } else if (part.startsWith("AccountKey=")) {
                    azureStorageKey = part.substring("AccountKey=".length());
                }
            }
            contextBuilder = ContextBuilder.newBuilder("azureblob").credentials(azureStorageName, azureStorageKey);
        } else if (s3Endpoint.startsWith("https://s3.") && s3Endpoint.endsWith(".amazonaws.com")) {
            LOGGER.warning("[STORAGE] Using AWS S3 on endpoint: " + s3Endpoint);
            contextBuilder = ContextBuilder.newBuilder("aws-s3")
                    .credentials(awsAccessKeyId, awsSecretAccessKey)
                    .endpoint(s3Endpoint);

            BucketToRegionHack b2mModule = new BucketToRegionHack();
            contextBuilder.modules(ImmutableSet.of(b2mModule));
            Pattern pattern = Pattern.compile("s3\\.([a-z0-9-]+)\\.amazonaws\\.com");
            Matcher matcher = pattern.matcher(s3Endpoint);
            if (matcher.find()) {
                String region = matcher.group(1);
                b2mModule.setRegion(region);
            } else {
                LOGGER.warning("[STORAGE] Unable to determine region from endpoint: " + s3Endpoint);
                throw new IllegalArgumentException("Unable to determine region from endpoint: " + s3Endpoint);
            }
        } else {
            LOGGER.warning("[STORAGE] Using S3 compatible storage (not AWS) on endpoint: " + s3Endpoint);
            contextBuilder = ContextBuilder.newBuilder("s3")
                    .credentials(awsAccessKeyId, awsSecretAccessKey)
                    .endpoint(s3Endpoint);
        }

        return contextBuilder.buildView(BlobStoreContext.class).getBlobStore();
    }

    /*
     * This class is a hack to work around the fact that the jclouds AWS S3 module
     * tries to determine the region of a bucket by making a request to the AWS API,
     * even if you override the endpoint.
     */
    @ConfiguresHttpApi
    private static class BucketToRegionHack extends AWSS3HttpApiModule {
        private String region;

        public void setRegion(String region) {
            this.region = region;
        }

        @Override
        @SuppressWarnings("Guava")
        protected CacheLoader<String, Optional<String>> bucketToRegion(Supplier<Set<String>> regionSupplier, S3Client client) {
            Set<String> regions = regionSupplier.get();
            if (regions.isEmpty()) {
                return new CacheLoader<String, Optional<String>>() {

                    @Override
                    @SuppressWarnings({"Guava", "NullableProblems"})
                    public Optional<String> load(String bucket) {
                        return Optional.of(BucketToRegionHack.this.region);
                    }

                    @Override
                    public String toString() {
                        return "noRegions()";
                    }
                };
            } else if (regions.size() == 1) {
                final String onlyRegion = Iterables.getOnlyElement(regions);
                return new CacheLoader<String, Optional<String>>() {

                    @Override
                    @SuppressWarnings("NullableProblems")
                    public Optional<String> load(String bucket) {
                        return Optional.of(BucketToRegionHack.this.region);
                    }

                    @Override
                    public String toString() {
                        return "onlyRegion(" + onlyRegion + ")";
                    }
                };
            } else {
                return new CacheLoader<String, Optional<String>>() {
                    @Override
                    @SuppressWarnings("NullableProblems")
                    public Optional<String> load(String bucket) {
                        return Optional.of(BucketToRegionHack.this.region);
                    }

                    @Override
                    public String toString() {
                        return "bucketToRegion()";
                    }
                };
            }
        }
    }
}