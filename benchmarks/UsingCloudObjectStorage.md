# Using Cloud Object Storage in this Project

## Prerequisites

* Access to a cloud object storage:
  * AWS S3
  * S3-compatible object storage like IBM Cloud Object Storage or local MinIO server
  * Azure Blob Storage
  * Google Cloud Storage
* Data files appropriate for each benchmark

Some of the benchmark programs use cloud object storage to store input/output data.

## S3

The required configurations are:
* Specify credentials using two environment variables `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY` 
  * Or the system properties `serverlessbench.s3-access-key-id` and `serverlessbench.s3-secret-access-key`
  * These are the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` values which you probably already have
    * You can create Access Keys in the [AWS Management Console](https://console.aws.amazon.com/iam/home?#/security_credentials)
  * **The default value for these is `minioadmin` and `minioadmin`**
* Specify an endpoint URL using an environment variable `S3_ENDPOINT` or
  the system property `serverlessbench.s3-endpoint`
  * Default value is `http://localhost:9000`
  * For AWS S3, the endpoint URL must be region-specific, e.g., `https://s3.us-east-1.amazonaws.com`

## Azure Blob Storage
If you deploy the benchmark on Azure as a Function App, you don't need to set any additional environment variables.
The Azure Function App will automatically use the Azure Blob Storage associated with the Function App.

If you want to use a different Azure Blob Storage account, you can set the following environment variables:
* `AzureWebJobsStorage` - the connection string to the Azure Storage account
  * Or the system property `serverlessbench.azureWebJobsStorage`

## Google Cloud Storage
Google Cloud Platform uses OAuth2 for authentication.
Set the client_email and private_key property from the JSON key file as the environment variables `GCP_CLIENT_EMAIL` and `GC_PRIVATE_KEY` (or the system properties `serverlessbench.gcp-client-email` and `serverlessbench.gcp-private-key`).

The required steps to obtain a JSON Key are:
1. Go to the Developer Console.
2. Choose your project.
3. Choose API & auth > Credentials. 
4. Click "Create new Client ID".
5. Select "Service account" and click "Create Client ID". 
   - Details of the new service account will be displayed.
6. Download a JSON key for a service account by clicking Generate new JSON key

---
To run the stand-alone Java version:
```shell
# For S3 storage
export S3_ACCESS_KEY_ID=<KeyID>
export S3_SECRET_ACCESS_KEY=<AccessKey>
export S3_ENDPOINT=<EndpointURL>
# For Azure Blob Storage
export AzureWebJobsStorage=<Connection String>
# For Google Cloud Storage
export GCP_CLIENT_EMAIL=<Client Email>
export GCP_PRIVATE_KEY=<Private Key>

# Name of the Storage Bucket (optional, can be set/overwritten in the request)
export STORAGE_BUCKET=<BucketName>

java -jar benchmarks/<benchName>/target/quarkus-app/quarkus-run.jar
```

To run the stand-alone native version:
```shell
# For S3 storage
export S3_ACCESS_KEY_ID=<KeyID>
export S3_SECRET_ACCESS_KEY=<AccessKey>
export S3_ENDPOINT=<EndpointURL>
# For Azure Blob Storage
export AzureWebJobsStorage=<Connection String>
# For Google Cloud Storage
export GCP_CLIENT_EMAIL=<Client Email>
export GCP_PRIVATE_KEY=<Private Key>

# Name of the Storage Bucket (optional, can be set/overwritten in the request)
export STORAGE_BUCKET=<BucketName>

benchmarks/<benchName>/target/<benchName>-1.0.0-SNAPSHOT-runner
```

Likewise, these environment variables will need to be set as part of a container runtime environment.


## Copying Input Data to Object Storage

We used sample input data published by the ETH Z&uuml;rich team to test many of the benchmarks (https://github.com/spcl/serverless-benchmarks-data.git).

**Input data must be copied into a "input" directory in the object storage bucket.**
Output data will be written to an "output" directory in the same bucket.

If you use the deployer.py script, the input data needs to be stored under the `benchmarks-data/<benchName>` directory.
The script will handle the creation of the bucket, uploading the data, and setting the `STORAGE_BUCKET` environment variable to the deployed function.

## Specifying Object Storage Parameters within a Request

If the `STORAGE_BUCKET` environment variable is not set, the bucket name must be specified in the request.
Bucket name and object keys (AKA file names) are specified as POST data in JSON format.

For example, to send a request to a HTTP-triggered function:
```shell
curl http://localhost:8080/thumbnailer \
     -X POST \
     -H 'Content-Type: application/json' \
     -d '{ "bucket": "Bucket", \
           "objectKey": "test.jpg", \
           "width": 300, \
           "height", 200 }'
```

To send a request as a Knative event:

```shell
curl http://<broker-endpoint>:<port>/ \
     -v \
     -X POST \
     -H 'Ce-Id: 1234' \
     -H 'Ce-Source: curl' \
     -H 'Ce-Specification: 1.0' \
     -H 'Ce-Type: thumbnailer' \
     -H 'Content-Type: application/json' \
     -d '{ "bucket": "Bucket", \
           "objectKey": "test.jpg", \
           "width": 300, \
           "height", 200 }'
```

**Note for Knative events:** The `curl` command simply posts a Cloud Event to the broker and exits,
returning the HTTP status code `202 Accepted`.
(The `-v` option tells `curl` to show the HTTP status code.)
A listener must be configured for the event returned
from the service in order to receive the returned value.
