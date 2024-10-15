# java-serverless-bench Project

This project contains a benchmark suite for Java (Quarkus) applications that can be deployed as AWS Lambda, Google Cloud Functions, Azure Functions and Knative Functions. 
It is easily extensible with other programmes and offers many useful features, such as cloud agnostic object store access (with Apache jClouds) and a deployer tool. 
The existing applications are based on the [knative-quarkus-bench](https://github.com/IBM/knative-quarkus-bench) project, where applications have been ported from [SeBS: Serverless Benchmark Suite](https://github.com/spcl/serverless-benchmarks), which was developed by researchers at ETH Z&uuml;rich.

Quarkus is a cloud-native Java framework based on modern standard APIs.
See https://quarkus.io/ for more information.

## Prerequisites

* Java 21 or higher (need JDK to build from source)
* Maven 3.6.2+ (3.9.6+ is recommended)
* Docker or Podman
* Linux or MacOS (Use WSL on Windows)

## Quick Start
If you only want to deploy the applications, use the deployer. Execute the prepare.py beforehand:
`python prepare.py`.

Depending on where you want to deploy to, you must have the following CLIs installed and be logged in:
* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
* [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
* [Gcloud CLI](https://cloud.google.com/sdk/docs/install)
* [Knative CLI](https://knative.dev/docs/functions/install-func/) + [Functions extension](https://knative.dev/docs/functions/install-func/#__tabbed_2_1)

The providers must also be configured in `config.json`.

You should then be able to use the deployer:

```bash
Usage: deployer.py create [OPTIONS]

Options:
  -b, --benchmarks [image-recognition|video-processing|dna-visualization|thumbnailer|compress|uploader|dynamic-html|graph-bfs|graph-mst|graph-pagerank]
                                  Benchmarks to deploy
  -n, --native                    Native deployment
  -p, --provider [aws|azure|gcp|knative]
                                  Provider  [required]
  --help                          Show this message and exit.

```