#!/usr/bin/env python3
from builtins import list

import click
import os
import sys
import uuid
import platform
from tabulate import tabulate

from serverlessbench.aws import AWS
from serverlessbench.azure import Azure
from serverlessbench.gcp import GCP
from serverlessbench.knative import Knative
from serverlessbench.logger import LoggingBase
from serverlessbench.utils import execute, load_config, load_deployments, save_config, save_deployments, find_deployment, \
    get_benchmark_names


class Deployer(LoggingBase):
    def __init__(self):
        super().__init__()
        self.aws = AWS()
        self.azure = Azure()
        self.gcp = GCP()
        self.knative = Knative()
        self.root_path = os.getcwd()
        self.mvwn = 'mvnw.cmd' if platform.system() == 'Windows' else 'mvnw'
        self.mvnw_path = os.path.join(self.root_path, self.mvwn)
        self.config = load_config()

    def delete(self, provider: str, benchmarks: list, native: bool):
        self.logging.info(f"Deleting benchmarks {benchmarks} from {provider}{' as native.' if native else '.'}")

        for benchmark_name in benchmarks:
            deployment = find_deployment(benchmark_name, provider, native)
            if deployment is None:
                self.logging.warning(
                    f"Benchmark {benchmark_name} not found in {provider}{' as native.' if native else '.'}")
                return

            if provider == 'gcp':
                self.gcp.delete(deployment['function_name'], deployment.get('bucket'), self.config['providers']['gcp'].get('region'),self.config['providers']['gcp'].get('project'), native)
            elif provider == 'aws':
                self.aws.delete(deployment['function_name'], deployment.get('bucket'), self.config['providers']['aws'].get('region'))
            elif provider == 'azure':
                self.azure.delete(deployment['function_name'], deployment['account_name'])
            elif provider == 'knative':
                self.knative.delete(deployment['function_name'], os.path.join(self.root_path, "benchmarks", benchmark_name), deployment.get('bucket'))
            else:
                self.logging.error("Unsupported provider. Please use 'aws', 'azure', 'gcp' or 'knative'.")
                sys.exit(1)

            deployments = load_deployments()
            del deployments[provider]["native" if native else "jvm"][benchmark_name]
            save_deployments(deployments)

    def create(self, provider: str, benchmarks: list, native: bool):
        self.logging.info(f"Deploying benchmarks {benchmarks} to {provider}{' as native.' if native else '.'}")

        benchmark_map = {benchmark['name']: benchmark for benchmark in self.config['benchmarks']}

        for benchmark_name in benchmarks:
            benchmark = benchmark_map.get(benchmark_name)
            if benchmark:
                function_name = f'quarkus{"-native" if native else ""}-{benchmark_name}-{str(uuid.uuid4())[0:8]}'

                deployment = find_deployment(benchmark_name, provider, native)
                update = False
                if deployment is not None:
                    function_name = deployment['function_name']
                    self.logging.warning(f"Function {function_name} already exists. Updating it.")
                    update = True

                deployments = load_deployments()
                if provider == 'gcp':
                    deployments, self.config = self.gcp.deploy(self.root_path, self.config, deployments, benchmark_name, benchmark, function_name, native, update)
                elif provider == 'aws':
                    deployments, self.config = self.aws.deploy(self.root_path, self.config, deployments, benchmark_name, benchmark, function_name, native, update)
                elif provider == 'azure':
                    deployments, self.config = self.azure.deploy(self.root_path, self.config, deployments, benchmark_name, benchmark, function_name, native, update)
                elif provider == 'knative':
                    deployments, self.config = self.knative.deploy(self.root_path, self.config, deployments, benchmark_name, benchmark, function_name, native, update)
                else:
                    self.logging.error("Unsupported provider. Please use 'aws', 'azure', 'gcp' or 'knative'.")
                    sys.exit(1)
                save_deployments(deployments)
                save_config(self.config)


def common_options(f):
    f = click.option('--provider', '-p', required=True, help='Provider',
                     type=click.Choice(['aws', 'azure', 'gcp', 'knative']))(f)
    f = click.option('--native', '-n', is_flag=True, default=False, help='Native deployment')(f)
    f = click.option('--benchmarks', '-b', default=get_benchmark_names(), multiple=True,
                     help='Benchmarks to deploy',
                     type=click.Choice(get_benchmark_names()))(f)
    return f


@click.group()
def cli():
    pass


@cli.command()
@common_options
def create(provider: str, benchmarks: list, native: bool):
    deployer = Deployer()
    deployer.create(provider, benchmarks, native)


@cli.command()
def list():
    deployments = load_deployments()
    table = []
    headers = ["Provider", "Runtime", "Benchmark", "Function Name", "URL"]

    for provider in deployments:
        for runtime in deployments[provider]:
            for benchmark in deployments[provider][runtime]:
                function_name = deployments[provider][runtime][benchmark]['function_name']
                url = deployments[provider][runtime][benchmark]['url']
                table.append([provider, runtime, benchmark, function_name, url])

    print(tabulate(table, headers, tablefmt="pretty"))


@cli.command()
@common_options
def delete(provider: str, benchmarks: list, native: bool):
    deployer = Deployer()
    deployer.delete(provider, benchmarks, native)


if __name__ == "__main__":
    cli()
