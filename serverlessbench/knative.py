import os
import platform
import shutil
import sys
import yaml
from datetime import datetime
from tzlocal import get_localzone

from serverlessbench.logger import LoggingBase
from serverlessbench.utils import execute, compute_directory_hash, find_cache, update_cache, calculate_cpu, load_config


class Knative(LoggingBase):
    def __init__(self):
        super().__init__()
        self.docker = None
        self.env = None
        self.mvnw = 'mvnw.cmd' if platform.system() == 'Windows' else 'mvnw'

    def deploy(self, root_path, config, deployments, benchmark_name, benchmark, function_name, native, update):
        self.__precheck()
        submodule_path = os.path.join(root_path, "benchmarks", benchmark_name)
        memory = benchmark['memory'][0] if isinstance(benchmark['memory'], list) else benchmark['memory']
        timeout = benchmark['timeout']
        storage = benchmark.get('storage', False)
        namespace = config['providers']['knative']['namespace']

        s3_endpoint = config['providers']['knative'].get('s3_endpoint')
        s3_access_key_id = config['providers']['knative'].get('s3_access_key_id')
        s3_secret_access_key = config['providers']['knative'].get('s3_secret_access_key')

        image_registry = config['providers']['knative']['image-registry']
        image_group = config['providers']['knative']['image-group']
        image_name = f'{image_registry}/{image_group}/quarkus-{benchmark_name}:{"native" if native else "jvm"}'

        if storage:
            if s3_endpoint is None or s3_access_key_id is None or s3_secret_access_key is None:
                self.logging.info("No S3 endpoint/credentials found in config. Please provide them.")
                sys.exit(1)
            else:
                self.env = {"AWS_ACCESS_KEY_ID": s3_access_key_id, "AWS_SECRET_ACCESS_KEY": s3_secret_access_key}

        src_hash = compute_directory_hash(os.path.join(submodule_path, 'src'))
        target_hash = compute_directory_hash(os.path.join(submodule_path, 'target')) if os.path.exists(
            os.path.join(submodule_path, 'target')) else None
        cache = find_cache('knative', benchmark_name, native)

        if not cache or cache['src_hash'] != src_hash or cache['target_hash'] != target_hash:
            self.build(root_path, benchmark_name, submodule_path, native)
            target_hash = compute_directory_hash(os.path.join(submodule_path, 'target'))
            update_cache('knative', benchmark_name, native, src_hash, target_hash)
            self.build_image(submodule_path, native, image_name)
            update_cache('knative', benchmark_name, native, src_hash, target_hash, self.get_image_hash(image_name))
            self.push_image(image_name)
        else:
            self.logging.warning(
                f'Skipping build for "{benchmark_name}" with profile {"knative-native" if native else "knative"}. No changes detected.')

            image_hash = self.get_image_hash(image_name)
            if image_hash is None or cache.get("image_hash") is None or cache.get("image_hash") != image_hash:
                self.build_image(submodule_path, native, image_name)
                update_cache('knative', benchmark_name, native, src_hash, target_hash, self.get_image_hash(image_name))
                self.push_image(image_name)
            else:
                self.logging.warning(
                    f'Skipping image build for "{benchmark_name}" with profile {"knative-native" if native else "knative"}. No changes detected.')

        self.update_func_yaml(submodule_path, function_name, image_name, memory, storage,
                              s3_endpoint, s3_access_key_id, s3_secret_access_key)

        if update:
            self.update_knative_function(function_name, submodule_path)
            return deployments, config

        url = self._deploy_knative_function(submodule_path, function_name, storage, namespace, s3_endpoint)

        deployments['knative']["native" if native else "jvm"][benchmark_name] = {
            'function_name': function_name,
            'url': url,
            'bucket': function_name if 'storage' in benchmark and benchmark['storage'] else None,
            'namespace': namespace
        }

        return deployments, config

    def build(self, root_path, benchmark_name, submodule_path, native):
        mvnw_path = os.path.join(root_path, self.mvnw)
        profile = 'knative-native' if native else 'knative'

        self.logging.info(f'Building benchmark "{benchmark_name}" with profile "{profile}".')
        execute([mvnw_path, 'clean', 'package', '-P', profile], "Error while building project.",
                self.logging, cwd=submodule_path)
        self.logging.debug(f'Benchmark "{benchmark_name}" successfully built with profile "{profile}".')

    def build_image(self, submodule_path, native, image_name):
        # mvnw_path = os.path.join(root_path, 'mvnw')
        # command = [mvnw_path, 'quarkus:image-build',
        #            '-Dquarkus.container-image.registry=' + image_registry,
        #            '-Dquarkus.container-image.group=' + image_group,
        #            '-Dquarkus.container-image.name=' + function_name]
        # if native:
        #     command.append('-Pnative')
        #
        # self.logging.info(f'Building docker image.')
        # execute(command, "Error while building docker image.", self.logging, cwd=submodule_path)
        self.logging.info(f'Building docker image.')
        execute([self.docker, 'build', '-f',
                 'src/main/docker/Dockerfile.native-micro' if native else 'src/main/docker/Dockerfile.jvm',
                 '-t', image_name, '.'],
                "Error while building docker image.", self.logging, cwd=submodule_path)

    def push_image(self, image_name):
        # mvnw_path = os.path.join(root_path, 'mvnw')
        # command = [mvnw_path, 'quarkus:image-push',
        #            '-Dquarkus.container-image.registry=' + image_registry,
        #            '-Dquarkus.container-image.group=' + image_group,
        #            '-Dquarkus.container-image.name=' + function_name]
        # if native:
        #     command.append('-Pnative')
        #
        # self.logging.info(f'Pushing docker image.')
        # execute(command, "Error while pushing docker image.", self.logging, cwd=submodule_path)
        self.logging.info(f'Pushing docker image.')
        execute([self.docker, 'push', image_name],
                "Error while pushing docker image.", self.logging)

    def _deploy_knative_function(self, submodule_path, function_name, storage, namespace, s3_endpoint):
        if storage:
            benchmark_name = os.path.basename(submodule_path)
            benchmarks_data_path = os.path.abspath(
                os.path.join(submodule_path, "../../benchmarks-data", benchmark_name))
            self.create_s3_bucket(s3_endpoint, function_name)
            if os.path.exists(benchmarks_data_path) and os.path.isdir(benchmarks_data_path):
                self.upload_folder_to_s3(s3_endpoint, function_name, benchmarks_data_path, "input")

        self.logging.info(f'Deploying "{function_name}" to Knative.')
        execute(['kn', 'func', 'deploy', '--build=false', '--push=false'],
                "Error while deploying Knative function.", self.logging, cwd=submodule_path)
        service_url = self._get_knative_service_url(function_name, namespace)
        self.logging.debug(f'Function "{function_name}" ({service_url}) deployed to Knative.')
        # self.disable_healthcheck(function_name)
        # execute(['kn', 'service', 'update', function_name,
        #          '--timeout', str(timeout)],
        #         "Error while setting timeout.", self.logging)

        return service_url

    def create_s3_bucket(self, endpoint, bucket_name):
        self.logging.info(f'Creating S3 bucket "{bucket_name}" in "{endpoint}".')
        execute(
            ['aws', '--endpoint-url', endpoint, 's3api', 'create-bucket',
             '--bucket', bucket_name,
             '--region', 'us-east-1'],
            "Error while creating S3 bucket.",
            self.logging, env=self.env
        )
        self.logging.info(f'S3 bucket "{bucket_name}" created.')

    def upload_folder_to_s3(self, endpoint, bucket_name, folder_path, s3_prefix):
        self.logging.info(f'Uploading contents of {folder_path} to s3://{bucket_name}/{s3_prefix}')
        execute(
            ['aws', '--endpoint-url', endpoint, 's3', 'cp', '--recursive', folder_path,
             f's3://{bucket_name}/{s3_prefix}/'],
            "Error while uploading folder to S3.", self.logging, env=self.env
        )
        self.logging.debug(f'Contents of {folder_path} uploaded to s3://{bucket_name}/{s3_prefix}')

    def update_knative_function(self, function_name, submodule_path):
        self.logging.info(f'Updating "{function_name}" on Knative.')
        execute(['kn', 'func', 'deploy', '--build=false', '--push=false'],
                "Error while updating Knative function.", self.logging, cwd=submodule_path)
        self.logging.debug(f'Function "{function_name}" updated on Knative.')
        # self.disable_healthcheck(function_name)

    def _get_knative_service_url(self, function_name, namespace):
        result = execute(['kn', 'func', 'describe', function_name, '-n', namespace, '-o', 'url'],
                         "Error while getting Knative service URL.", self.logging)
        return result.strip()

    def delete(self, function_name, submodule_path, bucket):
        self.logging.info(f'Deleting Knative function.')
        config = load_config()
        self.update_func_yaml(submodule_path, function_name, 'DELETED', 0)

        execute(['kn', 'func', 'delete', '-n', config['providers']['knative']['namespace']],
                "Error while deleting Knative function.", self.logging, cwd=submodule_path)

        if bucket:
            env = {"AWS_ACCESS_KEY_ID": config['providers']['knative']['s3_access_key_id'],
                   "AWS_SECRET_ACCESS_KEY": config['providers']['knative']['s3_secret_access_key']}
            self.logging.info(f'Deleting S3 bucket "{bucket}".')
            execute(
                ['aws', '--endpoint-url', config['providers']['knative']['s3_endpoint'], 's3', 'rb', f's3://{bucket}',
                 '--force'],
                "Error while deleting S3 bucket.", self.logging, env=self.env or env)

        shutil.rmtree(os.path.join(submodule_path, '.func'), ignore_errors=True)
        os.remove(os.path.join(submodule_path, 'func.yaml'))

    def __precheck(self):
        if shutil.which('kn') is None:
            self.logging.error('knative CLI is not installed. Please install it before proceeding.')
            sys.exit(1)
        try:
            execute(['kn', 'func'],
                    "The functions extension for the knative CLI is not installed. Please install it before proceeding.",
                    self.logging, disableCmdLog=True)
        except Exception as e:
            sys.exit(1)

        self.docker = os.path.basename(shutil.which('podman') or shutil.which('docker'))
        if self.docker is None:
            self.logging.error('Neither docker nor podman is installed. Please install one of them before proceeding.')
            sys.exit(1)
        execute([self.docker, 'info'], "Docker/Podman is not running.", self.logging, disableCmdLog=True)

    def get_image_hash(self, image_name):
        try:
            result = execute([self.docker, 'inspect', '--format', '{{.Id}}', image_name], disableCmdLog=True)
            image_id = result.strip()
            if image_id:
                return image_id
            else:
                return None
        except Exception as e:
            return None

    def update_func_yaml(self, submodule_path, function_name, image, memory, storage=False,
                         s3_endpoint=None, s3_access_key_id=None, s3_secret_access_key=None):

        self.logging.info(f'Updating func.yaml for "{function_name}".')
        func_yaml_path = os.path.join(submodule_path, 'func.yaml')
        func_config = {
            'specVersion': '0.36.0',
            'name': function_name,
            'runtime': 'quarkus',
            'image': image,
            'namespace': load_config()['providers']['knative'].get('namespace'),
            'created': datetime.now(get_localzone()).isoformat(),
            'invoke': 'http',
            'deploy': {
                'options': {
                    'scale': {
                        'min': 0,
                        'max': 1,
                        'metric': 'concurrency',
                        'target': 0.01,
                        'utilization': 1
                    },
                    'resources': {
                        # 'requests': {
                        #     'cpu': calculate_cpu(memory),
                        #     'memory': str(memory) + 'Mi'
                        # },
                        'limits': {
                            'cpu': calculate_cpu(memory),
                            'memory': str(memory) + 'Mi',
                            'concurrency': 100
                        }
                    }
                }
            }, 'run': {
                'envs': []
            }}

        if storage:
            func_config['run']['envs'] = [
                {"name": "S3_ENDPOINT", "value": s3_endpoint},
                {"name": "S3_ACCESS_KEY_ID", "value": s3_access_key_id},
                {"name": "S3_SECRET_ACCESS_KEY", "value": s3_secret_access_key},
                {"name": "STORAGE_BUCKET", "value": function_name}
            ]

        if not os.path.exists(os.path.join(submodule_path, '.func')):
            os.makedirs(os.path.join(submodule_path, '.func'))
        with open(os.path.join(submodule_path, '.func', 'built-image'), 'w') as file:
            file.write(image)

        with open(func_yaml_path, 'w') as file:
            yaml.safe_dump(func_config, file)