import json
import os
import platform
import shutil
import sys

from serverlessbench.logger import LoggingBase
from serverlessbench.utils import execute, compute_directory_hash, find_cache, update_cache, calculate_cpu, save_config, \
    clean_json_output


class GCP(LoggingBase):
    def __init__(self):
        super().__init__()
        self.mvnw = 'mvnw.cmd' if platform.system() == 'Windows' else 'mvnw'
        self.key_file = 'gcloud_key.json'

    def deploy(self, root_path, config, deployments, benchmark_name, benchmark, function_name, native, update):
        self._precheck(config)

        project_id = config['providers']['gcp']['project']
        memory = benchmark['memory'][0] if isinstance(benchmark['memory'], list) else benchmark['memory']
        timeout = benchmark['timeout']
        storage = benchmark.get('storage', False)
        submodule_path = os.path.join(root_path, "benchmarks", benchmark_name)

        region = config['providers']['gcp']['region']

        src_hash = compute_directory_hash(os.path.join(submodule_path, 'src'))
        target_hash = compute_directory_hash(os.path.join(submodule_path, 'target')) if os.path.exists(
            os.path.join(submodule_path, 'target')) else None
        cache = find_cache('gcp', benchmark_name, native)

        if not cache or cache['src_hash'] != src_hash or cache['target_hash'] != target_hash:
            self.build(root_path, benchmark_name, submodule_path, native)
            target_hash = compute_directory_hash(os.path.join(submodule_path, 'target'))
            update_cache('gcp', benchmark_name, native, src_hash, target_hash)
        else:
            self.logging.warning(f"Skipping build for {'gcp-native' if native else 'gcp'}. No changes detected.")

        url = self.deploy_gcp(function_name, memory, timeout, submodule_path, region, project_id,
                              native, storage, update)

        deployments['gcp']["native" if native else "jvm"][benchmark_name] = {
            'function_name': function_name,
            'url': url,
            'bucket': function_name if storage else None
        }

        return deployments, config

    def build(self, root_path, benchmark_name, submodule_path, native):
        mvnw_path = os.path.join(root_path, self.mvnw)
        profile = 'gcp-native' if native else 'gcp'
        self.logging.info(f'Building benchmark "{benchmark_name}" with profile "{profile}".')
        execute([mvnw_path, 'clean', 'package', '-P', profile], "Error while building project.",
                self.logging, cwd=submodule_path)
        self.logging.debug(f'Benchmark "{benchmark_name}" successfully built with profile "{profile}".')

    def deploy_gcp(self, function_name, memory, timeout, submodule_path, region, project, native, storage,
                   update=False):
        if storage:
            self._check_and_load_gcloud_key(project)
            benchmark_name = os.path.basename(submodule_path)
            benchmarks_data_path = os.path.abspath(
                os.path.join(submodule_path, "../../benchmarks-data", benchmark_name))
            if not update:
                self.create_storage_bucket(function_name, project, region)
                if os.path.exists(benchmarks_data_path) and os.path.isdir(benchmarks_data_path):
                    self.upload_folder_to_bucket(project, function_name, benchmarks_data_path, "input")

        if native:
            return self._gcp_native(function_name, memory, timeout, submodule_path, region, project, storage)
        else:
            return self._gcp_jvm(function_name, memory, timeout, submodule_path, region, project, storage)

    def create_storage_bucket(self, bucket_name, project, location):
        self.logging.info(f'Creating GCP Storage bucket "{bucket_name}" in project "{project}".')
        execute(['gcloud', 'storage', 'buckets', 'create', f'gs://{bucket_name}', '--location', location, '--project',
                 project],
                "Error while creating GCP Storage bucket.", self.logging)

    def upload_folder_to_bucket(self, project, bucket_name, folder_path, folder_name):
        self.logging.info(f'Uploading folder "{folder_name}" to GCP Storage bucket "{bucket_name}".')
        execute(
            ['gcloud', 'storage', 'cp', '-r', folder_path, f'gs://{bucket_name}/{folder_name}', '--project', project],
            "Error while uploading folder to GCP Storage bucket.", self.logging)

    def delete(self, function_name, bucket_name, region, project, native):
        self.logging.info(f'Deleting GCP Cloud Function "{function_name}".')
        if native:
            execute(['gcloud', 'run', 'services', 'delete', function_name, '--region', region, '--platform', 'managed',
                     '--quiet', '--project', project],
                    f'Error while deleting cloud run service "{function_name}" from GCP.', self.logging)
        else:
            execute(
                ['gcloud', 'functions', 'delete', function_name, '--region', region, '--quiet', '--project', project],
                f'Error while deleting function "{function_name}" from GCP.', self.logging)
        if bucket_name:
            execute(['gcloud', 'storage', 'rm', '-r', f'gs://{bucket_name}', '--project', project],
                    f'Error while deleting bucket "{bucket_name}" from GCP.', self.logging)
        self.logging.info(f'Deleted GCP Cloud Function "{function_name}".')

    def _gcp_jvm(self, function_name, memory, timeout, submodule_path, region, project, storage):
        deployment_path = os.path.join(submodule_path, 'target', 'deployment')
        command = [
            'gcloud', 'functions', 'deploy', function_name,
            '--entry-point', 'io.quarkus.gcp.functions.http.QuarkusHttpFunction',
            '--runtime', 'java21',
            '--trigger-http',
            '--allow-unauthenticated',
            '--gen2',
            '--source', deployment_path,
            '--memory', str(memory) + 'Mi',
            '--cpu', str(calculate_cpu(memory)),
            '--timeout', str(timeout),
            '--region', region,
            '--project', project,
        ]
        if storage:
            self._check_and_load_gcloud_key(project)
            command.extend([f'--flags-file={self.key_file}', '--update-env-vars', f'STORAGE_BUCKET={function_name}'])

        self.logging.info(f'Deploying "{function_name}" to GCP Cloud Functions.')
        execute(command, f'Error while deploying function "{function_name}" to GCP.', self.logging)
        self.logging.debug(
            f'Function "{function_name}" (https://{region}-{project}.cloudfunctions.net/{function_name}) deployed to GCP Cloud Functions.')

        return f"https://{region}-{project}.cloudfunctions.net/{function_name}"

    def _gcp_native(self, function_name, memory, timeout, submodule_path, region, project, storage):
        self.logging.info(f'Deploying "{function_name}" to GCP Cloud Run.')

        # If no Cloud Run Artifact Registry exists, create one
        self._create_artifact_registry_for_cloud_run(project, region)

        # Native Deployment Part
        command = [
            'gcloud', 'run', 'deploy', function_name,
            '--allow-unauthenticated',
            '--source', submodule_path,
            '--memory', str(memory) + 'Mi',
            '--cpu', str(calculate_cpu(memory)),
            '--timeout', str(timeout),
            '--region', region,
            '--project', project,
        ]
        if storage:
            command.extend([f'--flags-file={self.key_file}', '--update-env-vars', f'STORAGE_BUCKET={function_name}'])

        dockerfile_path = os.path.join(submodule_path, 'src/main/docker/Dockerfile.native-micro')
        symlink_path = os.path.join(submodule_path, 'Dockerfile')

        try:
            self.logging.debug(f'Creating symbolic link for Dockerfile: {symlink_path}')
            os.symlink(os.path.relpath(dockerfile_path, os.path.dirname(symlink_path)), symlink_path)
        except OSError as e:
            self.logging.error(f'Error creating symbolic link for Dockerfile: {e}')

        try:
            execute(command, f'Error while deploying function "{function_name}" to GCP.', self.logging)

            service_url = execute(['gcloud', 'run', 'services', 'describe', function_name, '--region', region,
                                   '--platform', 'managed', '--project', project, '--format', 'value(status.url)'],
                                  f'Error while getting service URL for "{function_name}" from GCP.', self.logging)
            service_url = service_url.strip()
            self.logging.debug(f'Function "{function_name}" ({service_url}) deployed to GCP Cloud Run.')

            return service_url
        finally:
            try:
                self.logging.debug(f'Removing symbolic link for Dockerfile: {symlink_path}')
                os.remove(symlink_path)
            except OSError as e:
                self.logging.error(f'Error removing symbolic link for Dockerfile: {e}')

    def _create_artifact_registry_for_cloud_run(self, project, region):
        """ Is needed for native deployments as they are deployed to Cloud Run."""
        repository_name = "cloud-run-source-deploy"
        artifact_registry_command = [
            'gcloud', 'artifacts', 'repositories', 'list', '--project', project, '--format=json', '--quiet'
        ]
        cmd_stdout: str = execute(artifact_registry_command,
                                  f'Error while listing Artifact Registry repositories in GCP.',
                                  self.logging)
        existing_artifact_registries = json.loads(clean_json_output(cmd_stdout))

        if not any([repo['name'].endswith(repository_name) for repo in existing_artifact_registries]):
            self.logging.info(f'No Repository for Cloud Run Artifacts found. Creating new repository...')
            # Command to create the repository if it does not exist
            create_repository_command = [
                'gcloud', 'artifacts', 'repositories', 'create', {repository_name},
                '--repository-format=docker',
                '--location', region,
                '--description', 'Repository for Cloud Run source deployments',
                '--project', project
            ]
            execute(create_repository_command, f'Error while creating Artifact Registry repository {repository_name}.',
                    self.logging)
            self.logging.info(f'Repository {repository_name} created successfully.')


    def _check_and_load_gcloud_key(self, project):

        def is_correct_state(data):
            return "--update-env-vars" in data and "GCP_CLIENT_EMAIL" in data[
                "--update-env-vars"] and "GCP_PRIVATE_KEY" in data["--update-env-vars"]

        if os.path.exists(self.key_file):
            with open(self.key_file, 'r') as f:
                key_data = json.load(f)
            if is_correct_state(key_data):
                return key_data
        else:
            execute(
                ['gcloud', 'iam', 'service-accounts', 'create', 'benchmarkStorageCredentials', "--project", project],
                'Error while creating service account benchmarkStorageCredentials.', self.logging)
            execute(['gcloud', 'projects', 'add-iam-policy-binding', project,
                     '--member', f'serviceAccount:benchmarkStorageCredentials@{project}.iam.gserviceaccount.com',
                     '--role', 'roles/owner'],
                    'Error while assigning owner rights to service account.', self.logging)
            execute(['gcloud', 'iam', 'service-accounts', 'keys', 'create', self.key_file,
                     '--iam-account', f'benchmarkStorageCredentials@{project}.iam.gserviceaccount.com', "--project",
                     project],
                    'Error while creating key file.', self.logging)
            with open(self.key_file, 'r') as f:
                key_data = json.load(f)

        transformed_data = {
            "--update-env-vars": {
                "GCP_CLIENT_EMAIL": key_data.get('client_email'),
                "GCP_PRIVATE_KEY": key_data.get('private_key')
            }
        }

        with open(self.key_file, 'w') as f:
            json.dump(transformed_data, f, indent=4)

        return transformed_data

    def _precheck(self, config):
        if shutil.which('gcloud') is None:
            self.logging.error('gcloud CLI is not installed. Please install it before proceeding.')
            sys.exit(1)

        self._enable_necessary_apis(config)

    def _enable_necessary_apis(self, config):
        # Can be retrieved via gcloud services list --enabled --project=PROJECT_ID
        # Check if APIs are already marked as enabled in config
        if config['providers']['gcp'].get('apis_enabled', False):
            return

        # Get the project ID from config
        project_id = config['providers']['gcp'].get('project')
        if not project_id:
            self.logging.error(
                "Project ID not found in config. Please provide a project ID and ensure gcloud is authenticated.")
            exit(-1)

        # List of necessary APIs for Benchmarks
        necessary_apis = [
            'artifactregistry.googleapis.com',
            'cloudbuild.googleapis.com',
            'cloudfunctions.googleapis.com',
            'containerregistry.googleapis.com',
            'iam.googleapis.com',
            'iamcredentials.googleapis.com',
            'logging.googleapis.com',
            'monitoring.googleapis.com',
            'pubsub.googleapis.com',
            'run.googleapis.com',
            'serviceusage.googleapis.com',
            'source.googleapis.com',
            'storage-api.googleapis.com',
            'storage-component.googleapis.com'
        ]

        # Fetch currently enabled APIs
        command = [
            'gcloud', 'services', 'list', '--enabled', '--format=json', '--project', project_id
        ]
        output = execute(command, 'Error listing enabled GCP APIs services.', self.logging)

        if output:
            enabled_apis = [api['config']['name'] for api in json.loads(output)]
        else:
            self.logging.error('Failed to fetch enabled APIs.')
            return

        # Enable necessary APIs that are not currently enabled
        for api in necessary_apis:
            if api not in enabled_apis:
                self.logging.info(f"Enabling API: {api}")
                enable_command = ['gcloud', 'services', 'enable', api, '--project', project_id]
                execute(enable_command, f'Error enabling API: {api}.', self.logging)

        # Mark APIs as enabled in config
        config['providers']['gcp']['apis_enabled'] = True
        save_config(config)
        self.logging.debug("All necessary APIs have been enabled.")
