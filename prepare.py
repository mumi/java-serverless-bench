#!/usr/bin/env python3

import argparse
import os
import subprocess
import platform
import shutil
import sys


def execute(cmd, cwd=None):
    ret = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, cwd=cwd
    )
    if ret.returncode:
        raise RuntimeError(
            "Running {} failed!\n Output: {}".format(cmd, ret.stdout.decode("utf-8"))
        )
    return ret.stdout.decode("utf-8")


def check_java():
    try:
        java_version_output = execute("java -version")
        if "21" in java_version_output:
            print("Java 21 is installed.")
        else:
            print("Java 21 is not installed or not set in PATH. Please install Java 21 and set it in PATH.")
            sys.exit(1)
    except RuntimeError as e:
        print("Java is not installed or not set in PATH. Please install Java 21 and set it in PATH.")
        print(e)
        sys.exit(1)

    java_home = os.environ.get('JAVA_HOME')
    if java_home:
        print(f"JAVA_HOME: {java_home}")
    else:
        print("JAVA_HOME is not set.")


def check_container_tool():
    docker = os.path.basename(shutil.which('podman') or shutil.which('docker'))
    if docker is None:
        print('Neither docker nor podman is installed. Please install one of them before proceeding.')
        sys.exit(1)
    else:
        print(f"Docker/podman is installed.")


parser = argparse.ArgumentParser(description="Prepare the environment.")
parser.add_argument('--venv', metavar='DIR', type=str, default=".venv",
                    help='destination of local Python virtual environment')
parser.add_argument('--python-path', metavar='DIR', type=str,
                    default=os.path.basename(shutil.which('python3') or shutil.which('python')),
                    help='Path to local Python installation.')
args = parser.parse_args()

env_dir = args.venv

activate_script = os.path.join(env_dir, "Scripts\\activate") if platform.system() == "Windows" else f"source {os.path.join(env_dir, 'bin/activate')}"
pip_executable = os.path.basename(shutil.which('pip3') or shutil.which('pip'))

if not os.path.exists(env_dir):
    print(f"Creating Python virtualenv at {env_dir}")
    execute(f"{args.python_path} -m venv {env_dir}")
    execute(f"{activate_script} && pip install --upgrade pip")
else:
    print(f"Using existing Python virtualenv at {env_dir}")

print("Install Python dependencies with pip")
execute(f"{activate_script} && pip install -r requirements.txt --upgrade")

check_java()

check_container_tool()

mvnw = 'mvnw.cmd' if platform.system() == 'Windows' else './mvnw'

maven_cmd_1 = f"{mvnw} install:install-file -Dfile=serverlessbench/jclouds-allblobstore-2.6.1-SNAPSHOT-modified.jar -DgroupId=org.apache.jclouds -DartifactId=jclouds-allblobstore -Dversion=2.6.1-SNAPSHOT-modified -Dpackaging=jar"
maven_cmd_2 = f"{mvnw} clean install"

script_dir = os.path.dirname(os.path.realpath(__file__))

print(f"Executing Maven command: {maven_cmd_1}")
execute(maven_cmd_1, cwd=script_dir)

print(f"Executing Maven command: {maven_cmd_2}")
execute(maven_cmd_2, cwd=script_dir)

additional_info = f"""
==========================================================
                IMPORTANT INFORMATION:
==========================================================
If you want to use the benchmarker on Azure Functions, you need to run:
    {mvnw} initialize -P azure-bytecode-modification
once. This and many other features do not work on Windows, consider using WSL.

If you are an ARM user, keep in mind that the GraalVM native images are compiled for amd64 and therefore take 10-20 minutes. 
For local development, you can set the quarkus.native.builder-image to mandrel-arm64-builder-image in benchmarks/pom.xml. 
If you want to build a Container, you would also have to adapt the Dockerfiles, as it would otherwise be built for amd64.
==========================================================
"""

print(additional_info)
print(f"\nTo activate the virtual environment, run:\n{activate_script}\n")
