import hashlib
import json
import os
import platform
import subprocess
from typing import List


def execute(cmd, errorMessage=None, logger=None, cwd=None, disableCmdLog=False, env=None) -> str:
    shell = True if platform.system() == 'Windows' else False

    if not disableCmdLog and logger is not None:
        if cwd:
            logger.debug(f"[{cwd}] {' '.join(cmd)}")
        else:
            logger.debug(' '.join(cmd))

    env_vars = os.environ.copy()
    if env:
        env_vars.update(env)

    ret = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env_vars, shell=shell)

    if ret.returncode:
        if errorMessage is not None and logger is not None:
            logger.error(errorMessage)
        raise RuntimeError(ret.stdout.decode("utf-8"))
    return ret.stdout.decode("utf-8")


def load_config():
    with open('config.json', 'r') as file:
        return json.load(file)


def save_config(config):
    with open('config.json', 'w') as file:
        json.dump(config, file, indent=4)


def clean_json_output(output):
    """
    Filter out non-JSON lines from the stdout to obtain a clean JSON string.
    """
    json_lines = []
    json_started = False

    for line in output.splitlines():
        line = line.strip()
        if line.startswith('['):
            json_started = True
        if json_started:
            json_lines.append(line)

    return '\n'.join(json_lines)


def save_deployments(deployments):
    with open('deployments.json', 'w') as file:
        json.dump(deployments, file, indent=4)


def get_benchmark_names() -> List[str]:
    return [benchmark['name'] for benchmark in load_config()['benchmarks']]


def load_deployments():
    path = 'deployments.json'
    if not os.path.exists(path):
        with open(path, 'w') as file:
            json.dump({}, file)
    with open(path, 'r') as file:
        return json.load(file)


def find_deployment(benchmark_name: str, provider: str, native: bool):
    deployments = load_deployments()
    if provider not in deployments:
        deployments[provider] = {}
    if native:
        if "native" not in deployments[provider]:
            deployments[provider]["native"] = {}
        if benchmark_name not in deployments[provider]["native"]:
            deployments[provider]["native"][benchmark_name] = {}
            save_deployments(deployments)
            return None
        if deployments[provider]["native"][benchmark_name].get('function_name') is None:
            return None
        return deployments[provider]["native"][benchmark_name]
    else:
        if "jvm" not in deployments[provider]:
            deployments[provider]["jvm"] = {}
        if benchmark_name not in deployments[provider]["jvm"]:
            deployments[provider]["jvm"][benchmark_name] = {}
            save_deployments(deployments)
            return None
        if deployments[provider]["jvm"][benchmark_name].get('function_name') is None:
            return None
        return deployments[provider]["jvm"][benchmark_name]


def compute_directory_hash(directory_path):
    hash_obj = hashlib.sha256()

    for root, dirs, files in os.walk(directory_path):
        for file_name in sorted(files):
            file_path = os.path.join(root, file_name)
            with open(file_path, 'rb') as f:
                while chunk := f.read(8192):
                    hash_obj.update(chunk)

    return hash_obj.hexdigest()


def load_cache():
    path = 'cache.json'
    if not os.path.exists(path):
        with open(path, 'w') as file:
            json.dump({}, file)
    with open(path, 'r') as file:
        return json.load(file)


def save_cache(cache):
    with open('cache.json', 'w') as file:
        json.dump(cache, file, indent=4)


def find_cache(provider, benchmark_name, native):
    cache = load_cache()
    if provider not in cache:
        cache[provider] = {}
    if native:
        if "native" not in cache[provider]:
            cache[provider]["native"] = {}
        if benchmark_name not in cache[provider]["native"]:
            cache[provider]["native"][benchmark_name] = {}
            save_cache(cache)
            return None
        if cache[provider]["native"][benchmark_name].get('src_hash') is None:
            return None
        return cache[provider]["native"][benchmark_name]
    else:
        if "jvm" not in cache[provider]:
            cache[provider]["jvm"] = {}
        if benchmark_name not in cache[provider]["jvm"]:
            cache[provider]["jvm"][benchmark_name] = {}
            save_cache(cache)
            return None
        if cache[provider]["jvm"][benchmark_name].get('src_hash') is None:
            return None
        return cache[provider]["jvm"][benchmark_name]


def update_cache(provider, benchmark_name, native, src_hash, target_hash, image_hash=None):
    cache = load_cache()
    if provider not in cache:
        cache[provider] = {}
    if native:
        if "native" not in cache[provider]:
            cache[provider]["native"] = {}
        cache[provider]["native"][benchmark_name]["src_hash"] = src_hash
        cache[provider]["native"][benchmark_name]["target_hash"] = target_hash
    else:
        if "jvm" not in cache[provider]:
            cache[provider]["jvm"] = {}
        cache[provider]["jvm"][benchmark_name]["src_hash"] = src_hash
        cache[provider]["jvm"][benchmark_name]["target_hash"] = target_hash
    if image_hash:
        cache[provider]["native" if native else "jvm"][benchmark_name]["image_hash"] = image_hash
    save_cache(cache)


def calculate_cpu(memory_mib):
    """
    Calculate the CPU allocation for Google Cloud and Kubernetes based on AWS Lambda's memory to CPU proportion.

    Parameters:
    memory_mib (int): The amount of memory in mebibytes (MiB).

    Returns:
    float: The CPU allocation in vCPUs.
    """
    if memory_mib <= 832:
        return round(0.5 / 832 * memory_mib, 3)
    elif memory_mib <= 1769:
        slope1 = (1 - 0.5) / (1769 - 832)
        intercept1 = 0.5 - slope1 * 832
        return round(slope1 * memory_mib + intercept1, 3)
    else:
        slope2 = (2 - 1) / (3008 - 1769)
        intercept2 = 1 - slope2 * 1769
        return round(slope2 * memory_mib + intercept2, 3)
