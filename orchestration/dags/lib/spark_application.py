from __future__ import annotations

import copy
import time
from pathlib import Path
from typing import Any

import yaml
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException


GROUP = "sparkoperator.k8s.io"
VERSION = "v1beta2"
PLURAL = "sparkapplications"


def _api() -> client.CustomObjectsApi:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
    return client.CustomObjectsApi()


def _load_manifest(template_path: str, namespace: str, name: str | None, arguments: list[str] | None) -> dict[str, Any]:
    with Path(template_path).open("r", encoding="utf-8") as fp:
        manifest = yaml.safe_load(fp)
    manifest = copy.deepcopy(manifest)
    manifest.setdefault("metadata", {})
    manifest["metadata"]["namespace"] = namespace
    if name:
        manifest["metadata"]["name"] = name
    if arguments is not None:
        manifest.setdefault("spec", {})["arguments"] = arguments
    return manifest


def delete_spark_application(name: str, namespace: str) -> None:
    api = _api()
    try:
        api.delete_namespaced_custom_object(GROUP, VERSION, namespace, PLURAL, name)
    except ApiException as exc:
        if exc.status != 404:
            raise


def submit_spark_application(
    template_path: str,
    namespace: str = "criteo-lakehouse",
    name: str | None = None,
    arguments: list[str] | None = None,
    delete_existing: bool = False,
) -> str:
    manifest = _load_manifest(template_path, namespace, name, arguments)
    app_name = manifest["metadata"]["name"]
    if delete_existing:
        delete_spark_application(app_name, namespace)
        time.sleep(2)
    api = _api()
    try:
        api.create_namespaced_custom_object(GROUP, VERSION, namespace, PLURAL, manifest)
    except ApiException as exc:
        if exc.status == 409:
            api.patch_namespaced_custom_object(GROUP, VERSION, namespace, PLURAL, app_name, manifest)
        else:
            raise
    return app_name


def wait_spark_application(
    name: str,
    namespace: str = "criteo-lakehouse",
    timeout_seconds: int = 3600,
    poll_seconds: int = 20,
) -> str:
    api = _api()
    deadline = time.time() + timeout_seconds
    terminal_success = {"COMPLETED"}
    terminal_failure = {"FAILED", "FAILING", "SUBMISSION_FAILED", "INVALIDATING"}

    while time.time() < deadline:
        obj = api.get_namespaced_custom_object(GROUP, VERSION, namespace, PLURAL, name)
        state = obj.get("status", {}).get("applicationState", {}).get("state", "UNKNOWN")
        print(f"spark_application={name} state={state}", flush=True)
        if state in terminal_success:
            return state
        if state in terminal_failure:
            raise RuntimeError(f"SparkApplication {name} failed with state={state}")
        time.sleep(poll_seconds)
    raise TimeoutError(f"Timed out waiting for SparkApplication {name}")
