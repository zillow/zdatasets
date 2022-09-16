import base64
import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Union

import boto3
from kubernetes import client, config
from tenacity import (
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_fixed,
)


logger = logging.getLogger(__name__)

# Ensure we return a non-None value
# Prefer to throw exception rather than returning None if secret can't be found
secret_return_type = Union[str, Dict[str, str]]

# Global secret cache to prevent redundant
# secret retrieval calls to external system if possible
secret_cache = defaultdict(dict)


@dataclass
class Secret:
    """
    Dataclass interface for secret retrieval.

    Need to provide exact one of the following secret source variable:
        cluster_secret_name: name of cluster secret.
            secret value to be pulled from cluster secret
        aws_secret_arn: name or arn of aws secret manager secret
            secret value to be pulled from aws secret manager through boto3
        env_var: env var name containing the secret value
            secret value to be pulled from env var
        raw_secret: raw secret value
            secret value to be pulled directly through the variable

    Optional variables:
        key: if secret source contains multiple secrets as a dict,
             return secret[key] if provided, otherwise return full secret
        force_reload: ignore cached secret (if exists) and force reload from    source

    Interface requirement:
        A "value" property to return the actual secret value,
        either as a str (for single secret value) or a Dict[str, str] (for a group of secrets)
    """

    cluster_secret_name: str = None
    aws_secret_arn: str = None
    env_var: str = None
    raw_secret: secret_return_type = None

    key: str = None

    force_reload: bool = False

    @property
    def value(self) -> secret_return_type:
        """
        Returns:
            if key is not None: key field value of secret
            if key is None: entire secret
        """
        self._variable_validation()

        if self.cluster_secret_name:
            return self._fetch_cluster_secret()
        if self.aws_secret_arn:
            return self._fetch_aws_secret()
        if self.env_var:
            return self._fetch_env_secret()
        if self.raw_secret:
            return self._fetch_raw_secret()

    def _variable_validation(self):
        """
        Ensure exact one secret source variable provided.
        Ensure key is of str type if provided
        """
        secret_source_variables = ["cluster_secret_name", "aws_secret_arn", "env_var", "raw_secret"]

        secret_source_variable_used = [
            getattr(self, variable) is not None for variable in secret_source_variables
        ]

        if secret_source_variable_used.count(True) != 1:
            raise ValueError("Must provide exact one secret source variable!")

        if self.key is not None and not isinstance(self.key, str):
            raise ValueError("key should be an string!")

    def _fetch_cluster_secret(self) -> secret_return_type:
        # Try to fetch from cache first
        global secret_cache
        secret_from_cache = secret_cache["cluster_secret"].get(self.cluster_secret_name)
        if secret_from_cache is not None and not self.force_reload:
            logger.info(f"Using secret from cache {self.cluster_secret_name}")
            secret_value = secret_from_cache
        else:
            config.load_incluster_config()
            core_api = client.CoreV1Api()
            namespace = get_current_namespace()
            for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(5), reraise=True):
                with attempt:
                    raw_secret_value = core_api.read_namespaced_secret(
                        self.cluster_secret_name, namespace
                    ).data

            # Ensure we convert to Dict[str, str] type
            secret_value = {}
            for k, v in raw_secret_value.items():
                # The last decode("utf-8") converts bytes type to str
                secret_value[str(k)] = base64.b64decode(v).decode("utf-8")
            secret_cache["cluster_secret"][self.cluster_secret_name] = secret_value

        return secret_value[self.key] if self.key is not None else secret_value

    def _fetch_aws_secret(self) -> secret_return_type:
        global secret_cache
        secret_from_cache = secret_cache["aws_secret"].get(self.aws_secret_arn)
        if secret_from_cache is not None and not self.force_reload:
            logger.info(f"Using secret from cache {self.aws_secret_arn}")
            secret_value = secret_from_cache
        else:
            # We have AWS_REGION env var injected in most of our workloads
            secrets_manager_client = boto3.Session(region_name=os.getenv("AWS_REGION", "us-west-2")).client(
                "secretsmanager"
            )

            for attempt in Retrying(
                retry=retry_if_exception(
                    lambda e: e.response["Error"]["Code"] == "InternalServiceErrorException"
                ),
                stop=stop_after_attempt(3),
                wait=wait_fixed(5),
                reraise=True,
            ):
                with attempt:
                    get_secret_value_response = secrets_manager_client.get_secret_value(
                        SecretId=self.aws_secret_arn
                    )

            secret_value = get_secret_value_response.get("SecretString")
            if not secret_value:
                raise ValueError("Empty SecretString response from boto3 calls!")

            secret_cache["aws_secret"][self.aws_secret_arn] = secret_value

        return self._try_decode_with_json(secret_value)

    def _fetch_env_secret(self) -> secret_return_type:
        env_value = os.getenv(self.env_var)
        if env_value is None:
            raise ValueError(f"Env var {self.env_var} does not exist!")
        return self._try_decode_with_json(env_value)

    def _fetch_raw_secret(self) -> secret_return_type:
        self.raw_secret = self._secret_value_formatter(self.raw_secret)
        return self.raw_secret[self.key] if self.key is not None else self.raw_secret

    def _try_decode_with_json(self, secret_value: str) -> secret_return_type:
        if not isinstance(secret_value, str):
            raise ValueError(f"input secret_value should be an string! Getting {type(secret_value)} instead!")

        try:
            decoded_secret_value = json.loads(secret_value)
            decoded_secret_value = self._secret_value_formatter(decoded_secret_value)
            return decoded_secret_value[self.key] if self.key is not None else decoded_secret_value
        except json.decoder.JSONDecodeError:
            # if value is not json compatible, return raw string if key is not provided
            if self.key is not None:
                raise ValueError(f"Secret value is not json decodable! Can not fetch key {self.key}")
            return secret_value

    @staticmethod
    def _secret_value_formatter(secret_value) -> secret_return_type:
        """
        Ensure secret is of type Union[str, Dict[str, str]]
        """
        if isinstance(secret_value, dict):
            converted_secret_value = {}
            for k, v in secret_value.items():
                converted_secret_value[str(k)] = str(v)
            return converted_secret_value
        else:
            return str(secret_value)


def get_current_namespace() -> str:
    """
    Get current namespace if in a kubernetes cluster

    https://stackoverflow.com/questions/46046110/how-to-get-the-current-namespace-in-a-pod
    Returns:
        namespace
    """
    namespace_file = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    if not os.path.exists(namespace_file):
        raise RuntimeError("Could not find current namespace!")
    with open(namespace_file) as f:
        namespace = f.readline().strip()
    logger.info(f"Current namespace: {namespace}")
    return namespace
