import base64
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

import boto3
from botocore.exceptions import ClientError
from kubernetes import client, config
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_fixed


logger = logging.getLogger(__name__)

secret_return_type = Optional[Union[str, Dict[str, Any]]]


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

    An optional key variable in case if secret source contains multiple secrets as a dict
        return secret[key] if provided, otherwise return full secret

    Interface requirement:
        A value property to return the actual secret
    """

    cluster_secret_name: str = None
    aws_secret_arn: str = None
    env_var: str = None
    raw_secret: Union[str, Dict[str, str]] = None

    key: str = None

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
        Ensure exact one secret source variable provided. Throw an Error otherwise.
        """
        secret_source_variables = ["cluster_secret_name", "aws_secret_arn", "env_var", "raw_secret"]

        secret_source_variable_used = [
            getattr(self, variable) is not None for variable in secret_source_variables
        ]

        if secret_source_variable_used.count(True) != 1:
            raise ValueError("Must provide exact one secret source variable!")

    def _fetch_cluster_secret(self) -> secret_return_type:
        config.load_incluster_config()
        core_api = client.CoreV1Api()
        namespace = get_current_namespace()
        secret_value = core_api.read_namespaced_secret(self.cluster_secret_name, namespace).data
        for k, v in secret_value.items():
            secret_value[k] = base64.b64decode(v)
        return secret_value.get(self.key) if self.key is not None else secret_value

    def _fetch_aws_secret(self) -> secret_return_type:
        try:
            # We have AWS_REGION env var injected in most of our workloads
            secrets_manager_client = boto3.Session(region_name=os.getenv("AWS_REGION", "us-west-2")).client(
                "secretsmanager"
            )
            get_secret_value_response = _get_aws_secret_value(self.aws_secret_arn, secrets_manager_client)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "DecryptionFailureException":
                logger.exception(
                    "Secrets Manager can't decrypt the protected secret text using the provided KMS key."
                )
            elif error_code == "ResourceNotFoundException":
                logger.exception("Cannot find the resource asked for.")
            raise e

        secret_value = get_secret_value_response.get("SecretString")
        return self._try_decode_with_json(secret_value)

    def _fetch_env_secret(self) -> secret_return_type:
        env_value = os.getenv(self.env_var)
        return self._try_decode_with_json(env_value)

    def _fetch_raw_secret(self) -> secret_return_type:
        return self.raw_secret.get(self.key) if self.key is not None else self.raw_secret

    def _try_decode_with_json(self, secret_value) -> secret_return_type:
        try:
            decoded_secret_value = json.loads(secret_value)
            return decoded_secret_value.get(self.key) if self.key is not None else self.decoded_secret_value
        except json.decoder.JSONDecodeError:
            # env var value is not json compatible, return raw string instead
            return secret_value


def _retry_if_aws_error(exception: ClientError) -> bool:
    return exception.response["Error"]["Code"] == "InternalServiceErrorException"


@retry(retry=retry_if_exception(_retry_if_aws_error), stop=stop_after_attempt(3), wait=wait_fixed(2))
def _get_aws_secret_value(secret_name, client) -> Dict[str, Any]:
    return client.get_secret_value(SecretId=secret_name)


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
