import base64
import os
import unittest
from unittest import mock

import boto3
import pytest
from moto import mock_secretsmanager

from datasets.utils.secret import Secret, get_current_namespace


case = unittest.TestCase()


def test_secret_value_formatter():
    assert Secret._secret_value_formatter({"example": 1}) == {"example": "1"}
    assert Secret._secret_value_formatter("1") == "1"
    assert Secret._secret_value_formatter(1) == "1"


def test_try_decode_with_json():
    assert Secret()._try_decode_with_json('{"key": "value"}') == {"key": "value"}
    assert Secret(key="key")._try_decode_with_json('{"key": "value"}') == "value"
    with pytest.raises(KeyError):
        Secret(key="wrong_key")._try_decode_with_json('{"key": "value"}')
    assert Secret()._try_decode_with_json('"example_value"') == "example_value"
    with pytest.raises(TypeError):
        Secret(key="key")._try_decode_with_json('"example_value"')
    with pytest.raises(ValueError):
        Secret(key="key")._try_decode_with_json("example_value")
    with pytest.raises(ValueError):
        Secret()._try_decode_with_json(1)


def test_fetch_raw_secret():
    assert Secret(raw_secret={"key": "value"}).value == {"key": "value"}
    assert Secret(raw_secret={"key": "value"}, key="key").value == "value"
    with pytest.raises(KeyError):
        Secret(raw_secret={"key": "value"}, key="wrong_key").value


@mock.patch.dict(os.environ, {"EXAMPLE_SECRET": '{"key": "value"}'})
def test_fetch_env_secret_json_decodable_dict():
    assert Secret(env_var="EXAMPLE_SECRET").value == {"key": "value"}
    assert Secret(env_var="EXAMPLE_SECRET", key="key").value == "value"
    with pytest.raises(KeyError):
        Secret(env_var="EXAMPLE_SECRET", key="wrong_key").value


@mock.patch.dict(os.environ, {"EXAMPLE_SECRET": '"example_value"'})
def test_fetch_env_secret_json_decodable_str():
    assert Secret(env_var="EXAMPLE_SECRET").value == "example_value"
    with pytest.raises(TypeError):
        Secret(env_var="EXAMPLE_SECRET", key="key").value

    with pytest.raises(ValueError):
        Secret(env_var="NON_EXISTENT_ENV_VAR").value


@mock.patch.dict(os.environ, {"EXAMPLE_SECRET": "example_value"})
def test_fetch_env_secret_not_json_decodable():
    assert Secret(env_var="EXAMPLE_SECRET").value == "example_value"
    with pytest.raises(ValueError):
        Secret(env_var="EXAMPLE_SECRET", key="key").value


@mock_secretsmanager
def test_fetch_aws_secret():
    from datasets.utils.secret import logger, secret_cache

    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name="json-decodable-dict", SecretString='{"key": "value"}')
    conn.create_secret(Name="json-decodable-str", SecretString='"example_value"')
    conn.create_secret(Name="not-json-decodable", SecretString="example_value")
    conn.create_secret(Name="empty", SecretString="")

    # Json decodable dict
    assert Secret(aws_secret_arn="json-decodable-dict").value == {"key": "value"}
    # Test secret is cached
    assert secret_cache["aws_secret"]["json-decodable-dict"] == '{"key": "value"}'
    # Test secret fetched from cache
    conn.update_secret(SecretId="json-decodable-dict", SecretString='{"key": "new_value"}')
    with case.assertLogs(logger=logger, level="INFO") as cm:
        assert Secret(aws_secret_arn="json-decodable-dict", key="key").value == "value"
    assert any("Using secret from cache json-decodable-dict" in log for log in cm.output)
    # Test force_reload
    assert Secret(aws_secret_arn="json-decodable-dict", key="key", force_reload=True).value == "new_value"
    # Test cache updated after force_reload
    assert secret_cache["aws_secret"]["json-decodable-dict"] == '{"key": "new_value"}'

    with pytest.raises(KeyError):
        Secret(aws_secret_arn="json-decodable-dict", key="wrong_key").value

    # Json decodable str
    assert Secret(aws_secret_arn="json-decodable-str").value == "example_value"
    with pytest.raises(TypeError):
        Secret(aws_secret_arn="json-decodable-str", key="key").value

    # Not json decodable str
    assert Secret(aws_secret_arn="not-json-decodable").value == "example_value"
    with pytest.raises(ValueError):
        Secret(aws_secret_arn="not-json-decodable", key="key").value

    # Empty string
    with pytest.raises(ValueError):
        Secret(aws_secret_arn="empty").value


@mock.patch("datasets.utils.secret.get_current_namespace")
@mock.patch("datasets.utils.secret.config")
@mock.patch("datasets.utils.secret.client")
def test_fetch_cluster_secret(client, config, namespace):
    from datasets.utils.secret import logger, secret_cache

    example_cluster_secret = {
        "key": base64.b64encode(b"value"),
    }

    example_new_cluster_secret = {
        "key": base64.b64encode(b"new_value"),
    }

    client.CoreV1Api.return_value.read_namespaced_secret.return_value.data = example_cluster_secret
    assert Secret(cluster_secret_name="test").value == {"key": "value"}
    # Test secret is cached
    assert secret_cache["cluster_secret"]["test"] == {"key": "value"}
    # Test secret fetched from cache
    client.CoreV1Api.return_value.read_namespaced_secret.return_value.data = example_new_cluster_secret
    with case.assertLogs(logger=logger, level="INFO") as cm:
        assert Secret(cluster_secret_name="test", key="key").value == "value"
    assert any("Using secret from cache test" in log for log in cm.output)
    # Test force_reload
    assert Secret(cluster_secret_name="test", key="key", force_reload=True).value == "new_value"
    # Test cache updated after force_reload
    assert secret_cache["cluster_secret"]["test"] == {"key": "new_value"}

    with pytest.raises(KeyError):
        Secret(cluster_secret_name="test", key="wrong_key").value


def test_variable_validation():
    with pytest.raises(ValueError):
        Secret(env_var="TEST_ENV_VAR", raw_secret={"key": "value"}).value

    with pytest.raises(ValueError):
        Secret(raw_secret={"key": "value"}, key=1).value


def test_get_current_namespace():
    with mock.patch("os.path.exists", return_value=True):
        with mock.patch("builtins.open", mock.mock_open(read_data="test_namespace")):
            assert get_current_namespace() == "test_namespace"

    with mock.patch("os.path.exists", return_value=False):
        with pytest.raises(RuntimeError):
            assert get_current_namespace()
