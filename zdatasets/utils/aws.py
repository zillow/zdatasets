import logging
from typing import Iterable, Optional, Tuple
from urllib.parse import ParseResult, urlparse

import boto3
from botocore.credentials import (
    AssumeRoleCredentialFetcher,
    DeferredRefreshableCredentials,
)
from botocore.session import Session


_logger = logging.getLogger(__name__)


def get_aws_session(role_arn: str = None, profile_name: str = None) -> Session:
    """
    Args:
        role_arn: AWS ARN
        profile_name: AWS profile
    Returns: boto3 Session
    """
    region_name = "us-west-2"

    if profile_name:
        source_session = boto3.Session(profile_name=profile_name, region_name=region_name)
    else:
        source_session = boto3.Session(region_name=region_name)

    if role_arn is None:
        return source_session

    # Fetch assumed role's credentials
    fetcher = AssumeRoleCredentialFetcher(
        client_creator=source_session._session.create_client,
        source_credentials=source_session.get_credentials(),
        role_arn=role_arn,
    )

    # Create new session with assumed role and auto-refresh
    botocore_session = Session()
    botocore_session._credentials = DeferredRefreshableCredentials(
        method="assume-role",
        refresh_using=fetcher.fetch_credentials,
    )

    return boto3.Session(botocore_session=botocore_session, region_name=region_name)


def get_aws_client(role_arn: str, service: str):
    """
    Args:
        role_arn: AWS ARN
        service: AWS service, example: "s3"

    Returns: Returns an AWS client

    """
    session = get_aws_session(role_arn)
    return session.client(service)


def send_sqs_message(
        queue_url: str,
        message_body: str,
        *,  # keyword-only parameters
        role_arn: str = None,
        profile_name: str = None) -> None:
    """
        :param queue_url: the url of SQS to write messages to, i.e.'https://sqs.<region>.amazonaws.com/<account_id>/<queue_name>'
        :param message_body: i.e.  ‘{"date_key": "2023-01-01", "accuracy_threshold": 0.5}'
        :param role_arn: optional
        :param profile_name: optional, mainly for testing/debugging purposes
        :return: None
        Exceptions:
            botocore.exceptions.ClientError  # no permissions to assume role or to SendMessage to SQS
            AWS.SimpleQueueService.NonExistentQueue
            SQS.Client.exceptions.InvalidMessageContents
            SQS.Client.exceptions.UnsupportedOperation
    """
    # Create session from given iam role and/or aws profile
    session = get_aws_session(role_arn, profile_name)

    # Create an SQS client from the session
    sqs = session.client('sqs')

    # Send message to SQS queue
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body
        )

        _logger.debug(f"Successfully sent the message {message_body} to sqs {queue_url} with MessageId {response['MessageId']}")
    except Exception as err:
        _logger.error(f"Failed to send the message {message_body} to sqs {queue_url}")
        raise err


def get_paginated_list_objects_iterator(
    s3_client: boto3.session.Session.client, search: Optional[str] = "Content", **kwargs
) -> Iterable:
    """
    List objects using paginator (to retrieve >1000 objects), and filter by search

    The default search='Content' will let this function return an iterator
        of all objects Content field
    [{u'ETag': '"455724dd89cbd82a34d1fd72de1725c3"',
     u'Key': u'rent_zestimate_2.0_output_sandbox/2020-05-30-04/scores/1003_propattr_qrf_S1.pkl',
     u'LastModified': datetime.datetime(2020, 5, 30, 5, 12, 54, tzinfo=tzlocal()),
     u'Owner': {u'DisplayName': 'datalake-admins',
      u'ID': 'e37414776fcd5b202034a0322bd0b30ba3d8f714cf0ce1633d22a5ac9cfb1f2b'},
     u'Size': 1599704,
     u'StorageClass': 'STANDARD'}, {next obj}, ...]

     To get a iterator of all objects keys only, use search='Content[].Key' instead
     [u'rent_zestimate_2.0_output_sandbox/2020-05-30-04/scores/1003_propattr_qrf_S1.pkl',
     u'rent_zestimate_2.0_output_sandbox/2020-05-30-04/scores/1003_propattr_qrf_S2.pkl',
     u'rent_zestimate_2.0_output_sandbox/2020-05-30-04/scores/1003_propattr_qrf_S3.pkl',
     u'rent_zestimate_2.0_output_sandbox/2020-05-30-04/scores/1003_relation_prr_S1.pkl',
     u'rent_zestimate_2.0_output_sandbox/2020-05-30-04/scores/1007_propattr_qrf_S1.pkl', ...]

     For more usage of search field, check https://jmespath.org/

     If search is None, return the raw paginator (a PageIterator)

    Args:
        s3_client: s3 client
        search: search within page_iterator https://boto3.amazonaws.com/v1/documentation/api/
            latest/guide/paginators.html#filtering-results-with-jmespath
        kwargs: passed into paginator, e.g., Bucket, Prefix, Delimiter, etc.
    Returns:
        The raw PageIterator if search is None
        Filtered iterator otherwise
    """
    paginator = s3_client.get_paginator("list_objects")
    # kwargs same as list_objects usage: Bucket, Prefix, etc.
    page_iterator = paginator.paginate(**kwargs)
    return page_iterator.search(search) if search else page_iterator


def get_s3_bucket_key(path: str) -> Tuple[str, str]:
    """
    Splits S3 path into bucket and key
    Args:
        path: S3 Path

    Returns: bucket and key
    """
    parse_result: ParseResult = urlparse(path)
    bucket = parse_result.netloc
    key = parse_result.path.lstrip("/")
    return bucket, key
