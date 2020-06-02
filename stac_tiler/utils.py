"""stac-tiler utils."""

from boto3.session import Session as boto3_session


def s3_get_object(bucket, key, client: boto3_session.client = None) -> bytes:
    """GetObject from S3."""
    if not client:
        session = boto3_session()
        client = session.client("s3")

    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()
