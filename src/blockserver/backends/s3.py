import boto3
import tempfile
from botocore.exceptions import ClientError

REGION = 'eu-west-1'
BUCKET = 'qabel'


def file_key(prefix, file_path):
    return '{}/{}'.format(prefix, file_path)


class Transfer:
    def __init__(self):
        client = boto3.client('s3')
        config = boto3.s3.transfer.TransferConfig(
                max_concurrency=10,
                num_download_attempts=10,
        )
        self.transfer = boto3.s3.transfer.S3Transfer(client, config)
        self.s3 = boto3.resource('s3')

    def store(self, prefix, file_path, file):
        self, self.transfer.upload_file(file, BUCKET, file_key(prefix, file_path))

    def retrieve(self, prefix, file_path):
        try:
            with tempfile.NamedTemporaryFile('wb', delete=False) as temp:
                self.transfer.download_file(BUCKET, file_key(prefix, file_path), temp.name)
            return temp.name
        except ClientError:
            return None

