import boto3
import tempfile
from botocore.exceptions import ClientError

from blockserver.backends.util import StorageObject

REGION = 'eu-west-1'
BUCKET = 'qabel'


def file_key(storage_object: StorageObject):
    return '{}/{}'.format(storage_object.prefix, storage_object.file_path)


class Transfer:
    def __init__(self):
        self.s3 = boto3.resource('s3')

    def store(self, storage_object: StorageObject):
        obj = self.s3.Object(BUCKET, file_key(storage_object))
        with open(storage_object.local_file, 'rb') as f_in:
            response = obj.put(Body=f_in)
            return storage_object._replace(etag=response['ETag'])

    def retrieve(self, storage_object: StorageObject):
        obj = self.s3.Object(BUCKET, file_key(storage_object))
        try:
            if storage_object.etag:
                response = obj.get(IfNoneMatch=storage_object.etag, )
            else:
                response = obj.get()
        except ClientError as e:
            status = e.response['ResponseMetadata']['HTTPStatusCode']
            if status == 304:
                return storage_object._replace(local_file=None)
            else:
                return None

        with tempfile.NamedTemporaryFile('wb', delete=False) as temp:
            streaming_body = response['Body']
            for chunk in iter(lambda: streaming_body.read(8192), b''):
                temp.write(chunk)
            return storage_object._replace(local_file=temp.name, etag=response['ETag'])

    def delete(self, storage_object):
        self.s3.Object(BUCKET, file_key(storage_object)).delete()
