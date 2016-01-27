import logging
import tempfile

import boto3
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler, stream_request_body
from tornadostreamform.multipart_streamer import MultiPartStreamer
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

REGION = 'eu-west-1'
BUCKET = 'qabel'

client = boto3.client('s3')
transfer = boto3.s3.transfer.S3Transfer(client)
s3 = boto3.resource('s3')


def file_key(prefix, file_path):
    return '{}/{}'.format(prefix, file_path)


def store_file(prefix, file_path, file):
    transfer.upload_file(file, BUCKET, file_key(prefix, file_path))


def retrieve_file(prefix, file_path):
    try:
        with tempfile.NamedTemporaryFile('wb', delete=False) as temp:
            transfer.download_file(BUCKET, file_key(prefix, file_path), temp.name)
        return temp.name
    except ClientError:
        return None


async def check_auth(auth, prefix, file_path, action):
    if action == 'POST':
        return auth == "Token MAGICFARYDUST"
    else:
        return True


@stream_request_body
class FileHandler(RequestHandler):
    auth = None
    streamer = None

    async def prepare(self):
        self.auth = None
        self.streamer = None
        try:
            prefix = self.path_kwargs['prefix']
            file_path = self.path_kwargs['file_path']
        except KeyError:
            self.send_error(403)
            return
        auth = self.request.headers.get('Authorization', None)
        if not await check_auth(auth, prefix, file_path, self.request.method):
            self.auth = False
            self.send_error()
        else:
            self.auth = True
        if self.request.method == 'POST':
            self.streamer = MultiPartStreamer(0)

    async def data_received(self, chunk):
        if not self.auth:
            self.send_error()
        self.streamer.data_received(chunk)

    async def get(self, prefix, file_path):
        body = retrieve_file(prefix, file_path)
        with open(body, 'rb') as f_in:
            self.write(f_in.read())
        self.finish()

    async def post(self, prefix, file_path):
        if self.streamer:
            self.streamer.data_complete()
            if not len(self.streamer.parts) == 1:
                self.send_error()
            store_file(prefix, file_path, self.streamer.parts[0].f_out.name)
            self.streamer.release_parts()

    async def head(self, prefix, file_path):
        self.set_status(304)
        self.finish()

if __name__ == "__main__":
    application = Application([
        (r'^/files/(?P<prefix>[\d\w-]+)/(?P<file_path>[\d\w-]+)', FileHandler),
    ], debug=False)
    application.listen(8888)
    IOLoop.instance().start()
