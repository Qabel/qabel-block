import os
import tempfile

from tornado.options import options
from tornado.testing import AsyncHTTPTestCase

from blockserver import server

options.debug = True
options.dummy = True
options.noauth = True
app = server.make_app()


class ServerTestCase(AsyncHTTPTestCase):

    def setUp(self):
        super(ServerTestCase, self).setUp()
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            temp.write(b'Dummy\n')
            self.dummy = temp.name

    def tearDown(self):
        os.remove(self.dummy)

    def get_app(self):
        return app

    def test_not_found(self):
        response = self.fetch('/files/foo/bar')
        self.assertEqual(response.code, 404)
