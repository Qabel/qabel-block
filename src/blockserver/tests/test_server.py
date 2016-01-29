from tornado.options import options
from tornado.testing import AsyncHTTPTestCase

from blockserver import server
from blockserver.backends import dummy

options.debug = True
app = server.make_app()

PATH = '/files/test/bar'


class ServerTestCase(AsyncHTTPTestCase):

    use_dummy = True

    def setUp(self):
        super(ServerTestCase, self).setUp()
        dummy.files = {}
        options.dummy = self.use_dummy

    def tearDown(self):
        dummy.files = {}
        options.dummy = False

    def get_app(self):
        return app

    def headers(self):
        return {'Authorization': 'Token MAGICFARYDUST'}

    def test_not_found(self):
        response = self.fetch(PATH)
        self.assertEqual(response.code, 404)

    def test_no_body(self):
        response = self.fetch(PATH, method='POST', body=b'', headers=self.headers())
        self.assertEqual(response.code, 204)

    def test_no_auth(self):
        response = self.fetch(PATH, method='POST', body=b'')
        self.assertEqual(response.code, 403)

    def test_normal_cycle(self):
        response = self.fetch(PATH, method='POST', body=b'Dummy', headers=self.headers())
        self.assertEqual(response.code, 204)
        response = self.fetch(PATH, method='GET')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b'Dummy')
        response = self.fetch(PATH, method='DELETE', headers=self.headers())
        self.assertEqual(response.code, 204)
        response = self.fetch(PATH, method='GET')
        self.assertEqual(response.code, 404)


class S3ServerTestCase(ServerTestCase):
    use_dummy = False

