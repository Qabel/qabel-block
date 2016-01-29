import os
import tempfile

from tornado.options import options
from tornado.testing import AsyncHTTPTestCase

from blockserver import server
from blockserver.backends import dummy

options.debug = True
app = server.make_app()

PATH = '/files/foo/bar'


class ServerTestCase(AsyncHTTPTestCase):

    def setUp(self):
        super(ServerTestCase, self).setUp()
        dummy.files = {}
        options.dummy = True

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

    def test_store_retrieve(self):
        response = self.fetch(PATH, method='POST', body=b'Dummy', headers=self.headers())
        self.assertEqual(response.code, 200)
        response = self.fetch(PATH, method='GET')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, b'Dummy')


