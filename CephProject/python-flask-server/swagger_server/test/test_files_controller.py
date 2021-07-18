# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from swagger_server.test import BaseTestCase


class TestFilesController(BaseTestCase):
    """FilesController integration test stubs"""

    def test_delete_file(self):
        """Test case for delete_file

        Deletes a file
        """
        response = self.client.open(
            '/v2/files/{name}'.format(name='name_example'),
            method='DELETE')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_files_get(self):
        """Test case for files_get

        Returns a list of files.
        """
        response = self.client.open(
            '/v2/files',
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_file_by_name(self):
        """Test case for get_file_by_name

        Download a file by name
        """
        response = self.client.open(
            '/v2/files/{name}'.format(name='name_example'),
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_upload_file(self):
        """Test case for upload_file

        Upload a new file
        """
        data = dict(upfile=(BytesIO(b'some file data'), 'file.txt'),
                    name='name_example')
        response = self.client.open(
            '/v2/files',
            method='POST',
            data=data,
            content_type='multipart/form-data')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
