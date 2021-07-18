# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from swagger_server.test import BaseTestCase


class TestStatsController(BaseTestCase):
    """StatsController integration test stubs"""

    def test_stats_get(self):
        """Test case for stats_get

        Returns a list of files.
        """
        response = self.client.open(
            '/v2/stats',
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
