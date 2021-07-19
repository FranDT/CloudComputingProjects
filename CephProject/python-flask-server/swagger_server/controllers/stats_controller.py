import connexion
import six
import backend as server

from swagger_server import util


def stats_get():  # noqa: E501
    """Returns a list of files.

     # noqa: E501


    :rtype: str
    """
    request = {'type': 'stats'}
    return server.processrequest(request)
