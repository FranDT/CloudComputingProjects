import connexion
import six
import backend as server
from swagger_server import util


def delete_file(name):  # noqa: E501
    """Deletes a file

     # noqa: E501

    :param name: file name to delete
    :type name: str

    :rtype: None
    """
    request = {'type': 'delete', 'name': name}
    return server.processrequest(request)


def files_get():  # noqa: E501
    """Returns a list of files.

     # noqa: E501


    :rtype: str
    """
    request = {'type': 'ls'}
    return server.processrequest(request)


def get_file_by_name(name):  # noqa: E501
    """Download a file by name

    Returns a single file # noqa: E501

    :param name: name of the file to download
    :type name: str

    :rtype: file
    """
    request = {'type': 'download', 'name': name}
    return server.processrequest(request)

def upload_file(upfile, name):  # noqa: E501
    """Upload a new file

     # noqa: E501

    :param upfile: File data
    :type upfile: werkzeug.datastructures.FileStorage
    :param name: File name
    :type name: str

    :rtype: None
    """
    request = {'type': 'upload', 'file': {'name': name, 'content': upfile}}
    return server.processrequest(request)
