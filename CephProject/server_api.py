from flask import Flask
import backend as server
from flask import request


app = Flask(__name__)


@app.route('/v1/files', methods=['GET'])
def retrieve_list_of_files():
    req = {'type': 'ls'}
    return server.processRequest(req)

@app.route('/v1/files/<path:file_name>' , methods=['DELETE'])
def delete_file(file_name):
        req = {'type': 'delete', 'name': file_name}
        return server.processRequest(req)

@app.route('/v1/files/<path:file_name>' , methods=['GET'])
def download_file(file_name):
        req = {'type': 'download', 'name': file_name}
        return server.processRequest(req)

@app.route('/v1/files' , methods=['POST'])
def upload_file():
    file = request.files['file']
    file_name = file.filename
    file_content = file.read()
    req = {'type': 'upload', 'file': {'name': file_name, 'content': file_content}}
    return server.processRequest(req)

@app.route('/v1/statistics' , methods=['GET'])
def show_statistics():
    req = {'type': 'stats'}
    return server.processRequest(req)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

