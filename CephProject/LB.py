#!flask/bin/python
from flask import Flask, request, redirect
import time

app = Flask(__name__)

IPs = 'monitors.txt'

@app.route('/v1/files', methods=['GET', 'POST'])
def fwdUploadLS():
    myIP = chooseIP()  
    return redirect('http://{}:8080/{}'.format(myIP, request.path), code=307)

@app.route('/v1/stats', methods=['GET'])
def stats():
    myIP = chooseIP()  
    return redirect('http://{}:8080/{}'.format(myIP, request.path), code=307)

@app.route('/v1/files/<name>', methods=['GET', 'DELETE'])
def fwdDeleteDownload(name):
    myIP = chooseIP()  
    return redirect('http://{}:8080/{}'.format(myIP, request.path), code=307)

def chooseIP():
    lines = []
    with open(IPs) as f:
        lines = f.readlines()
    now = time.time()
    return lines[hash(now)%len(lines)]
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
