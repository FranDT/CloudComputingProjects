import rados


def processRequest(request):
    cluster = rados.Rados(conffile='ceph.conf')
    cluster.connect()
    pool = "drive"
    if not cluster.pool_exists(pool):
        cluster.create_pool(pool)
    response = ""
    if (request['type'] == "ls"):
        response = ls(cluster, pool)
    elif (request['type'] == "upload"):
        response = upload(cluster, pool, request['file'])
    elif (request['type'] == "download"):
        response = download(cluster, pool, request['name'])
    elif (request['type'] == "delete"):
        response = delete(cluster, pool, request['name'])
    elif (request['type'] == "stats"):
        response = stats(cluster, pool)
    else:
        response = "Invalid Operation"
    cluster.shutdown()
    return response

    

def ls(cluster, pool):
    response = ""
    try:
        ioctx = cluster.open_ioctx(pool)
        for obj in ioctx.list_objects():
            response += (str(obj.key) + "\n")
    except Exception as e:
        print("Cannot get list of files because of: {}".format(e))
        response = "Failed to get list due to Exception"
    finally:
        ioctx.close()
        return response


def delete(cluster, pool, name):
    response = ""
    try:
        ioctx = cluster.open_ioctx(pool)
        res = ioctx.remove_object(name)
        if res:
            response = "File Deleted"
        else:
            response = "Could not Delete File"
    except Exception as e:
        print("Cannot delete because of: {}".format(e))
        response = "File Not Deleted due to Exception"
    finally:
        ioctx.close()
        return response


def upload(cluster, pool, file):
    response = ""
    try:
        ioctx = cluster.open_ioctx(pool)
        res = ioctx.write_full(file['name'], file['content'])
        if res == 0:
            response = "File Uploaded"
        else:
            response = "File Not Uploaded"
    except Exception as e:
        print("Cannot upload because of: {}".format(e))
        response = "File Not Uploaded due to Exception"
    finally:
        ioctx.close()
        return response
        
def download(cluster, pool, name):
    response = ""
    try:
        ioctx = cluster.open_ioctx(pool)
        response = ioctx.read(name)
    except Exception as e:
        print("Cannot download because of: {}".format(e))
        response = "File Not Downloaded due to Exception"
    finally:
        ioctx.close()
        return response

def stats(cluster, pool):
    response = ""
    try:
        ioctx = cluster.open_ioctx(pool)
        usage = ioctx.get_stats()
        for k, v in usage.items():
            response += str(k) + ": " + str(v) + "\n"
    except Exception as e:
        print("Cannot get statistics because of: {}".format(e))
        response = "Cannot get statistics due to Exception"
    finally:
        ioctx.close()
        return response

request = {'type': 'ls'}
print(processRequest(request))
content = 'ahmed'
myBytes = bytes(content, 'utf-8')
request = {'type': 'upload', 'file': {'name': 'ahmed', 'content': myBytes}}
print(processRequest(request))
request = {'type': 'ls'}
print(processRequest(request))
request = {'type': 'download', 'name': 'ahmed'}
print(processRequest(request))
request = {'type': 'ls'}
print(processRequest(request))
request = {'type': 'delete', 'name': 'ahmed'}
print(processRequest(request))
request = {'type': 'ls'}
print(processRequest(request))
request = {'type': 'stats'}
print(processRequest(request))
    
