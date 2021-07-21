# How To Run

## Running the backend
for each vm used as OpenStack compute node, open the ceph-mon container shell.

for machine 172.16.3.200:
```
lxc exec juju-f7b247-1-lxd-0 /bin/bash
```
for machine 172.16.3.198:
```
lxc exec juju-f7b247-2-lxd-1 /bin/bash
```
for machine 172.16.3.211:
```
lxc exec juju-f7b247-3-lxd-1 /bin/bash
```
for each one, go to the "/home/ubuntu" directory and run the server_api script:
```
python3 server_api.py
```
## Running the load balancer
open machine 172.16.3.207 and from there, run the docker container named "ceph-lb":
```
docker run -p 8080:8080 -it ceph-lb
```
## Running the client
Simply run CephClient:
```
python3 CephClient.py
```
