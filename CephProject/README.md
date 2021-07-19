# How To Run

## Running the backend
first go to each vm and open the shell inside the container.

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
go to the directory "home/ubuntu" and run the server:
```
python3 server_api.py
```
## Running the load balancer
open machine 172.16.3.207 and go to directory cephLB. from there run the docker container named "ceph-lb":
```
docker run -p 8080:8080 -it ceph-lb
```
## Running the fronted
Simply run CephClient:
```
python3 CephClient.py
```
