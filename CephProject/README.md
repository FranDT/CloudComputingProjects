# How To Run

In the following section, you will have a simple guide on how to run the Ceph implementation.
Major details can be found in the documentation.

## Running the backend
For each vm used as OpenStack compute node, open a shell inside the container for ceph-mon.

For machine 172.16.3.200:
```
lxc exec juju-f7b247-1-lxd-0 /bin/bash
```
For machine 172.16.3.198:
```
lxc exec juju-f7b247-2-lxd-1 /bin/bash
```
For machine 172.16.3.211:
```
lxc exec juju-f7b247-3-lxd-1 /bin/bash
```
For each one, go to the directory "home/ubuntu" and run the server:
```
python3 server_api.py
```
## Running the load balancer
Open machine 172.16.3.207 and go to directory cephLB. from there, run the docker container named "ceph-lb":
```
docker run -p 8080:8080 -it ceph-lb
```
## Running the fronted
Simply run CephClient:
```
python3 CephClient.py
```
