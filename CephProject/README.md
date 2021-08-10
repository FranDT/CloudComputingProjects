# Ceph-Based File Manager
The application we have developed is a distributed file storage system based on Ceph. The file storage exports a REST interface through which external users can perform the following operations:
* Retrieve the list of files currently stored in the system;
* Delete a file;
* Upload / Download a file;
* Shows current statistics on the status of the cluster.

In the following sections, you will have a simple guide on the project implementation.
Major details can be found in the documentation.

## System Architecture
The application is composed by two layers: a frontend layer and a backend layer, that interacts through a load balancer, like in the following picture:
<img width="723" alt="Screen Shot 2021-08-10 at 21 53 28" src="https://user-images.githubusercontent.com/41535744/128926048-837f49bf-b8d1-4c88-b1ef-99af17dc1f95.png">

## Implementation
To interact with Ceph, a module exploiting the librados python library has been written: this module is used in the REST endpoint on the server side and is therefore deployed on each Ceph-mon instance.
On another machine, a docker container is deployed for the load balancer. The load balancer is in charge of forwarding client requests to the servers: the load balancing is done by hashing the timestamp of the request. After that, in order for the load balancer and the server to communicate with each other, we need to activate port forwarding for each container hosting a Ceph-mon instance. Finally, a python client with a simple command line user interface has been implemented in order to ease the life of the user.

## How To Run
### Running the backend
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
### Running the load balancer
Open machine 172.16.3.207 and from there, run the docker container named "ceph-lb":
```
docker run -p 8080:8080 -it ceph-lb
```
### Running the client
Simply run CephClient:
```
python3 CephClient.py
```
