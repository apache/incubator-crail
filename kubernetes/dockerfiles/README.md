# Pocket Container Images

Code and instructions for generating Pocket container images. 

## Pocket Node Images on Dockerhub
* [namenode](https://hub.docker.com/r/yawenw/pocket-namenode/)

* [datanode-dram](https://hub.docker.com/r/yawenw/pocket-datanode-dram/)

* [reflex](https://hub.docker.com/r/yawenw/pocket-reflex/)


## Build Docker Image for Pocket 
``` 
# AWS AMI for Kubernetes: k8s-1.8-debian-jessie-amd64-hvm-ebs-2018-02-08 (ami-f5d2548d)

# Build namenode
cp Dockerfile-namenode Dockerfile
sudo docker build -t pocket-namenode .

# Build DRAM datanode
cp Dockerfile-datanode-dram Dockerfile
sudo docker build -t pocket-datanode-dram .

# Build ReFlex datanode 
# pull and complie ReFlex following instructions at https://github.com/stanford-mast/reflex
cp reflex_scripts/* reflex/
cp Dockerfile-reflex Dockerfile
sudo docker build -t pocket-reflex .
```

## Run Pocket in Container
``` 
# Run namenode
sudo docker run -it --net=host --privileged pocket-namenode

# Run DRAM datanode
sudo docker run -it --net=host --privileged -v /dev:/dev pocket-datanode-dram

# Run ReFlex datanode 
sudo docker run -it --net=host --privileged -v /dev:/dev -v /lib/modules/`uname -r`:/lib/modules/`uname -r` pocket-reflex
```

