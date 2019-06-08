#rm -rf /dev/hugepages
mkdir -p /root/hugepages
mkdir /root/hugepages/cache
mkdir /root/hugepages/data
chmod -R 1777 /root/hugepages
mount -t tmpfs -o rw,size=6G tmpfs /root/hugepages
#chmod -R a+rw /root/hugepages


# 30000 * 2048 * 1024 = 62GB
#sh -c 'for i in /sys/devices/system/node/node*/hugepages/hugepages-2048kB/nr_hugepages; do echo 30000 > $i; done' 
