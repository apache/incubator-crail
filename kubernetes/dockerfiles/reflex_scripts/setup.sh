# this setup script should be put under reflex/

# update ReFlex ip
if ifconfig eth1 ; then
        ifconfig eth1 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}' >> /dev/reflex_ip
        ifconfig eth1 | grep 'inet addr:' | cut -d: -f2 | sed -i "47s@.*@$(awk '{print "crail.storage.reflex.bindip             "$1""}')@g" ../crail/conf/crail-site.conf
        ifconfig eth1 | grep 'inet addr:' | cut -d: -f2 | sed -i "15s@.*@$(awk '{print "host_addr=\""$1"/16\""}')@g" ix.conf
        ifconfig eth1 down
else
        cat /dev/reflex_ip | sed -i "47s@.*@$(awk '{print "crail.storage.reflex.bindip             "$1""}')@g" ../crail/conf/crail-site.conf
        cat /dev/reflex_ip | sed -i "15s@.*@$(awk '{print "host_addr=\""$1"/16\""}')@g" ix.conf
fi

modprobe uio
insmod deps/dpdk/build/kmod/igb_uio.ko
deps/dpdk/usertools/dpdk-devbind.py --bind=igb_uio 00:04.0

deps/spdk/scripts/setup.sh

sh -c 'for i in /sys/devices/system/node/node*/hugepages/hugepages-2048kB/nr_hugepages; do echo 4096 > $i; done'
