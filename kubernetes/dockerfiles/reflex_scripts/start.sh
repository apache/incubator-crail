# this script should be put under reflex/

# Run setup script for ReFlex and datanode 
./setup.sh

# Start ReFlex
./dp/ix &> /dev/null &

# Start datanode
cd ../crail
taskset -c 1 ./bin/crail datanode -t com.ibm.crail.storage.reflex.ReFlexStorageTier


