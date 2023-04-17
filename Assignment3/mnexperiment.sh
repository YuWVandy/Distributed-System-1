h1 ./zookeeper/bin/zkServer.sh start-foreground > zk1.txt 2>&1 &

h2 python3 ./DiscoveryAppln.py -n disc1 > disc1.txt 2>&1 &
