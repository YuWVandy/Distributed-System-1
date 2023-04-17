sudo ./zookeeper/bin/zkServer.sh start-foreground

python3 ./DiscoveryAppln.py
python3 ./DiscoveryAppln.py -n disc2 -a 10.0.0.2 -p=5559

python3 ./BrokerAppln.py
python3 ./BrokerAppln.py -n broker2 -d=5562


python3 ./PublisherAppln.py

python3 ./SubscriberAppln.py
