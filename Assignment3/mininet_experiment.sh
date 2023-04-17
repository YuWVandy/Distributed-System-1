h1 /opt/zookeeper/bin/zkServer.sh start-foreground > tests/logs/zk1.txt 2>&1 &
h2 python3 ./DiscoveryAppln.py -n disc3 -a 10.0.0.2 -p 5555 > tests/logs/disc3.txt 2>&1 &
h2 python3 ./DiscoveryAppln.py -n disc3 -a 10.0.0.2 -p 5555 > tests/logs/disc3.txt 2>&1 &
h2 python3 ./DiscoveryAppln.py -n disc12 -a 10.0.0.2 -p 5556 > tests/logs/disc12.txt 2>&1 &
h4 python3 ./DiscoveryAppln.py -n disc11 -a 10.0.0.4 -p 5555 > tests/logs/disc11.txt 2>&1 &
h4 python3 ./DiscoveryAppln.py -n disc17 -a 10.0.0.4 -p 5556 > tests/logs/disc17.txt 2>&1 &
h5 python3 ./DiscoveryAppln.py -n disc4 -a 10.0.0.5 -p 5555 > tests/logs/disc4.txt 2>&1 &
h5 python3 ./DiscoveryAppln.py -n disc20 -a 10.0.0.5 -p 5556 > tests/logs/disc20.txt 2>&1 &
h6 python3 ./DiscoveryAppln.py -n disc13 -a 10.0.0.6 -p 5555 > tests/logs/disc13.txt 2>&1 &
h6 python3 ./DiscoveryAppln.py -n disc18 -a 10.0.0.6 -p 5556 > tests/logs/disc18.txt 2>&1 &
h8 python3 ./DiscoveryAppln.py -n disc16 -a 10.0.0.8 -p 5555 > tests/logs/disc16.txt 2>&1 &
h8 python3 ./DiscoveryAppln.py -n disc19 -a 10.0.0.8 -p 5556 > tests/logs/disc19.txt 2>&1 &
h9 python3 ./DiscoveryAppln.py -n disc6 -a 10.0.0.9 -p 5555 > tests/logs/disc6.txt 2>&1 &
h10 python3 ./DiscoveryAppln.py -n disc8 -a 10.0.0.10 -p 5555 > tests/logs/disc8.txt 2>&1 &
h11 python3 ./DiscoveryAppln.py -n disc7 -a 10.0.0.11 -p 5555 > tests/logs/disc7.txt 2>&1 &
h11 python3 ./DiscoveryAppln.py -n disc14 -a 10.0.0.11 -p 5556 > tests/logs/disc14.txt 2>&1 &
h13 python3 ./DiscoveryAppln.py -n disc1 -a 10.0.0.13 -p 5555 > tests/logs/disc1.txt 2>&1 &
h13 python3 ./DiscoveryAppln.py -n disc15 -a 10.0.0.13 -p 5556 > tests/logs/disc15.txt 2>&1 &
h15 python3 ./DiscoveryAppln.py -n disc5 -a 10.0.0.15 -p 5555 > tests/logs/disc5.txt 2>&1 &
h17 python3 ./DiscoveryAppln.py -n disc2 -a 10.0.0.17 -p 5555 > tests/logs/disc2.txt 2>&1 &
h17 python3 ./DiscoveryAppln.py -n disc9 -a 10.0.0.17 -p 5556 > tests/logs/disc9.txt 2>&1 &
h17 python3 ./DiscoveryAppln.py -n disc10 -a 10.0.0.17 -p 5557 > tests/logs/disc10.txt 2>&1 &
h1 python3 ./PublisherAppln.py -n pub1 -a 10.0.0.1 -p 7777 -T 7 > tests/logs/pub1.txt 2>&1 &
h1 python3 ./PublisherAppln.py -n pub5 -a 10.0.0.1 -p 7778 -T 6 > tests/logs/pub5.txt 2>&1 &
h6 python3 ./PublisherAppln.py -n pub4 -a 10.0.0.6 -p 7777 -T 5 > tests/logs/pub4.txt 2>&1 &
h8 python3 ./PublisherAppln.py -n pub3 -a 10.0.0.8 -p 7777 -T 5 > tests/logs/pub3.txt 2>&1 &
h19 python3 ./PublisherAppln.py -n pub2 -a 10.0.0.19 -p 7777 -T 8 > tests/logs/pub2.txt 2>&1 &
h2 python3 ./SubscriberAppln.py -n sub1 -T 5 -R 20-5-5 > tests/logs/sub1.txt 2>&1 &
h3 python3 ./SubscriberAppln.py -n sub5 -T 5 -R 20-5-5 > tests/logs/sub5.txt 2>&1 &
h7 python3 ./SubscriberAppln.py -n sub3 -T 5 -R 20-5-5 > tests/logs/sub3.txt 2>&1 &
h16 python3 ./SubscriberAppln.py -n sub4 -T 8 -R 20-5-5 > tests/logs/sub4.txt 2>&1 &
h17 python3 ./SubscriberAppln.py -n sub2 -T 5 -R 20-5-5 > tests/logs/sub2.txt 2>&1 &
