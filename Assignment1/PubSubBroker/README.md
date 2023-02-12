This folder is for setting up the pub-sub directly without broker
Put all this files in one folder and run the following command:

```linux
python3 DiscoveryAppln.py --name='dis1' --num_pub=1 --num_sub=1 --num_broker=1 --port=5555 > discovery.out 2>&1&
python3 PublisherAppln.py --name='pub1' --num_topics=2 --port=5577 > pub1.out 2>&1&
python3 SubscriberAppln.py --name='sub1' --num_topics=2 --port=5579 > sub1.out 2>&1&
python3 BrokerAppln.py --name='broker1'
```
