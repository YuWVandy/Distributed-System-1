h1 python3 DiscoveryAppln.py --name='dis1' --num_pub=2 --num_sub=2 --port=5555 > discovery.out 2>&1&
h2 python3 PublisherAppln.py --name='pub1' --num_topics=2 --port=5577 > pub1.out 2>&1&
h2 python3 PublisherAppln.py --name='pub2' --num_topics=2 --port=5578 > pub2.out 2>&1&

h3 python3 SubscriberAppln.py --name='sub1' --num_topics=2 --port=5579 > sub1.out 2>&1&
h3 python3 SubscriberAppln.py --name='sub2' --num_topics=2 --port=5580 > sub2.out 2>&1&
