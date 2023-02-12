h1 python3 DiscoveryAppln.py --name='dis1' --num_pub=1 --num_sub=1 > discovery.out
h2 python3 PublisherAppln.py --name='pub1' --d='10.0.0.1:5555' --a='10.0.0.2' --num_topics=2 --port=5577 > pub1.out
# h3 python3 PublisherAppln.py --name='pub2' --d='10.0.0.1:5555' --a='10.0.0.3' --num_topics=1 --port=5578 > pub2.out 2>&1&
# h4 python3 PublisherAppln.py --name='pub3' --d='10.0.0.1:5555' --a='10.0.0.4' --num_topics=3 --port=5579 > pub3.out 2>&1&
h5 python3 SubscriberAppln.py --name='sub1' --d='10.0.0.1:5555' --a='10.0.0.3' --num_topics=1 > sub1.out
# h6 python3 SubscriberAppln.py --name='sub2' --d='10.0.0.1:5555' --a='10.0.0.6' --num_topics=1 > sub2.out 2>&1&
# h7 python3 SubscriberAppln.py --name='sub3' --d='10.0.0.1:5555' --a='10.0.0.7' --num_topics=1 > sub3.out 2>&1&
# h8 python3 SubscriberAppln.py --name='sub4' --d='10.0.0.1:5555' --a='10.0.0.8' --num_topics=1 > sub4.out 2>&1&
