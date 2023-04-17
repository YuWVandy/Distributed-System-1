###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the subscriber side of things.
#
# Remember that the subscriber middleware does not do anything on its own.
# It must be invoked by the application level logic. This middleware object maintains
# the ZMQ sockets and knows how to talk to Discovery service, etc.
#
# Here is what this middleware should do
# (1) it must maintain the ZMQ sockets, one in the REQ role to talk to the Discovery service
# and one in the SUB role to receive topic data
# (2) It must, on behalf of the application logic, register the subscriber application with the
# discovery service. To that end, it must use the protobuf-generated serialization code to
# send the appropriate message with the contents to the discovery service.
# (3) On behalf of the subscriber appln, it must use the ZMQ setsocket method to subscribe to all the
# user-supplied topics of interest. 
# (4) Since it is a receiver, the middleware object will maintain a poller and even loop waiting for some
# subscription to show up (or response from Discovery service).
# (5) On receipt of a subscription, determine which topic it is and let the application level
# handle the incoming data. To that end, you may need to make an upcall to the application-level
# object.
#
import csv
import time

import zmq  # ZMQ sockets

# import serialization logic
from kazoo.client import KazooClient

from CS6381_MW import discovery_pb2
#from CS6381_MW import topic_pb2  # you will need this eventually

# import any other packages you need.

##################################
#       Subscriber Middleware class
##################################
class SubscriberMW ():

    def __init__(self, logger):
        self.logger = logger  # internal logger for print statements
        self.req = None  # will be a ZMQ REQ socket to talk to Discovery service
        self.sub = None  # will be a ZMQ sub socket for receiving
        self.push = None # will be a ZMQ push sockect for push data to influxdb
        self.poller = None  # used to wait on incoming replies
        self.addr = None  # our advertised IP address
        self.port = None  # port num where we are going to publish our topics
        self.upcall_obj = None  # handle to appln obj to handle appln-specific data
        self.handle_events = True  # in general we keep going thru the event loop

        self.zk = None

    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info("SubscriberMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.port = args.port
            self.addr = args.addr

            # Next get the ZMQ context
            self.logger.debug("SubscriberMW::configure - obtain ZMQ context")
            context = zmq.Context()  # returns a singleton object

            # get the ZMQ poller object
            self.logger.debug("SubscriberMW::configure - obtain the poller")
            self.poller = zmq.Poller()

            # Now acquire the REQ and PUB sockets
            # REQ is needed because we are the client of the Discovery service
            # PUB is needed because we publish topic data
            self.logger.debug("SubscriberMW::configure - obtain REQ and SUB sockets")
            self.req = context.socket(zmq.REQ)
            self.sub = context.socket(zmq.SUB)

            # Since are using the event loop approach, register the REQ socket for incoming events
            # Note that nothing ever will be received on the PUB socket and so it does not make
            # any sense to register it with the poller for an incoming message.
            self.logger.debug("SubscriberMW::configure - register the REQ socket for incoming replies")
            self.poller.register(self.req, zmq.POLLIN)

            # Now connect ourselves to the discovery service. Recall that the IP/port were
            # supplied in our argument parsing. Best practices of ZQM suggest that the
            # one who maintains the REQ socket should do the "connect"
            self.logger.debug("SubscriberMW::configure - connect to Discovery service")
            # For our assignments we will use TCP. The connect string is made up of
            # tcp:// followed by IP addr:port number.

            self.zk = KazooClient(hosts="localhost:2181")
            self.zk.start()
            election_path = "/election/discovery"

            children = self.zk.get_children(election_path)
            children = sorted(children)
            path = f"{election_path}/{children[0]}"

            data, _ = self.zk.get(path)

            ip, port = data.decode("utf-8").split(":")

            connect_str = "tcp://" + ip + ":" + str(port)
            self.logger.info("connect with {}".format(connect_str))
            self.req.connect(connect_str)

        except Exception as e:
            raise e

    # def subConnect(self, addr, port):
    #     if self.upcall_obj.dissemination == 'Direct':
    #         self.logger.info("Choose Direct strategy")
    #         bind_string = "tcp://:" + addr + str(self.port)
    #         self.sub.connect("tcp://localhost:5577")
    #     else:
    #         # Connect to the broker
    #         self.logger.info("Choose Via-Broker strategy")
    #         self.sub.connect("tcp://localhost:5599")

    def event_loop(self, timeout=None):

        try:
            self.logger.info("SubscriberMW::event_loop - run the event loop")


            while self.handle_events:  # it starts with a True value

                events = dict(self.poller.poll(timeout=timeout))


                if not events:

                    timeout = self.upcall_obj.invoke_operation()

                elif self.req in events:  # this is the only socket on which we should be receiving replies

                    # handle the incoming reply from remote entity and return the result
                    timeout = self.handle_reply()

                elif self.sub in events:
                    timeout = self.handle_pub_reply()

                else:
                    raise Exception("Unknown event after poll")

            self.logger.info("SubscriberMW::event_loop - out of the event loop")
        except Exception as e:
            raise e


    def handle_reply(self):

        try:
            self.logger.info("SubscriberMW::handle_reply")

            # let us first receive all the bytes
            bytesRcvd = self.req.recv()

            # now use protobuf to deserialize the bytes
            # The way to do this is to first allocate the space for the
            # message we expect, here DiscoveryResp and then parse
            # the incoming bytes and populate this structure (via protobuf code)
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)

            self.logger.info("SubscriberMW::sub received data {}".format(disc_resp))

            if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
                # let the appln level object decide what to do
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            # elif(disc_resp.msg_type == discovery_pb2.TYPE_ISREADY):
            #     timeout = 0

            elif (disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
                # this is a response to is ready request
                self.logger.info("SubscriberMW::sub received data {}".format(disc_resp.lookup_resp.info))
                timeout = self.upcall_obj.lookup_response(disc_resp.lookup_resp.info)

            else:  # anything else is unrecognizable by this object
                # raise an exception here
                raise ValueError("Unrecognized response message")

            return timeout

        except Exception as e:
            raise e


    def handle_pub_reply(self):

        try:
            self.logger.info("SubscriberMW::handle_pub_reply")

            # let us first receive all the bytes
            bytesRcvd = self.sub.recv()
            self.logger.info("SubscriberMW::sub received data {}".format(bytesRcvd))

            str = bytesRcvd.decode()
            strs = str.split(":")
            tsampt = time.time()
            res = [{'Topic':strs[0],'Content':strs[1],'Timestamp':tsampt}]

            if (len(bytesRcvd) > 0):
                self.recordTimeStamp(res)
                timeout = self.upcall_obj.topic_response(bytesRcvd)

            else:  # anything else is unrecognizable by this object
                # raise an exception here
                raise ValueError("Unrecognized response message")

            return timeout

        except Exception as e:
            raise e

    def register(self, name, topiclist):
        ''' register the appln with the discovery service '''

        try:
            self.logger.info("SubscriberMW::register")

            # Build the Registrant Info message first.
            self.logger.debug("SubscriberMW::register - populate the Registrant Info")
            reg_info = discovery_pb2.RegistrantInfo()  # allocate
            reg_info.id = name  # our id
            reg_info.addr = self.addr  # our advertised IP addr where we are publishing
            reg_info.port = self.port  # port on which we are publishing
            self.logger.debug("SubscriberMW::register - done populating the Registrant Info")

            # Next build a RegisterReq message
            self.logger.debug("SubscriberMW::register - populate the nested register req")
            register_req = discovery_pb2.RegisterReq()  # allocate
            register_req.role = discovery_pb2.ROLE_SUBSCRIBER # we are a subscriber
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            register_req.info.CopyFrom(reg_info)  # copy contents of inner structure
            register_req.topiclist[
            :] = topiclist  # this is how repeated entries are added (or use append() or extend ()
            self.logger.debug("SubscriberMW::register - done populating nested RegisterReq")

            # Finally, build the outer layer DiscoveryReq Message
            self.logger.debug("SubscriberMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.register_req.CopyFrom(register_req)
            self.logger.debug("SubscriberMW::register - done building the outer message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug("Stringified serialized buf = {}".format(buf2send))

            # now send this to our discovery service
            self.logger.debug("SubscriberMW::register - send stringified buffer to Discovery service")
            self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # now go to our event loop to receive a response to this request
            self.logger.info("SubscriberMW::register - sent register message and now now wait for reply")

        except Exception as e:
            raise e


    def lookup_pub(self, topiclist):
        ''' register the appln with the discovery service '''

        try:
            self.logger.info("SubscriberMW::lookup_pub")


            lookup_req = discovery_pb2.LookupPubByTopicReq()  # allocate
            lookup_req.topiclist.extend(topiclist)


            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC

            disc_req.lookup_req.CopyFrom(lookup_req)

            # self.logger.debug("SubscriberMW::lookup_pub - done building the outer message")
            self.logger.info("Stringified serialized buf = {}".format(disc_req))
            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug("Stringified serialized buf = {}".format(buf2send))

            # now send this to our discovery service
            self.logger.debug("SubscriberMW::lookup_pub - send stringified buffer to Discovery service")
            self.req.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # now go to our event loop to receive a response to this request
            self.logger.info("SubscriberMW::lookup_pub - request sent and now wait for reply")

        except Exception as e:
            raise e


    def set_upcall_handle(self, upcall_obj):
        ''' set upcall handle '''
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        ''' disable event loop '''
        self.handle_events = False

    def connect2pub(self, IP, port):

        if self.upcall_obj.dissemination == 'Direct':
            self.logger.info("Choose Direct strategy")
            connect_str = "tcp://" + IP + ":" + str(port)
            self.sub.connect(connect_str)
            self.logger.info("SubscriberMW:connect2pub - connecting to {}".format(connect_str))
        else:
            # Connect to the broker
            self.logger.info("Choose Via-Broker strategy")
            broker_path = "/election/broker"
            children = self.zk.get_children(broker_path)
            children = sorted(children)
            path = f"{broker_path}/{children[0]}"

            data, _ = self.zk.get(path)

            ip, port = data.decode("utf-8").split(":")

            connect_str = "tcp://" + ip + ":" + str(port)
            self.sub.connect(connect_str)
            self.logger.info("SubscriberMW:connect2pub - connecting to tcp://localhost:5589")

        self.poller.register(self.sub, zmq.POLLIN)

    def subscribe(self, topiclist):
        for topic in topiclist:
            self.logger.debug("SubscriberMW: subscriber-topic = {}". format(topic))
            self.sub.setsockopt(zmq.SUBSCRIBE, bytes(topic, "utf-8"))


    def create_database(self):
        fieldnames = ['Topic', 'Content', 'Timestamp']
        with open(r'record_timeStamp.csv', 'w', newline='') as file:
            file_write = csv.DictWriter(file, fieldnames=fieldnames)
            file_write.writeheader()


    def recordTimeStamp(self, res):
        fieldnames = ['Topic', 'Content', 'Timestamp']
        with open(r'record_timeStamp.csv', 'a', newline='') as file:
            file_write = csv.DictWriter(file, fieldnames=fieldnames)
            for data in res:
                file_write.writerow(data)

    def connect_to_zookeeper(self, zookeeper_servers):
        """Connect to the ZooKeeper cluster."""
        self.logger.info("Connecting to ZooKeeper servers: %s", zookeeper_servers)
        self.zk = KazooClient(hosts=zookeeper_servers)
        self.zk.start()

    def register_to_zookeeper(self, znode_path, ip, port):
        data = f"{ip}:{port}".encode()
        self.zk.create(znode_path, data, ephemeral=True, makepath=True)

    def get_discovery_leader_info(self, leader_znode_path):
        if self.zk.exists(leader_znode_path):
            data, _ = self.zk.get(leader_znode_path)
            ip, port = data.decode().split(':')
            return ip, int(port)
        else:
            return None
