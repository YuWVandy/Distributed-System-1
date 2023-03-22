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
# (3) On behalf of the subscriber appln, it must use the ZMQ setsockopt method to subscribe to all the
# user-supplied topics of interest.
# (4) Since it is a receiver, the middleware object will maintain a poller and even loop waiting for some
# subscription to show up (or response from Discovery service).
# (5) On receipt of a subscription, determine which topic it is and let the application level
# handle the incoming data. To that end, you may need to make an upcall to the application-level
# object.
#


# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging  # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets

# import serialization logic
from CS6381_MW import discovery_pb2
import csv
# from CS6381_MW import topic_pb2  # you will need this eventually

# import any other packages you need.

##################################
#       Publisher Middleware class
##################################


class SubscriberMW ():

    ########################################
    # constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger  # internal logger for print statements
        self.req = None  # will be a ZMQ REQ socket to talk to Discovery service
        self.sub = None  # will be a ZMQ SUB socket for dissemination
        self.poller = None  # used to wait on incoming replies
        self.addr = None  # our advertised IP address
        self.port = None  # port num where we are going to publish our topics
        self.upcall_obj = None  # handle to appln obj to handle appln-specific data
        self.handle_events = True  # in general we keep going thru the event loop

        self.name = None
        self.interest_topics = None

    ########################################
    # configure/initialize
    ########################################
    def configure(self, args):
        ''' Initialize the object '''

        try:
            self.name = args.name

            # with open('./res/' + self.name + '_track_data.csv', 'w') as data:
            #     dw = csv.DictWriter(data, delimiter = ',', fieldnames = ['topic', 'content', 'time'])
            #     dw.writeheader()

            # data.close()


            # Here we initialize any internal variables
            self.logger.info("SubscriberMW::configure")

            # First retrieve our advertised IP addr and the subscriber port num
            self.port = args.port
            self.addr = args.addr

            self.interest_topics = args.topiclist

            # Next get the ZMQ context
            self.logger.debug("SubscriberMW::configure - obtain ZMQ context")
            context = zmq.Context()  # returns a singleton object

            # get the ZMQ poller object
            self.logger.debug("SubscriberMW::configure - obtain the poller")
            self.poller = zmq.Poller()

            # Now acquire the REQ and SUB sockets
            # REQ is needed because we are the client of the Discovery service
            # SUB is needed because we subscribe topic data
            self.logger.debug(
                "SubscriberMW::configure - obtain REQ and SUB sockets")
            self.req = context.socket(zmq.DEALER)
            self.req.identity = bytes(args.name, "utf-8")
            self.sub = context.socket(zmq.SUB)

            # Since are using the event loop approach, register the REQ socket for incoming events
            # Note that nothing ever will be received on the SUB socket and so it does not make
            # any sense to register it with the poller for an incoming message.
            self.logger.debug(
                "SubscriberMW::configure - register the REQ socket for incoming replies")
            self.poller.register(self.req, zmq.POLLIN)

            self.logger.debug(
                "SubscriberMW::configure - register the SUB socket for incoming replies")
            self.poller.register(self.sub, zmq.POLLIN)

            # Now connect ourselves to the discovery service. Recall that the IP/port were
            # supplied in our argument parsing. Best practices of ZQM suggest that the
            # one who maintains the REQ socket should do the "connect"
            self.logger.debug(
                "SubscriberMW::configure - connect to Discovery service")
            # For our assignments we will use TCP. The connect string is made up of
            # tcp:// followed by IP addr:port number.
            connect_str = "tcp://" + args.discovery
            self.req.connect(connect_str)

            # Since we are the subscriber, the best practice as suggested in ZMQ is for us to
            # "bind" the SUB socket
            self.logger.debug(
                "SubscriberMW::configure - bind to the pub socket")
            # note that we subscribe on any interface hence the * followed by port number.
            # We always use TCP as the transport mechanism (at least for these assignments)
            # Since port is an integer, we convert it to string to make it part of the URL
            bind_string = "tcp://*:" + str(self.port)
            self.sub.bind(bind_string)

            self.logger.info("SubscriberMW::configure completed")

        except Exception as e:
            raise e

    #################################################################
    # run the event loop where we expect to receive a reply to a sent request
    #################################################################
    def event_loop(self, timeout=None):

        try:
            self.logger.info("SubscriberMW::event_loop - run the event loop")

            # we are using a class variable called "handle_events" which is set to
            # True but can be set out of band to False in order to exit this forever
            # loop
            while self.handle_events:  # it starts with a True value
                # poll for events. We give it an infinite timeout.
                # The return value is a socket to event mask mapping
                events = dict(self.poller.poll(timeout=timeout))

                # Unlike the previous starter code, here we are never returning from
                # the event loop but handle everything in the same locus of control
                # Notice, also that after handling the event, we retrieve a new value
                # for timeout which is used in the next iteration of the poll

                # check if a timeout has occurred. We know this is the case when
                # the event mask is empty
                if not events:
                    # timeout has occurred so it is time for us to make appln-level
                    # method invocation. Make an upcall to the generic "invoke_operation"
                    # which takes action depending on what state the application
                    # object is in.
                    timeout = self.upcall_obj.invoke_operation()

                elif self.req in events:  # this is the only socket on which we should be receiving replies
                    # handle the incoming reply from remote entity and return the result
                    timeout = self.handle_reply()

                elif self.sub in events:
                    self.process_publisher_topic()
                else:
                    raise Exception("Unknown event after poll")

            self.logger.info(
                "SubscriberMW::event_loop - out of the event loop")
        except Exception as e:
            raise e

    def process_publisher_topic(self):
        receive = self.sub.recv().decode('utf-8').split(':')

        topic, content, send_time = receive[0], receive[1], float(receive[2])
        recei_time = time.time()


        self.logger.info('Topc:' + topic + '\tContent:' + content + '\tTime_interval:' + str(recei_time - send_time))

        with open('./res/' + self.name + '_track_data.csv', 'a') as data:
            data.write('\n')
            data.write(','.join([receive[0], receive[1], str(recei_time - send_time)]))
            data.close()


    #################################################################
    # handle an incoming reply
    #################################################################
    def handle_reply(self):

        try:
            self.logger.info("SubscriberMW::handle_reply")

            # let us first receive all the bytes
            bytesRcvd = self.req.recv_multipart ()
            bytesRcvd = bytesRcvd[1]

            # now use protobuf to deserialize the bytes
            # The way to do this is to first allocate the space for the
            # message we expect, here DiscoveryResp and then parse
            # the incoming bytes and populate this structure (via protobuf code)
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)

            # demultiplex the message based on the message type but let the application
            # object handle the contents as it is best positioned to do so. See how we make
            # the upcall on the application object by using the saved handle to the appln object.
            #
            # Note also that we expect the return value to be the desired timeout to use
            # in the next iteration of the poll.
            if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
                # let the appln level object decide what to do
                timeout = self.upcall_obj.register_response(
                    disc_resp.register_resp)
            elif (disc_resp.msg_type == discovery_pb2.TYPE_ISREADY):
                # this is a response to is ready request
                timeout = self.upcall_obj.isready_response(
                    disc_resp.isready_resp)

            # anything else is unrecognizable by this object
            elif (disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
                lookup_resp = disc_resp.lookup_resp

                start_time = lookup_resp.start_time
                end_time = time.time()
                diff_lookup_time = time.time() - start_time

                with open('./res/' + 'sub_look_up_time.csv', 'a') as data:
                    data.write(str(abs(diff_lookup_time)))
                    data.write('\n')
                    data.close()

                addrs, ports = [], []
                for info in lookup_resp.info:
                    addrs.append(info.addr)
                    ports.append(info.port)

                self.logger.info(
                    "SubscriberMW::try to set the topic filtering")
                for topic in self.interest_topics:
                    self.sub.setsockopt_string(zmq.SUBSCRIBE,  topic)

                self.logger.info(
                    "SubscriberMW::connect to publisher")

                for addr, port in zip(addrs, ports):
                    connect_str = "tcp://" + addr + ':' + str(port)

                    self.sub.connect(connect_str)

                self.upcall_obj.state = self.upcall_obj.State.RECEIVE

                timeout = None

            else:
                raise ValueError("Unrecognized response message")

            return timeout

        except Exception as e:
            raise e

    ########################################
    # register with the discovery service
    #
    # this method is invoked by application object passing the necessary
    # details but then as a middleware object it is our job to do the
    # serialization using the protobuf generated code
    #
    # No return value from this as it is handled in the invoke_operation
    # method of the application object.
    ########################################
    def register(self, name, topiclist):
        ''' register the appln with the discovery service '''

        try:
            self.logger.info("SubscriberMW::register")

            # as part of registration with the discovery service, we send
            # what role we are playing, the list of topics we are publishing,
            # and our whereabouts, e.g., name, IP and port

            # The following code shows serialization using the protobuf generated code.

            # Build the Registrant Info message first.
            self.logger.debug(
                "SubscriberMW::register - populate the Registrant Info")
            reg_info = discovery_pb2.RegistrantInfo()  # allocate
            reg_info.id = name  # our id
            reg_info.addr = self.addr  # our advertised IP addr where we are publishing
            reg_info.port = self.port  # port on which we are publishing
            reg_info.time = time.time()

            self.logger.info(reg_info.time)

            self.logger.debug(
                "SubscriberMW::register - done populating the Registrant Info")

            # Next build a RegisterReq message
            self.logger.debug(
                "SubscriberMW::register - populate the nested register req")
            register_req = discovery_pb2.RegisterReq()  # allocate
            self.logger.info(str(discovery_pb2.ROLE_SUBSCRIBER))
            register_req.role = discovery_pb2.ROLE_SUBSCRIBER  # we are a subscriber
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            # copy contents of inner structure
            register_req.info.CopyFrom(reg_info)
            # this is how repeated entries are added (or use append() or extend ()
            register_req.topiclist[:] = topiclist
            self.logger.debug(
                "SubscriberMW::register - done populating nested RegisterReq")

            # Finally, build the outer layer DiscoveryReq Message
            self.logger.debug(
                "SubscriberMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.register_req.CopyFrom(register_req)
            self.logger.debug(
                "SubscriberMW::register - done building the outer message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug(
                "Stringified serialized buf = {}".format(buf2send))

            # now send this to our discovery service
            self.logger.debug(
                "SubscriberMW::register - send stringified buffer to Discovery service")
            # we use the "send" method of ZMQ that sends the bytes
            self.req.send_multipart ([b"", buf2send])

            # now go to our event loop to receive a response to this request
            self.logger.info(
                "SubscriberMW::register - sent register message and now now wait for reply")

        except Exception as e:
            raise e

    ########################################
    # check if the discovery service gives us a green signal to proceed
    #
    # Here we send the isready message and do the serialization
    #
    # No return value from this as it is handled in the invoke_operation
    # method of the application object.
    ########################################
    def is_ready(self):
        ''' register the appln with the discovery service '''

        try:
            self.logger.info("SubscriberMW::is_ready")

            # we do a similar kind of serialization as we did in the register
            # message but much simpler as the message format is very simple.
            # Then send the request to the discovery service

            # The following code shows serialization using the protobuf generated code.

            # first build a IsReady message
            self.logger.debug(
                "SubscriberMW::is_ready - populate the nested IsReady msg")
            isready_req = discovery_pb2.IsReadyReq()  # allocate
            isready_req.start_idx = 200
            isready_req.pub_idlist[:] = []
            isready_req.sub_idlist[:] = []
            # actually, there is nothing inside that msg declaration.
            self.logger.debug(
                "SubscriberMW::is_ready - done populating nested IsReady msg")

            # Build the outer layer Discovery Message
            self.logger.debug(
                "SubscriberMW::is_ready - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.isready_req.CopyFrom(isready_req)
            self.logger.debug(
                "SubscriberMW::is_ready - done building the outer message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug(
                "Stringified serialized buf = {}".format(buf2send))

            # now send this to our discovery service
            self.logger.debug(
                "SubscriberMW::is_ready - send stringified buffer to Discovery service")
            # we use the "send" method of ZMQ that sends the bytes
            self.req.send_multipart([b"", buf2send])

            # now go to our event loop to receive a response to this request
            self.logger.info(
                "SubscriberMW::is_ready - request sent and now wait for reply")

        except Exception as e:
            raise e

    #################################################################
    # Find the corresponding ip of publisher from discoery
    #
    #
    # This part is left as an exercise.
    #################################################################
    def find(self, topic_list):
        LookupPubByTopicReq = discovery_pb2.LookupPubByTopicReq()
        LookupPubByTopicReq.topiclist.extend(topic_list)
        LookupPubByTopicReq.start_time = time.time()

        self.logger.info(LookupPubByTopicReq.topiclist)

        disc_req = discovery_pb2.DiscoveryReq()  # allocate

        disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC

        disc_req.lookup_req.CopyFrom(LookupPubByTopicReq)

        # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
        # a real string
        buf2send = disc_req.SerializeToString()

        self.req.send_multipart([b"", buf2send])

    ########################################
    # set upcall handle
    #
    # here we save a pointer (handle) to the application object
    ########################################

    def set_upcall_handle(self, upcall_obj):
        ''' set upcall handle '''
        self.upcall_obj = upcall_obj

    ########################################
    # disable event loop
    #
    # here we just make the variable go false so that when the event loop
    # is running, the while condition will fail and the event loop will terminate.
    ########################################
    def disable_event_loop(self):
        ''' disable event loop '''
        self.handle_events = False
