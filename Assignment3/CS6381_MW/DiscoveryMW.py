###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the discovery middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student.
#
# The discovery service is a server. So at the middleware level, we will maintain
# a REP socket binding it to the port on which we expect to receive requests.
#
# There will be a forever event loop waiting for requests. Each request will be parsed
# and the application logic asked to handle the request. To that end, an upcall will need
# to be made to the application logic.

import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
from kazoo.client import KazooClient
from kazoo.recipe.election import Election
from kazoo.exceptions import NoNodeError, NodeExistsError
import kazoo
from kazoo.protocol.states import EventType
# import serialization logic
from CS6381_MW import discovery_pb2

class DiscoveryMW():

    def __init__(self, logger):
        self.logger = logger  # internal logger for print statements
        self.rep = None
        self.poller = None  # used to wait on incoming replies
        self.addr = None  # our advertised IP address
        self.port = 5555  # port num where we are going to publish our topics
        self.upcall_obj = None  # handle to appln obj to handle appln-specific data
        self.handle_events = True  # in general we keep going thru the event loop
        self.pub_info = None
        self.sub_info = None

        self.my_znode = None
        self.leader_znode= None
        self.election_path = None
        self.disc_id = None

    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info("DiscoveryMW::configure")

            # First retrieve our advertised IP addr and the publication port num
            self.port = args.port
            self.addr = args.addr
            self.disc_id = args.disc_id

            # Next get the ZMQ context
            self.logger.debug("DiscoveryMW::configure - obtain ZMQ context")
            context = zmq.Context()  # returns a singleton object

            # get the ZMQ poller object
            self.logger.debug("DiscoveryMW::configure - obtain the poller")
            self.poller = zmq.Poller()


            self.logger.debug("DiscoveryMW::configure - obtain REP sockets")
            self.rep = context.socket(zmq.REP)


            self.logger.debug("DiscoveryMW::configure - register the REP socket for incoming replies")
            self.poller.register(self.rep, zmq.POLLIN)



            # Since we are the subscriber, the best practice as suggested in ZMQ is for us to
            # "bind" the SUB socket
            self.logger.debug("DiscoveryMW::configure - bind to the sub socket")

            bind_string = "tcp://*:" + str(self.port)
            self.rep.bind(bind_string)

            self.logger.info("DiscoveryMW::configure completed")

            self.zk = KazooClient(hosts="localhost:2181")

            self.zk.start()
            self.election_path = "/election/discovery"
            self.election = Election(self.zk, self.election_path)

            self.run_election()
            self.logger.info('12313')

            self.watch_leader()



        except Exception as e:
            raise e


    def event_loop(self, timeout=None):

        try:
            self.logger.info("DiscoveryMW::event_loop - run the event loop")

            while self.handle_events:  # it starts with a True value
                # poll for events. We give it an infinite timeout.
                # The return value is a socket to event mask mapping
                events = dict(self.poller.poll(timeout=timeout))

                if not events:

                    timeout = self.upcall_obj.invoke_operation()

                elif self.rep in events:  # this is the only socket on which we should be receiving replies

                    # handle the incoming reply from remote entity and return the result
                    timeout = self.handle_request()
                else:
                    raise Exception("Unknown event after poll")

            self.logger.info("DiscoveryMW::event_loop - out of the event loop")
        except Exception as e:
            raise e


    def handle_request(self):

        try:
            self.logger.info("DiscoveryMW::handle_request")

            # let us first receive all the bytes
            bytesRcvd = self.rep.recv()

            # now use protobuf to deserialize the bytes
            # The way to do this is to first allocate the space for the
            # message we expect, here DiscoveryResp and then parse
            # the incoming bytes and populate this structure (via protobuf code)
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.ParseFromString(bytesRcvd)
            self.logger.info("DiscoveryMW::Receive data {}".format(disc_req))
            if (disc_req.msg_type == discovery_pb2.TYPE_REGISTER):
                # let the appln level object decide what to do

                timeout = self.upcall_obj.register_response(disc_req.register_req)
            elif (disc_req.msg_type == discovery_pb2.TYPE_ISREADY):
                # this is a response to is ready request
                timeout = self.upcall_obj.isready_response(disc_req.isready_req)

            elif(disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
                timeout = self.upcall_obj.lookup_response(disc_req.lookup_req)


            else:  # anything else is unrecognizable by this object
                # raise an exception here
                raise ValueError("Unrecognized response message")

            return timeout

        except Exception as e:
            raise e


    def reply(self, name, message):
        ''' register the appln with the discovery service '''

        try:
            self.logger.info("DiscoveryMW::reply register message")

            self.logger.info("DiscoveryMW::register - populate the Registrant Info")
            reg_info = discovery_pb2.RegistrantInfo()  # allocate
            reg_info.id = name  # our id

            self.logger.info("DiscoveryMW::register - done populating the Registrant Info")

            # Next build a RegisterReq message
            self.logger.info("DiscoveryMW::register - populate the nested register req")
            register_resp = discovery_pb2.RegisterResp()  # allocate
            register_resp.status = discovery_pb2.STATUS_SUCCESS  # we are a publisher
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            self.logger.info("DiscoveryMW::register - done populating nested RegisterReq")

            # Finally, build the outer layer DiscoveryReq Message
            self.logger.info("DiscoveryMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryResp()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.register_resp.CopyFrom(register_resp)
            self.logger.info("DiscoveryMW::register - done building the outer message")
            self.logger.info("DiscoveryMW::send data {}".format(disc_req))
            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()


            # now send this to our discovery service
            self.logger.info("DiscoveryMW::register - send stringified buffer to Discovery service")
            self.rep.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # now go to our event loop to receive a response to this request
            self.logger.info("DiscoveryMW::register - sent register message and now now wait for reply")

        except Exception as e:
            raise e


    def is_ready_reply(self):
        ''' register the appln with the discovery service '''

        try:
            self.logger.info("DiscoveryMW::reply register message")

            # as part of registration with the discovery service, we send
            # what role we are playing, the list of topics we are publishing,
            # and our whereabouts, e.g., name, IP and port

            # The following code shows serialization using the protobuf generated code.

            # Build the Registrant Info message first.
            self.logger.info("DiscoveryMW::register - populate the Registrant Info")
            reg_info = discovery_pb2.IsReadyResp()  # allocate
            reg_info.status = 1  # our id

            self.logger.info("DiscoveryMW::register - done populating the Registrant Info")


            # Finally, build the outer layer DiscoveryReq Message
            self.logger.info("DiscoveryMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryResp()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY  # set message type
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.isready_resp.CopyFrom(reg_info)
            self.logger.info("DiscoveryMW::register - done building the outer message")
            self.logger.info("DiscoveryMW::Sent data {}".format(disc_req))
            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()


            # now send this to our discovery service
            self.logger.info("DiscoveryMW::register - send stringified buffer to Discovery service")
            self.rep.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # now go to our event loop to receive a response to this request
            self.logger.info("DiscoveryMW::register - sent register message and now now wait for reply")

        except Exception as e:
            raise e


    def lookup_reply(self, regInfo):
        try:
            self.logger.info("DiscoveryMW::reply lookup request")

            # Build the Registrant Info message first.
            self.logger.info("DiscoveryMW::register - populate the Registrant Info")

            for reg in regInfo:
                reg_info = discovery_pb2.RegistrantInfo()  # allocate

                reg_info.id = reg.id
                reg_info.addr = reg.addr
                reg_info.port = reg.port

                self.logger.info("DiscoveryMW::Send reg_info :{}".format(reg_info))

                # Finally, build the outer layer DiscoveryReq Message
                self.logger.info("DiscoveryMW::register - build the outer DiscoveryReq message")
                disc_req = discovery_pb2.LookupPubByTopicResp()  # allocate
                # self.logger.info("DiscoveryMW::register - build the outer DiscoveryReq message")
                # disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC  # set message type
                # self.logger.info("DiscoveryMW::register - build the outer DiscoveryReq message")
                # It was observed that we cannot directly assign the nested field here.

                disc_req.info.append(reg_info)
            resp = discovery_pb2.DiscoveryResp()
            resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            resp.lookup_resp.CopyFrom(disc_req)
            # A way around is to use the CopyFrom method as shown

            self.logger.info("DiscoveryMW::register - done building the outer message")
            self.logger.info("DiscoveryMW::send data {}".format(resp))
            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string

            buf2send = resp.SerializeToString()


            # now send this to our discovery service
            self.logger.info("DiscoveryMW::register - send stringified buffer to Discovery service")
            self.rep.send(buf2send)  # we use the "send" method of ZMQ that sends the bytes

            # now go to our event loop to receive a response to this request
            self.logger.info("DiscoveryMW::register - sent register message and now now wait for reply")

        except Exception as e:
            raise e



    def set_upcall_handle(self, upcall_obj):
        ''' set upcall handle '''
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        ''' disable event loop '''
        self.handle_events = False

    # def be_leader(self):
    #     self.logger.info("Discovery {} is the leader".format(self.port))
    #     pass
    #
    # def participant_election(self):
    #     self.logger.info("Discovery {} participant election".format(self.port))
    #     self.election.run(self.be_leader)
    #
    # def stop_session(self):
    #     self.zk.stop()

    def create_election_path(self):
        try:
            self.zk.create(self.election_path, ephemeral=False)
        except NodeExistsError:
            self.logger.warning("Node already exists: {}".format(self.election_path))

    def create_candidate_znode(self):
        self.my_znode = self.zk.create(f"{self.election_path}/candidate_", value=self.disc_id.encode(),
                                           ephemeral=True, sequence=True)

    def get_leader(self):
        children = self.zk.get_children(self.election_path)
        children = sorted(children)
        if not children:
            return None
        children.sort()
        self.leader_znode = f"{self.election_path}/{children[0]}"
        self.logger.info("leader node is {}".format(self.leader_znode))
        return self.leader_znode

    def am_i_leader(self):
        if self.my_znode == self.leader_znode:
            data = f"{self.addr}:{self.port}".encode("utf-8")
            self.zk.set(self.leader_znode,data)
            return True
        return False

    def run_election(self):
        self.logger.info('1234')
        self.create_election_path()

        if self.my_znode is None:
            self.create_candidate_znode()
        leader_path = self.get_leader()
        self.logger.info("leader path is {}".format(leader_path))
        if self.am_i_leader():
            self.logger.info("i {} am leader".format(self.disc_id))
            self.cleanup_candidates()
        else:
            self.logger.info("i {} am follower".format(self.disc_id))


    def watch_leader(self):
        def watch_leader_event(event):
            if event.type == EventType.DELETED:
                self.logger.info("Leader znode deleted, starting a new leader election.")
                self.run_election()

        leader_path = self.leader_znode
        try:
            # Set the watch on the leader znode
            data, _ = self.zk.get(leader_path, watch=watch_leader_event)
            ip, port = data.decode("utf-8").split(":")
            self.logger.info(f"Current leader is {ip}:{port}")
        except NoNodeError:
            self.logger.error("Leader znode does not exist. Starting a new leader election.")
            self.run_election()

    def cleanup_candidates(self):
        if self.leader_znode is None:
            self.logger.warning("No leader znode found, skipping cleanup_candidates.")
            return

        children = self.zk.get_children(self.election_path)
        sorted_children = sorted(children)

        leader_name = self.leader_znode.split("/")[-1]
        if leader_name not in sorted_children:
            self.logger.warning(f"Leader znode {leader_name} not found in children, skipping cleanup_candidates.")
            return

        # Find the index of the current leader znode in the sorted list
        leader_index = sorted_children.index(leader_name)

        # Remove all candidate znodes that are not the current leader
        for i in range(leader_index):
            candidate_path = f"{self.election_path}/{sorted_children[i]}"
            try:
                self.zk.delete(candidate_path)
                self.logger.info(f"Removed old candidate znode: {candidate_path}")
            except NoNodeError:
                self.logger.warning(f"Node already deleted: {candidate_path}")
            except Exception as e:
                self.logger.error(f"Error while deleting znode: {candidate_path}, Error: {e}")
    def stop(self):
        self.zk.stop()
        self.zk.close()
