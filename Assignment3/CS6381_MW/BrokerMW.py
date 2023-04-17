###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the broker middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the broker side of things.
#
# As mentioned earlier, a broker serves as a proxy and hence has both
# publisher and subscriber roles. So in addition to the REQ socket to talk to the
# Discovery service, it will have both PUB and SUB sockets as it must work on
# behalf of the real publishers and subscribers. So this will have the logic of
# both publisher and subscriber middleware.


import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
from kazoo.client import KazooClient
from kazoo.recipe.election import Election
from kazoo.exceptions import NoNodeError, NodeExistsError
from kazoo.protocol.states import EventType

from CS6381_MW import discovery_pb2

class BrokerMW():

    def __init__(self,logger, callback=None):
        self.logger = logger  # internal logger for print statements
        self.xsub = None
        self.xpub = None
        self.req = None
        self.addr = None  # our advertised IP address
        self.pub_port = None
        self.sub_port = None  # port num where we are going to publish our topics
        self.upcall_obj = None  # handle to appln obj to handle appln-specific data
        self.handle_events = True
        self.poller = None

        self.my_znode = None
        self.leader_znode = None
        self.election_path = None
        self.broker_id = None
        self.bro_id = None
        self.callback = callback

    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info("BrokerMW::configure")

            # First retrieve our advertised IP addr and the publication port num

            self.addr = args.addr
            self.broker_id = args.broker_id
            self.sub_port = self.broker_id + 1
            self.pub_port = self.broker_id
            self.bro_id = args.bro_id

            # Next get the ZMQ context
            self.logger.info("BrokerMW::configure - obtain ZMQ context")
            context = zmq.Context()  # returns a singleton object

            # get the ZMQ poller object
            self.poller = zmq.Poller()
            # self.req = context.socket(zmq.REQ)


            # connect_str = "tcp://" + args.Broker
            # self.req.connect(connect_str)
            # Now acquire the REQ and PUB sockets
            # REQ is needed because we are the client of the Broker service
            # PUB is needed because we publish topic data
            self.logger.info("BrokerMW::configure - obtain REP sockets")

            #receiving data from pub
            self.xpub = context.socket(zmq.XPUB)
            bind_string1 = "tcp://*:" + str(self.pub_port)
            self.xpub.bind(bind_string1)
            self.poller.register(self.xpub, zmq.POLLIN)
            self.logger.info("BrokerMW::configure - obtain xpub sockets")
            #sending data to sub
            self.xsub = context.socket(zmq.XSUB)
            bind_string2 = "tcp://*:" + str(self.sub_port)
            self.xsub.bind(bind_string2)
            self.logger.info("BrokerMW::configure - obtain xsub sockets")



            self.zk = KazooClient(hosts="localhost:2181")
            self.zk.start()
            self.election_path = "/election/broker"
            self.election = Election(self.zk, self.election_path)
            self.run_election()


            self.watch_leader()
            self.logger.info("BrokerMW::configure completed")


        except Exception as e:
            raise e

    def transferData(self):
        self.logger.info("Transfering message from publisher to subscriber")
        zmq.proxy(self.xsub, self.xpub)

        # while True:
        #     message = self.xsub.recv()
        #     self.logger.info("Received Data from pub {}".format(message))
        #     self.xpub.send(message)
        #     self.logger.info("Send Data to sub {}".format(message))


    def set_upcall_handle(self, upcall_obj):
        ''' set upcall handle '''
        self.upcall_obj = upcall_obj


    def create_election_path(self):
        try:
            self.zk.create(self.election_path, ephemeral=False)
        except NodeExistsError:
            self.logger.warning("Node already exists: {}".format(self.election_path))

    def create_candidate_znode(self):
        self.my_znode = self.zk.create(f"{self.election_path}/candidate_", value=self.bro_id.encode(),
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
            data = f"{self.addr}:{self.broker_id}".encode("utf-8")
            self.zk.set(self.leader_znode,data)
            return True
        return False

    def run_election(self):
        self.create_election_path()
        if self.my_znode is None:
            self.create_candidate_znode()
        leader_path = self.get_leader()
        self.logger.info("leader path is {}".format(leader_path))
        if self.am_i_leader():
            self.logger.info("i {} am leader".format(self.broker_id))
            self.cleanup_candidates()
        else:
            self.logger.info("i {} am follower".format(self.broker_id))

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

            if self.callback:
                self.callback(ip, int(port))

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
