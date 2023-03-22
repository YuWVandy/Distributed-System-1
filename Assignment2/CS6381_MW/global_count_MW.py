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


# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging  # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
from collections import defaultdict
import json
# import serialization logic
from CS6381_MW import discovery_pb2
import hashlib
# from CS6381_MW import topic_pb2  # you will need this eventually

# import any other packages you need.

##################################
#       Global_Count Middleware class
##################################


class Global_Count_MW ():

  ########################################
  # constructor
  ########################################
  def __init__(self, logger):
    self.logger = logger  # internal logger for print statements
    self.rep = None  # will be a ZMQ REP socket to talk to Discovery service
    self.poller = None  # used to wait on incoming replies
    self.addr = None  # our advertised IP address
    self.port = None  # port num where we are going to get registeration information
    self.upcall_obj = None  # handle to appln obj to handle appln-specific data
    self.handle_events = True  # in general we keep going thru the event loop

    self.num_pub = None
    self.num_sub = None

    self.count_pub = 0
    self.count_sub = 0

  ########################################
  # configure/initialize
  ########################################
  def configure(self, args):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.info("GlobalCountMW::configure")

      # First retrieve our advertised IP addr and the discovery port num
      self.port = args.port
      self.addr = args.addr
      self.id = args.name

      # Next get the ZMQ context

      self.logger.debug("GlobalCountMW::configure - obtain ZMQ context")
      context = zmq.Context()  # returns a singleton object

      # get the ZMQ poller object
      self.logger.debug("GlobalCountMW::configure - obtain the poller")
      self.poller = zmq.Poller()

      # Now acquire the REP sockets
      # REP is needed because we are the Server of the lookup service
      self.logger.debug("GlobalCountMW::configure - obtain REQ sockets")
      self.rep = context.socket(zmq.REP)

      # Since are using the event loop approach, register the REP socket for incoming events
      self.logger.debug(
          "GlobalCountMW::configure - register the REP socket for incoming replies")
      self.poller.register(self.rep, zmq.POLLIN)

      bind_string = "tcp://*:" + str(self.port)

      self.rep.bind(bind_string)


      self.logger.debug(
          "GlobalCountMW::configure - register the number of pubs and subs")
      self.num_pub = args.num_pub
      self.num_sub = args.num_sub


      self.logger.info("GlobalCountMW::configure completed")

    except Exception as e:
      raise e

  #################################################################
  # run the event loop where we expect to receive a reply to a sent request
  #################################################################
  def event_loop(self, timeout=None):

    try:
      self.logger.info("GlobalCountMW::event_loop - run the event loop")

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
        if self.rep in events:  # this is the only socket on which we should be receiving replies
          # handle the incoming reply from remote entity and return the result
          timeout = self.handle_reply()

        else:
          raise Exception("Unknown event after poll")

      self.logger.info("GlobalCountMW::event_loop - out of the event loop")
    except Exception as e:
      raise e

  #################################################################
  # handle an incoming reply
  #################################################################
  def handle_reply(self):

    try:
      self.logger.info("GlobalCountMW::handle_reply")

      # let us first receive all the bytes
      bytesRcvd = self.rep.recv()

      # now use protobuf to deserialize the bytes
      # The way to do this is to first allocate the space for the
      # message we expect, here REGISTERRESP and then parse
      # the incoming bytes and populate this structure (via protobuf code)
      Global_Count = discovery_pb2.Global_Count()
      Global_Count.ParseFromString(bytesRcvd)

      if Global_Count.msg_type == discovery_pb2.TYPE_COUNT:
        count_req = Global_Count.count_req

        # demultiplex the message based on the message type but let the application
        # object handle the contents as it is best positioned to do so. See how we make
        # the upcall on the application object by using the saved handle to the appln object.
        #
        # Note also that we expect the return value to be the desired timeout to use
        # in the next iteration of the poll.

        count_info = count_req.count_info
        self.count_pub, self.count_sub = self.count_pub + count_info.pub_num, self.count_sub + count_info.sub_num

        timeout = self.count_reply()

      elif Global_Count.msg_type == discovery_pb2.TYPE_ISREADY:

        timeout = self.is_ready_reply()

      else:  # anything else is unrecognizable by this object
        # raise an exception here
        raise ValueError("Unrecognized response message")

      return timeout

    except Exception as e:
      raise e

  ########################################
  # the global counter returns nothing
  #
  # Here we send the registered message and do the serialization
  #
  # No return value from this as it is handled in the invoke_operation
  # method of the application object.
  ########################################

  def count_reply(self):
    try:
      self.logger.info("GlobalCountMW::count_reply")

      # First build a RegisterResp message
      count_resp = discovery_pb2.CountResp()
      count_resp.status = discovery_pb2.STATUS_SUCCESS

      # Then build the outer layer DiscoveryReq Message
      Global_Count_resp = discovery_pb2.Global_count_Resp()
      Global_Count_resp.msg_type = discovery_pb2.TYPE_COUNT
      Global_Count_resp.register_resp.CopyFrom(register_resp)

      buf2send = disc_resp.SerializeToString()

      # now send this to our pub/sub service
      self.logger.debug(
          "GlobalCountMW:: count success - send stringified buffer to discovery service")
      # we use the "send" method of ZMQ that sends the bytes
      self.rep.send(buf2send)

      # now go to our event loop to receive a response to this request
      self.logger.info("GlobalCountMW:: count success - notification sent")

      return None

    except Exception as e:
      raise e

  ########################################
  # the discovery returns yes back to the all publisher and subscriber if the system is ready to go
  #
  # Here we send the is-ready message and do the serialization
  #
  # No return value from this as it is handled in the invoke_operation
  # method of the application object.
  ########################################

  def ready_reply(self):
    try:
      self.logger.info("DiscoveryMW::is_ready")

      # First build a IsReadyResp message
      self.logger.debug(
          "DiscoveryMW::is_ready_resp - populate the nested IsReady msg")
      isread_resp = discovery_pb2.IsReadyResp()
      isread_resp.status = self.system_status  # 1 - yes; 0 - no
      self.logger.debug(
          "DiscoveryMW::is_ready_resp - done populating nested IsReady msg")

      # Then build the outer layer Discovery Message
      self.logger.debug(
          "DiscoveryMW::is_ready - build the outer DiscoveryResp message")
      disc_resp = discovery_pb2.DiscoveryResp()
      disc_resp.msg_type = discovery_pb2.TYPE_ISREADY
      disc_resp.isready_resp.CopyFrom(isread_resp)

      buf2send = disc_resp.SerializeToString()

      # now send this to our pub/sub service
      self.logger.debug(
          "DiscoveryMW:: is_ready_Resp - send stringified buffer to pub/sub service")
      # we use the "send" method of ZMQ that sends the bytes
      self.rep.send(buf2send)

      # now go to our event loop to receive a response to this request
      self.logger.info("DiscoverMW:: is_read_Resp - notification sent")

      return None

    except Exception as e:
      raise e

  def find_reply(self, lookup_req):
    try:
      self.logger.info("DiscoveryMW::receiving subscriber's interested topics")

      # search for corresponding publisher
      publisher_addr, publisher_port = [], []
      addr_port = set()
      for topic in lookup_req.topiclist:
        for addr, port in self.topic_pub_map[topic]:
          if (addr, port) in addr_port:
            continue
          else:
            addr_port.add((addr, port))

            publisher_addr.append(addr)
            publisher_port.append(port)

      # First build a LookupPubByTopicResp message
      self.logger.debug(
          "DiscoveryMW::LookupPubByTopicResp - populate the nested LookupPubByTopicResp msg")
      LookupPubByTopicResp = discovery_pb2.LookupPubByTopicResp()
      LookupPubByTopicResp.addr.extend(publisher_addr)
      LookupPubByTopicResp.port.extend(publisher_port)

      self.logger.debug(
          "DiscoveryMW::LookupPubByTopicResp - done populating nested LookupPubByTopicResp msg")

      # Then build the outer layer Discovery Message
      self.logger.debug(
          "DiscoveryMW::LookupPubByTopicResp - build the outer LookupPubByTopicResp message")
      disc_resp = discovery_pb2.DiscoveryResp()
      disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
      disc_resp.lookup_resp.CopyFrom(LookupPubByTopicResp)

      buf2send = disc_resp.SerializeToString()


      # now send this to our pub/sub service
      self.logger.debug(
          "DiscoveryMW:: LookupPubByTopicResp - send stringified buffer to sub service")
      # we use the "send" method of ZMQ that sends the bytes
      self.rep.send(buf2send)

      # print(self.p)

      # now go to our event loop to receive a response to this request
      self.logger.info("DiscoverMW:: LookupPubByTopicResp - notification sent")

      return None

    except Exception as e:
      raise e

  #################################################################
  # disseminate the data on our pub socket
  #
  # do the actual dissemination of info using the ZMQ pub socket
  #
  # Note, here I am sending three diff params. I am eventually going to replace this
  # sending of a simple string with protobuf serialization. Recall that we need to be
  # sending publisher id, topic, data, timestamp at a minimum for our experimental
  # data collection. So anyway we will need to do the necessary serialization.
  #
  # This part is left as an exercise.
  #################################################################
  def disseminate(self, id, topic, data):
    try:
      self.logger.debug("PublisherMW::disseminate")

      # Now use the protobuf logic to encode the info and send it.  But for now
      # we are simply sending the string to make sure dissemination is working.
      send_str = topic + ":" + data
      self.logger.debug("PublisherMW::disseminate - {}".format(send_str))

      # send the info as bytes. See how we are providing an encoding of utf-8
      self.pub.send(bytes(send_str, "utf-8"))

      self.logger.debug("PublisherMW::disseminate complete")
    except Exception as e:
      raise e

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
