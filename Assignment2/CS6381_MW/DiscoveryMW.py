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
#       Discovery Middleware class
##################################


class DiscoveryMW ():

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

    self.pub_info = defaultdict(list)  # setup to save the information of publisher and subscriber
    self.sub_info = defaultdict(list)

    # mapping from topics to a list of address
    self.topic_pub_map = defaultdict(list)

    self.count_pub = 0
    self.count_sub = 0

  ########################################
  # configure/initialize
  ########################################
  def configure(self, args):
    ''' Initialize the object '''

    try:
      # Here we initialize any internal variables
      self.logger.info("DiscoveryMW::configure")

      # First retrieve our advertised IP addr and the discovery port num
      self.port = args.port
      self.addr = args.addr
      self.id = args.name

      # Next get the ZMQ context

      self.logger.debug("DiscoveryMW::configure - obtain ZMQ context")
      context = zmq.Context()  # returns a singleton object

      # get the ZMQ poller object
      self.logger.debug("DiscoveryMW::configure - obtain the poller")
      self.poller = zmq.Poller()

      # Now acquire the REP sockets
      # REP is needed because we are the Server of the lookup service
      self.logger.debug("DiscoveryMW::configure - obtain REP sockets")
      self.rep = context.socket(zmq.ROUTER)
      self.logger.debug("DiscoveryMW::configure - obtain REQ sockets")
      self.req = context.socket(zmq.DEALER)
      self.req.identity = bytes(self.id, "utf-8")

      # Since are using the event loop approach, register the REP socket for incoming events
      self.logger.debug(
          "DiscoveryMW::configure - register the REP socket for incoming replies")
      self.poller.register(self.rep, zmq.POLLIN)
      self.logger.debug(
          "DiscoveryMW::configure - register the REQ socket for incoming replies")
      self.poller.register(self.req, zmq.POLLIN)

      self.logger.debug(
          "DiscoveryMW::configure - register the number of pubs and subs")
      self.num_pub = args.num_pub
      self.num_sub = args.num_sub

      self.num_disc = args.num_disc

      # Since we are the discovery, the best practice as suggested in ZMQ is for us to
      # "bind" the REP socket
      self.logger.debug("DiscoveryMW::configure - bind to the rep socket")
      # note that we publish on any interface hence the * followed by port number.
      # We always use TCP as the transport mechanism (at least for these assignments)
      # Since port is an integer, we convert it to string to make it part of the URL
      bind_string = "tcp://*:" + str(self.port)


      self.rep.bind(bind_string)

      self.logger.info("DiscoveryMW:: configure - finger table generation")
      self.create_finger_table(args)

      self.logger.info("DiscoveryMW::configure completed")

    except Exception as e:
      raise e


  def create_finger_table(self, args):
    dht_info = json.load(open(args.json_file, 'rb'))['dht']

    self.sorted_hash = []
    self.finger_table = {}


    for i in range(len(dht_info)):

      self_id, hash_val, ip, port = dht_info[i]['id'], dht_info[i]['hash'], dht_info[i]['IP'], dht_info[i]['port']

      self.sorted_hash.append((hash_val, self_id, ip, port))

      if self_id == self.id:
          self.hash = hash_val
          self.ip = ip
          self.port = port


    self.sorted_hash = sorted(self.sorted_hash) #[[hash_val, self_id, ip, port], [hash_val, self_id, ip, port], ...,[hash_val, self_id, ip, port]]
    self.index = [i for i, tup in enumerate(self.sorted_hash) if tup[1] == self.id]
    self.index = self.index[0]

    self.logger.info([(h, i) for h, i, _, _ in self.sorted_hash])
    for m in range(48):
        cur_hash = (self.hash + 2 ** m) % (2 ** 48)

        # self.logger.info(self.hash)
        # self.logger.info(cur_hash)
        idx = 0
        while cur_hash <= self.sorted_hash[-1][0] and idx < len(self.sorted_hash) and self.sorted_hash[idx][0] < cur_hash:
            idx += 1

        # self.logger.info(self.sorted_hash[idx][0])
        self.finger_table[m] = self.sorted_hash[idx]

    self.logger.info(self.finger_table)

  def find_successor(self, hash_val):
    self.logger.info('Find successor node of hash value - {}'.format(hash_val))

    # self.logger.info(str(hash_val))
    # self.logger.info(str(self.hash))

    if hash_val > self.hash and hash_val <= self.finger_table[0][0]:
      return self.finger_table[0]

    else:
      tmp = self.closet_proceding(hash_val)

      return tmp

  def closet_proceding(self, hash_val):
    self.logger.info('Find proceding node')
    # self.logger.info('1: ' + str(hash_val))

    if hash_val <= self.hash:
      for i in range(47, -1, -1):
          if self.finger_table[i][0] <= self.hash:
            if self.finger_table[i][0]  < hash_val:
              return self.finger_table[i]
          else:
            return self.finger_table[i]
    else:
      for i in range(47, -1, -1):
        if self.finger_table[i][0] < hash_val and self.finger_table[i][0] > self.hash:
          return self.finger_table[i]

  #################################################################
  # run the event loop where we expect to receive a reply to a sent request
  #################################################################
  def event_loop(self, timeout=None):

    try:
      self.logger.info("DiscoveryMW::event_loop - run the event loop")

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

        if self.rep in events:  # this is the only socket on which we should be receiving replies
          # handle the incoming reply from remote entity and return the result
          timeout = self.handle_reply()

        else:
          raise Exception("Unknown event after poll")

      self.logger.info("DiscoveryMW::event_loop - out of the event loop")
    except Exception as e:
      raise e

  #################################################################
  # handle an incoming reply
  #################################################################
  def handle_reply(self):

    try:
      self.logger.info("DiscoveryMW::handle_reply")

      # let us first receive all the bytes
      bytesRcvd = self.rep.recv_multipart()
      self.logger.info(bytesRcvd)
      identity = bytesRcvd[0]
      bytesRcvd = bytesRcvd[2]

      # now use protobuf to deserialize the bytes
      # The way to do this is to first allocate the space for the
      # message we expect, here REGISTERRESP and then parse
      # the incoming bytes and populate this structure (via protobuf code)
      disc_req = discovery_pb2.DiscoveryReq()
      disc_req.ParseFromString(bytesRcvd)
      self.logger.info('5555')

      # demultiplex the message based on the message type but let the application
      # object handle the contents as it is best positioned to do so. See how we make
      # the upcall on the application object by using the saved handle to the appln object.
      #
      # Note also that we expect the return value to be the desired timeout to use
      # in the next iteration of the poll.

      if (disc_req.msg_type == discovery_pb2.TYPE_REGISTER):
        self.logger.info('6666')
        # As long as the discovery receives the register information, need to update either self.pub_info or self.sub_info
        register_req = disc_req.register_req
        role, id, addr, port, topiclist, start_reg_time = register_req.role, register_req.info.id, register_req.info.addr, register_req.info.port, register_req.topiclist, register_req.info.time

        if role == discovery_pb2.ROLE_PUBLISHER or role == discovery_pb2.ROLE_SUBSCRIBER:
          for topic in topiclist:
            topic_hash = self.hash_func(topic)

            self.logger.info(topic_hash)

            if topic_hash <= self.hash:
              if role == discovery_pb2.ROLE_PUBLISHER:
                self.pub_info[topic].append((addr, port))
                self.logger.info(str(len(self.pub_info)))
                self.logger.info('regis_pub_succ1132421')
              elif role == discovery_pb2.ROLE_SUBSCRIBER:
                self.logger.info('sub_register_111111111323')
                self.sub_info[topic].append((addr, port))
            else:
              successor = self.find_successor(topic_hash) #hash, id, addr, port

              self.logger.info('topic hash value is {}'.format(topic_hash))
              self.logger.info('successor hash value is {}'.format(successor[0]))
              self.logger.info('successor disc id is {}'.format(successor[1]))
              self.logger.info('successor disc addr is {}'.format(successor[2]))

              self.transfer_register_info(role, topic, successor, id, addr, port)
              self.transfer_register_reply()

          timeout = self.register_reply(identity, start_reg_time)

      elif (disc_req.msg_type == discovery_pb2.TYPE_ISREADY):
        num_pub, num_sub, start_idx = disc_req.isready_req.num_pub, disc_req.isready_req.num_sub, disc_req.isready_req.start_idx

        self.logger.info(str(num_pub))
        self.logger.info(str(num_sub))
        self.logger.info(str(start_idx))
        self.logger.info(str(self.index))

        sum_pub = sum([len(self.pub_info[key]) for key in self.pub_info])
        sum_sub = sum([len(self.sub_info[key]) for key in self.sub_info])


        if start_idx != self.index + 1:
          if start_idx == 200: #this is_ready check is from pub/sub
            start_idx = self.index

          self.transfer_ready_info(num_pub, num_sub, start_idx)
          status = self.transfer_ready_reply()



          timeout = self.is_ready_reply(identity, num_pub + sum_pub, num_sub + sum_sub, status, method = 1)
        else:
          timeout = self.is_ready_reply(identity, num_pub + sum_pub, num_sub + sum_sub, status = None, method = 2)

      elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
        self.logger.info('Find!!!!')

        publisher_addr, publisher_port = [], []
        addr_port = set()
        self.logger.info(disc_req.lookup_req.topiclist)
        start_time = disc_req.lookup_req.start_time

        for topic in disc_req.lookup_req.topiclist:
          topic_hash = self.hash_func(topic)
          self.logger.info(topic_hash)

          if topic_hash <= self.hash:
            self.logger.info(str(self.pub_info[topic]))
            for addr, port in self.pub_info[topic]:
              if (addr, port) in addr_port:
                continue
              else:
                addr_port.add((addr, port))

                publisher_addr.append(addr)
                publisher_port.append(port)
          else:
            successor = self.find_successor(topic_hash) #hash, id, addr, port

            self.logger.info('topic hash value is {}'.format(topic_hash))
            self.logger.info('successor hash value is {}'.format(successor[0]))
            self.logger.info('successor disc id is {}'.format(successor[1]))
            self.logger.info('successor disc addr is {}'.format(successor[2]))

            self.transfer_find_info(topic, successor)
            addrs, ports = self.transfer_find_reply()
            publisher_addr.extend(addrs)
            publisher_port.extend(ports)

          timeout = self.find_reply(identity, publisher_addr, publisher_port, start_time)


      else:  # anything else is unrecognizable by this object
        # raise an exception here
        raise ValueError("Unrecognized response message")

      return timeout

    except Exception as e:
      raise e

  def transfer_register_reply(self):
    try:
      self.logger.info("DiscoveryMW::transfer_register_reply")
      bytesRcvd = self.req.recv_multipart()
      bytesRcvd = bytesRcvd[1]

      disc_resp = discovery_pb2.DiscoveryResp()
      disc_resp.ParseFromString(bytesRcvd)
      self.logger.info("DiscoveryMW::transfer_register_reply_Receive data {}".format(disc_resp))

      if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
          if (disc_resp.register_resp.status == discovery_pb2.STATUS_SUCCESS):
              self.logger.info("DiscoveryAppln::transfer register_response - registration is success")

      else:  # anything else is unrecognizable by this object
          # raise an exception here
          raise ValueError("Unrecognized response message")

    except Exception as e:
        raise e

  def transfer_register_info(self, role, topic, successor, pub_id, pub_addr, pub_port):
    try:
      self.logger.info("DiscoveryMW:: transfer pub/sub info")

      reg_info = discovery_pb2.RegistrantInfo () # allocate
      reg_info.id = pub_id # our id
      reg_info.addr = pub_addr  # our advertised IP addr where we are publishing
      reg_info.port = pub_port # port on which we are publishing
      reg_info.time = time.time()
      self.logger.info ("DiscoveryMW::register - done populating the Registrant Info for transfer")

      # Next build a RegisterReq message
      self.logger.info ("DiscoveryMW::register - populate the nested register req for transfer")
      register_req = discovery_pb2.RegisterReq ()  # allocate
      register_req.role = role  # we are a publisher
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      register_req.info.CopyFrom (reg_info)  # copy contents of inner structure
      register_req.topiclist[:] = [topic]   # this is how repeated entries are added (or use append() or extend ()
      self.logger.info ("DiscoveryMW::register - done populating nested RegisterReq for transfer")

      # Finally, build the outer layer DiscoveryReq Message
      self.logger.info ("DiscoveryMW::register - build the outer DiscoveryReq message for transfer")
      disc_req = discovery_pb2.DiscoveryReq ()  # allocate
      disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.register_req.CopyFrom (register_req)
      self.logger.info ("DiscoveryMW::register - done building the outer message for transfer")

      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_req.SerializeToString ()
      self.logger.info ("Stringified serialized buf = {}".format (buf2send))

      addr = successor[2] + ':' + str(successor[3])
      self.logger.info ("DiscoveryMW:: register - transfer address {}".format(addr))
      connect_str = "tcp://" + addr
      self.logger.info(connect_str)
      self.logger.info ("DiscoveryMW:: register - transfer address {}".format(connect_str))
      self.req.connect(connect_str)
      self.logger.info ("DiscoveryMW:: Connect next node successfully")
      self.req.send_multipart([b"", buf2send])

      # now go to our event loop to receive a response to this request
      self.logger.info ("DiscoveryMW::register - transfer register message and now wait for reply for transfer")

    except Exception as e:
      raise e

  def transfer_ready_info(self, num_pub, num_sub, start_idx):
    try:
      self.logger.info ("DiscoveryMW::is_ready_transfer")

      # we do a similar kind of serialization as we did in the register
      # message but much simpler as the message format is very simple.
      # Then send the request to the discovery service

      # The following code shows serialization using the protobuf generated code.

      # first build a IsReady message
      self.logger.debug ("DiscoveryMW::is_ready - populate the nested IsReady msg")
      isready_req = discovery_pb2.IsReadyReq ()  # allocate

      tmp_sum_pub = sum([len(self.pub_info[key]) for key in self.pub_info])
      tmp_sum_sub = sum([len(self.sub_info[key]) for key in self.sub_info])

      self.logger.info(str(num_pub) + ':' + str(tmp_sum_pub))
      self.logger.info(str(num_sub) + ':' + str(tmp_sum_sub))

      isready_req.num_pub = num_pub + tmp_sum_pub
      isready_req.num_sub = num_sub + tmp_sum_sub

      self.logger.info(str(num_pub) + ':' + str(tmp_sum_pub))
      self.logger.info(str(num_sub) + ':' + str(tmp_sum_sub))
      isready_req.start_idx = start_idx

      # actually, there is nothing inside that msg declaration.
      self.logger.debug ("DiscoveryMW::is_ready - done populating nested IsReady msg")

      # Build the outer layer Discovery Message
      self.logger.debug ("DiscoveryMW::is_ready - build the outer DiscoveryReq message")
      disc_req = discovery_pb2.DiscoveryReq ()
      disc_req.msg_type = discovery_pb2.TYPE_ISREADY
      # It was observed that we cannot directly assign the nested field here.
      # A way around is to use the CopyFrom method as shown
      disc_req.isready_req.CopyFrom (isready_req)
      self.logger.debug ("DiscoveryMW::is_ready - done building the outer message")

      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_req.SerializeToString ()
      self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

      next_disc = self.sorted_hash[(self.index + 1) % self.num_disc]
      addr = next_disc[2] + ':' + str(next_disc[3])
      self.logger.info ("DiscoveryMW:: is_ready - check transfer address {}, {}".format(addr, next_disc[1]))
      connect_str = "tcp://" + addr
      self.logger.info(connect_str)
      self.logger.info ("DiscoveryMW:: is_ready - check transfer address {}".format(connect_str))
      self.req.connect(connect_str)
      self.logger.info ("DiscoveryMW:: is_ready - check Connect next node successfully")

      # now send this to our discovery service
      self.logger.debug ("DiscoveryMW::is_ready - send stringified buffer to Next Discovery service")
      self.req.send_multipart([b"", buf2send])  # we use the "send" method of ZMQ that sends the bytes

      # now go to our event loop to receive a response to this request
      self.logger.info ("DiscoveryMW::is_ready - request sent and now wait for reply")

    except Exception as e:
      raise e

  def transfer_ready_reply(self):
    try:
      self.logger.info("DiscoveryMW::transfer_ready_reply")
      bytesRcvd = self.req.recv_multipart()
      bytesRcvd = bytesRcvd[1]

      disc_resp = discovery_pb2.DiscoveryResp()
      disc_resp.ParseFromString(bytesRcvd)
      self.logger.info("DiscoveryMW::transfer_ready_reply_statua {}".format(disc_resp.isready_resp.status))

      return disc_resp.isready_resp.status

    except Exception as e:
        raise e

  def transfer_find_info(self, topic, successor):
    try:
      self.logger.info("DiscoveryMW:: transfer sub's find topic info")

      LookupPubByTopicReq = discovery_pb2.LookupPubByTopicReq()
      LookupPubByTopicReq.topiclist.extend([topic])

      disc_req = discovery_pb2.DiscoveryReq()  # allocate

      disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC

      disc_req.lookup_req.CopyFrom(LookupPubByTopicReq)

      # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
      # a real string
      buf2send = disc_req.SerializeToString()

      self.logger.info ("DiscoveryMW::transfer find topic info - done building the outer message for transfer")

      addr = successor[2] + ':' + str(successor[3])
      self.logger.info ("DiscoveryMW:: transfer find - transfer address {}".format(addr))
      connect_str = "tcp://" + addr
      self.logger.info(connect_str)
      self.logger.info ("DiscoveryMW:: transfer find - transfer address {}".format(connect_str))
      self.req.connect(connect_str)
      self.logger.info ("DiscoveryMW:: transfer find - Connect next node successfully")
      self.req.send_multipart([b"", buf2send])

      # now go to our event loop to receive a response to this request
      self.logger.info ("DiscoveryMW::register - transfer find message and now wait for reply for transfer")

    except Exception as e:
      raise e

  def transfer_find_reply(self):
    try:
      self.logger.info("DiscoveryMW::transfer_find_reply")
      bytesRcvd = self.req.recv_multipart()
      bytesRcvd = bytesRcvd[1]

      disc_resp = discovery_pb2.DiscoveryResp()
      disc_resp.ParseFromString(bytesRcvd)
      self.logger.info("DiscoveryMW::transfer_find_reply_Receive data {}".format(disc_resp))

      addrs, ports = [], []

      lookup_resp = disc_resp.lookup_resp
      for info in lookup_resp.info:
        addrs.append(info.addr)
        ports.append(info.port)

      self.logger.info('finish')

      return addrs, ports

    except Exception as e:
        raise e

  def hash_func(self, id):
    self.logger.debug("ExperimentGenerator::hash_func")
    # first get the digest from hashlib and then take the desired number of bytes from the
    # lower end of the 256 bits hash. Big or little endian does not matter.
    hash_digest = hashlib.sha256(bytes(id, "utf-8")).digest()  # this is how we get the digest or hash value
    # figure out how many bytes to retrieve
    num_bytes = int(48 / 8)  # otherwise we get float which we cannot use below
    hash_val = int.from_bytes(hash_digest[:num_bytes], "big")  # take lower N number of bytes

    return hash_val
  ########################################
  # the discovery returns yes back to the corresponding publisher or subscriber
  #
  # Here we send the registered message and do the serialization
  #
  # No return value from this as it is handled in the invoke_operation
  # method of the application object.
  ########################################

  def register_reply(self, identity, start_reg_time):
    try:
      self.logger.info("DiscoveryMW::register")

      # First build a RegisterResp message
      register_resp = discovery_pb2.RegisterResp()
      register_resp.status = discovery_pb2.STATUS_SUCCESS
      register_resp.start_time = start_reg_time

      # Then build the outer layer DiscoveryReq Message
      disc_resp = discovery_pb2.DiscoveryResp()
      disc_resp.msg_type = discovery_pb2.TYPE_REGISTER
      disc_resp.register_resp.CopyFrom(register_resp)

      buf2send = disc_resp.SerializeToString()

      # now send this to our pub/sub service
      self.logger.debug(
          "DiscoveryMW:: register success - send stringified buffer to pub/sub service")
      # we use the "send" method of ZMQ that sends the bytes
      self.rep.send_multipart([identity, b"", buf2send])

      # now go to our event loop to receive a response to this request
      self.logger.info("DiscoverMW:: register success - notification sent")

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

  def is_ready_reply(self, identity, num_pub, num_sub, status, method):
    try:
      self.logger.info("DiscoveryMW::is_ready")

      # First build a IsReadyResp message
      self.logger.debug(
          "DiscoveryMW::is_ready_resp - populate the nested IsReady msg")
      isread_resp = discovery_pb2.IsReadyResp()
      if method == 2:
        self.logger.info('method2')
        isread_resp.status = ((num_pub == self.num_pub) & (num_sub == self.num_sub))*1  # 1 - yes; 0 - no
        self.logger.info(str(num_pub) + ':' + str(self.num_pub))
        self.logger.info(str(num_sub) + ':' + str(self.num_sub))
      elif method == 1:
        isread_resp.status = status
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
      self.rep.send_multipart([identity, b"", buf2send])

      # now go to our event loop to receive a response to this request
      self.logger.info("DiscoverMW:: is_read_Resp - notification sent")

      return None

    except Exception as e:
      raise e

  def find_reply(self, identity, publisher_addr, publisher_port, start_time):
    try:
      self.logger.info("DiscoveryMW::receiving subscriber's interested topics")

      # First build a LookupPubByTopicResp message
      self.logger.info(
          "DiscoveryMW::LookupPubByTopicResp - populate the nested LookupPubByTopicResp msg")
      LookupPubByTopicResp = discovery_pb2.LookupPubByTopicResp()
      for addr, port in zip(publisher_addr, publisher_port):
        reg_info = discovery_pb2.RegistrantInfo()

        reg_info.addr = addr
        reg_info.port = port

        LookupPubByTopicResp.info.append(reg_info)
        LookupPubByTopicResp.start_time = start_time
      # self.logger.info(publisher_addr)
      # self.logger.info(publisher_port)
      # self.logger.info(LookupPubByTopicResp.addr)
      # LookupPubByTopicResp.addr.extend(publisher_addr)
      # self.logger.info('12313')
      # LookupPubByTopicResp.port.extend(publisher_port)


      self.logger.info(
          "DiscoveryMW::LookupPubByTopicResp - done populating nested LookupPubByTopicResp msg")

      # Then build the outer layer Discovery Message
      self.logger.info(
          "DiscoveryMW::LookupPubByTopicResp - build the outer LookupPubByTopicResp message")
      disc_resp = discovery_pb2.DiscoveryResp()
      disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
      disc_resp.lookup_resp.CopyFrom(LookupPubByTopicResp)

      buf2send = disc_resp.SerializeToString()


      # now send this to our pub/sub service
      self.logger.info(
          "DiscoveryMW:: LookupPubByTopicResp - send stringified buffer to sub service")
      # we use the "send" method of ZMQ that sends the bytes
      self.rep.send_multipart([identity, b"", buf2send])

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
