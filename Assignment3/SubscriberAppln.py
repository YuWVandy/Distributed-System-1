###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise to the student. Design the logic in a manner similar
# to the PublisherAppln. As in the publisher application, the subscriber application
# will maintain a handle to the underlying subscriber middleware object.
#
# The key steps for the subscriber application are
# (1) parse command line and configure application level parameters
# (2) obtain the subscriber middleware object and configure it.
# (3) As in the publisher, register ourselves with the discovery service
# (4) since we are a subscriber, we need to ask the discovery service to
# let us know of each publisher that publishes the topic of interest to us. Then
# our middleware object will connect its SUB socket to all these publishers
# for the Direct strategy else connect just to the broker.
# (5) Subscriber will always be in an event loop waiting for some matching
# publication to show up. We also compute the latency for dissemination and
# store all these time series data in some database for later analytics.

import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
from topic_selector import TopicSelector


from CS6381_MW.SubscriberMW import SubscriberMW

from CS6381_MW import discovery_pb2
from enum import Enum

class SubscriberAppln ():

    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        LOOKUPPUB = 3,
        CONNECTPUB = 4
        COMPLETED = 5

    def __init__(self, logger):
        self.state = self.State.INITIALIZE  # state that are we in
        self.name = None  # our name (some unique name)
        self.topiclist = None  # the different topics that we subscribe on
        self.iters = None  # number of iterations of subscription
        self.frequency = None  # rate at which dissemination takes place
        self.num_topics = None  # total num of topics we subscribe
        self.lookup = None  # one of the diff ways we do lookup
        self.dissemination = None  # direct or via broker
        self.mw_obj = None  # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements
        self.influx = None
    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info("SubscriberAppln::configure")

            # set our current state to CONFIGURE state
            self.state = self.State.CONFIGURE

            # initialize our variables
            self.name = args.name  # our name
            self.iters = args.iters  # num of iterations
            self.frequency = args.frequency  # frequency with which topics are disseminated
            self.num_topics = 5  # total num of topics we subscribe

            # Now, get the configuration object
            self.logger.debug("SubscribeAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]

            # Now get our topic list of interest
            self.logger.debug("SubscriberAppln::configure - selecting our topic list")
            ts = TopicSelector()
            self.topiclist = ts.interest(self.num_topics)  # let topic selector give us the desired num of topics

            # Now setup up our underlying middleware object to which we delegate
            # everything
            self.logger.debug("SubscriberAppln::configure - initialize the middleware object")
            self.mw_obj = SubscriberMW(self.logger)
            self.mw_obj.configure(args)  # pass remainder of the args to the m/w object

            self.mw_obj.set_upcall_handle(self)


            self.logger.info("SubscriberAppln::configure - configuration complete")

        except Exception as e:
            raise e

    def driver(self):
        ''' Driver program '''

        try:
            self.logger.info("SubscriberAppln::driver")

            # dump our contents (debugging purposes)
            self.dump()

            self.logger.debug("SubscriberAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle(self)

            self.state = self.State.REGISTER

            self.mw_obj.event_loop(timeout=0)  # start the event loop

            self.logger.info("SubscriberAppln::driver completed")

        except Exception as e:
            raise e

    def invoke_operation(self):
        ''' Invoke operating depending on state  '''

        try:
            self.logger.info("SubscriberAppln::invoke_operation")

            # check what state are we in. If we are in REGISTER state,
            # we send register request to discovery service. If we are in
            # ISREADY state, then we keep checking with the discovery
            # service.
            if (self.state == self.State.REGISTER):
                # send a register msg to discovery service
                self.logger.debug("SubscriberAppln::invoke_operation - register with the discovery service")
                self.mw_obj.register(self.name, self.topiclist)
                return None

            elif (self.state == self.State.LOOKUPPUB):
                self.logger.info("SubscriberAppln::LoopUp the publisher")
                self.mw_obj.lookup_pub(self.topiclist)
                return None

            elif (self.state == self.State.CONNECTPUB):
                self.logger.info("SubscriberAppln::Connect the publisher")
                self.mw_obj.create_database()
                for i in range(self.iters):
                    self.mw_obj.handle_pub_reply()
                    time.sleep(1 / float(self.frequency))
                return None

            elif (self.state == self.State.COMPLETED):

                self.mw_obj.disable_event_loop()
                return None

            else:
                raise ValueError("Undefined state of the appln object")

            self.logger.info("PublisherAppln::invoke_operation completed")

        except Exception as e:
            raise e

    # def LOOKUPPUB(self, topic):
    #     try :
    #         self.logger.info("SubscriberAppln::topic and data received are {}".format(topic))
    #     except Exception as e:
    #         raise e

    def register_response(self, reg_resp):
        ''' handle register response '''

        try:
            self.logger.info("SubscriberAppln::register_response")
            if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
                self.logger.debug("SubscriberAppln::register_response - registration is a success")

                # set our next state to  so that we can then send the  message right away
                self.state = self.State.LOOKUPPUB

                return 0

            else:
                self.logger.debug("PublisherAppln::register_response - registration is a failure with reason {}".format(
                    response.reason))
                raise ValueError("Publisher needs to have unique id")

        except Exception as e:
            raise e

    def lookup_response(self, lookup_resp):
        ''' handle lookup response '''

        try:
            self.logger.info("SubscriberAppln::lookup_response")

            if len(lookup_resp) > 0:
                self.mw_obj.subscribe(self.topiclist)
                self.mw_obj.connect2pub(lookup_resp[0].addr, lookup_resp[0].port)
                self.state = self.State.CONNECTPUB
                self.logger.info("SubscriberAppln::look up pub and connect")
                time.sleep(10)
                return 0

            else:
                self.logger.debug("PublisherAppln::lookup_response - look up is a failure with reason {}".format(
                    response.reason))
                raise ValueError("Publisher needs to have unique id")
        except Exception as e:
            raise e

    def topic_response(self, info):

        try:
            self.logger.info("SubscriberAppln::topic _response")

            self.logger.info("SubscriberAppln::topic_response received data {}".format(info))


            # self.mw_obj.recordTimeStamp(info)

            return 0


        except Exception as e:
            raise e

    def dump(self):
        ''' Pretty print '''

        try:
            self.logger.info("**********************************")
            self.logger.info("SubscriberAppln::dump")
            self.logger.info("------------------------------")
            self.logger.info("     Name: {}".format(self.name))
            self.logger.info("     Lookup: {}".format(self.lookup))
            self.logger.info("     Dissemination: {}".format(self.dissemination))
            self.logger.info("     Num Topics: {}".format(self.num_topics))
            self.logger.info("     TopicList: {}".format(self.topiclist))
            self.logger.info("     Iterations: {}".format(self.iters))
            self.logger.info("     Frequency: {}".format(self.frequency))
            self.logger.info("**********************************")

        except Exception as e:
            raise e

def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Subscriber Application")

    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)

    parser.add_argument("-n", "--name", default="sub", help="Some name assigned to us. Keep it unique per subscriber")

    parser.add_argument("-a", "--addr", default="localhost",
                        help="IP addr of this Subscriber to advertise (default: localhost)")

    parser.add_argument("-p", "--port", type=int, default=5566,
                        help="Port number on which our underlying subscriber ZMQ service runs, default=5577")

    parser.add_argument("-d", "--discovery", default="localhost:5556",
                        help="IP Addr:Port combo for the discovery service, default localhost:5555")

    parser.add_argument("-T", "--num_topics", type=int, choices=range(1, 10), default=1,
                        help="Number of topics to subscribe, currently restricted to max of 9")

    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

    parser.add_argument("-f", "--frequency", type=int, default=1,
                        help="Rate at which topics received: default once a second - use integers")

    parser.add_argument("-i", "--iters", type=int, default=1000,
                        help="number of subscribtion iterations (default: 1000)")

    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO,
                        choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL],
                        help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

    parser.add_argument("-s", "--dissemination", )
    return parser.parse_args()


def main():
    try:
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.info("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("SubscriberAppln")

        # first parse the arguments
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()

        # reset the log level to as specified
        logger.debug("Main: resetting log level to {}".format(args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format(logger.getEffectiveLevel()))

        # Obtain a publisher application
        logger.debug("Main: obtain the subscirber appln object")
        sub_app = SubscriberAppln(logger)

        # configure the object
        logger.debug("Main: configure the subscriber appln object")
        sub_app.configure(args)

        # now invoke the driver program
        logger.debug("Main: invoke the subscriber appln driver")
        sub_app.driver()

    except Exception as e:
        logger.error("Exception caught in main - {}".format(e))
        return


if __name__ == "__main__":

  # set underlying default logging capabilities
  logging.basicConfig (level=logging.DEBUG,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

  main ()