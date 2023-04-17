###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Broker application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Broker is involved only when
# the dissemination strategy is via the broker.
#
# A broker is an intermediary; thus it plays both the publisher and subscriber roles
# but in the form of a proxy. For instance, it serves as the single subscriber to
# all publishers. On the other hand, it serves as the single publisher to all the subscribers. 


import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.

# Import our topic selector. Feel free to use alternate way to
# get your topics of interest
from topic_selector import TopicSelector

# Now import our CS6381 Middleware
from CS6381_MW.BrokerMW import BrokerMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2
from enum import Enum

class BrokerAppln():

    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        TRANSFERDATA = 2,
        COMPLETED = 3

    def __init__(self, logger):
        self.state = self.State.INITIALIZE  # state that are we in
        self.entity_num = 0
        self.pub_num = 0
        self.sub_num = 0
        self.name = None  # our name (some unique name)
        self.mw_obj = None  # handle to the underlying Middleware object
        self.logger = logger


    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info("BrokerAppln::configure")

            # set our current state to CONFIGURE state
            self.state = self.State.CONFIGURE

            # initialize our variables
            self.name = args.name  # our name
            self.iters = args.iters  # num of iterations


            # Now, get the configuration object
            self.logger.debug("BrokerAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)

            # Now setup up our underlying middleware object to which we delegate
            # everything
            self.logger.debug("BrokerAppln::configure - initialize the middleware object")
            self.mw_obj = BrokerMW(self.logger)
            self.mw_obj.configure(args)  # pass remainder of the args to the m/w object

            self.logger.info("BrokerxAppln::configure - configuration complete")

        except Exception as e:
            raise e

    def driver(self):
        ''' Driver program '''

        try:
            self.logger.info("BrokerAppln::driver")

            # First ask our middleware to keep a handle to us to make upcalls.
            # This is related to upcalls. By passing a pointer to ourselves, the
            # middleware will keep track of it and any time something must
            # be handled by the application level, invoke an upcall.
            self.logger.debug("BrokerAppln::driver - upcall handle")
            self.mw_obj.transferData()

            # the next thing we should be doing is to register with the Broker
            # service. But because we are simply delegating everything to an event loop
            # that will call us back, we will need to know when we get called back as to
            # what should be our next set of actions.  Hence, as a hint, we set our state
            # accordingly so that when we are out of the event loop, we know what
            # operation is to be performed.  In this case we should be registering with
            # the Broker service. So this is our next state.
            self.state = self.State.TRANSFERDATA

            self.mw_obj.event_loop(timeout=None)  # start the event loop


            self.logger.info("BrokerAppln::driver completed")

        except Exception as e:
            raise e




def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Broker Application")

    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)

    parser.add_argument("-n", "--name", default="pub", help="Some name assigned to us. Keep it unique per Broker")

    parser.add_argument("-a", "--addr", default="localhost",
                        help="IP addr of this Broker to advertise (default: localhost)")

    parser.add_argument("-s", "--sub_port", type=int, default=5590,
                        help="Port number on which our underlying Broker ZMQ service runs, default=5599")

    parser.add_argument("-p", "--pub_port", type=int, default=5589,
                        help="Port number on which our underlying Broker ZMQ service runs, default=5589")

    parser.add_argument("-e", "--bro_id", default="0",
                        help="broker id number, default 0")

    parser.add_argument("-d", "--broker_id", type=int, default=5589,
                        help="IP Addr:Port combo for the discovery service, default localhost:5555")

    parser.add_argument("-T", "--num_topics", type=int, choices=range(1, 10), default=1,
                        help="Number of topics to publish, currently restricted to max of 9")

    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

    parser.add_argument("-f", "--frequency", type=int, default=1,
                        help="Rate at which topics disseminated: default once a second - use integers")

    parser.add_argument("-i", "--iters", type=int, default=1000,
                        help="number of publication iterations (default: 1000)")

    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO,
                        choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL],
                        help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

    return parser.parse_args()



def main():
    try:
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.info("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("BrokerAppln")

        # first parse the arguments
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()

        # reset the log level to as specified
        logger.debug("Main: resetting log level to {}".format(args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format(logger.getEffectiveLevel()))

        # Obtain a Broker application
        logger.debug("Main: obtain the Broker appln object")
        broker_app = BrokerAppln(logger)

        # configure the object
        logger.debug("Main: configure the Broker appln object")
        broker_app.configure(args)

        broker_app.driver()

    except Exception as e:
        logger.error("Exception caught in main - {}".format(e))
        return

if __name__ == "__main__":

  # set underlying default logging capabilities
  logging.basicConfig (level=logging.DEBUG,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


  main ()