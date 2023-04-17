###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Discovery application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Discovery service is a server
# and hence only responds to requests. It should be able to handle the register,
# is_ready, the different variants of the lookup methods. etc.
#
# The key steps for the discovery application are
# (1) parse command line and configure application level parameters. One
# of the parameters should be the total number of publishers and subscribers
# in the system.
# (2) obtain the discovery middleware object and configure it.
# (3) since we are a server, we always handle events in an infinite event loop.
# See publisher code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it
# (4) Some data structure or in-memory database etc will need to be used to save
# the registrations.
# (5) When all the publishers and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to is_ready method. Until then
# it will be false.


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
from CS6381_MW.DiscoveryMW import DiscoveryMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2
from enum import Enum

class DiscoveryAppln():
    # these are the states through which our publisher appln object goes thru.
    # We maintain the state so we know where we are in the lifecycle and then
    # take decisions accordingly
    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        ELECTION = 2,
        GETENTITY = 3,
        LOOKUP = 5,
        SENDTOBROKER = 6,
        COMPLETED = 7
    ########################################
    # constructor
    ########################################
    def __init__(self, logger):
        self.state = self.State.INITIALIZE  # state that are we in
        self.entity_num = 0
        self.pub_num = 0
        self.sub_num = 0
        self.name = None  # our name (some unique name)
        self.mw_obj = None  # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements
        self.pub_info = None
        self.sub_info = None
        self.infoToSub = None
    ########################################
    # configure/initialize
    ########################################
    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info("DiscoveryAppln::configure")

            # set our current state to CONFIGURE state
            self.state = self.State.CONFIGURE

            # initialize our variables
            self.name = args.name  # our name
            self.iters = args.iters  # num of iterations

            self.pub_info = []
            self.sub_info = []

            # Now, get the configuration object
            self.logger.debug("DiscoveryAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)

            # Now setup up our underlying middleware object to which we delegate
            # everything
            self.logger.debug("DiscoveryAppln::configure - initialize the middleware object")
            self.mw_obj = DiscoveryMW(self.logger)
            self.mw_obj.configure(args)  # pass remainder of the args to the m/w object

            self.infoToSub = []
            self.logger.info("DiscoveryxAppln::configure - configuration complete")

        except Exception as e:
            raise e

    # def do_election(self):
    #     try:
    #         self.mw_obj.participant_election()
    #
    #     except Exception as e:
    #         raise e

    def register_response(self, reg_resp):
        ''' handle register response '''

        try:
            self.logger.info("DiscoveryAppln::register_response")
            if (reg_resp.role == discovery_pb2.ROLE_PUBLISHER):
                self.logger.info("DiscoveryAppln::register_response - registration is success")
                self.entity_num += 1
                self.pub_num += 1
                self.pub_info.append(reg_resp)
                # self.mw_obj.reply(self.name, "Register Successfully")
                # return a timeout of zero so that the event loop in its next iteration will immediately make
                # an upcall to us
                self.state = self.State.GETENTITY
                return 0
            elif (reg_resp.role == discovery_pb2.ROLE_SUBSCRIBER):
                self.entity_num += 1
                self.sub_num += 1
                self.sub_info.append(reg_resp)
                # self.mw_obj.reply(self.name, "Register Successfully")
                self.state = self.State.GETENTITY
                return 0
            else:
                self.logger.info("PublisherAppln::register_response - registration is a failure with reason {}".format(response.reason))
                raise ValueError("Publisher needs to have unique id")

        except Exception as e:
            raise e

    ########################################
    # driver program
    ########################################
    def driver(self):
        ''' Driver program '''

        try:
            self.logger.info("DiscoveryAppln::driver")

            # dump our contents (debugging purposes)
            self.dump()

            # First ask our middleware to keep a handle to us to make upcalls.
            # This is related to upcalls. By passing a pointer to ourselves, the
            # middleware will keep track of it and any time something must
            # be handled by the application level, invoke an upcall.
            self.logger.debug("DiscoveryAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle(self)


            self.state = self.State.GETENTITY
            # As a rule, whenever we expect a reply from remote entity, we set timeout to
            # None or some large value, but if we want to send a request ourselves right away,
            # we set timeout is zero.
            #
            self.mw_obj.event_loop()  # start the event loop

            self.logger.info("DiscoveryAppln::driver completed")

        except Exception as e:
            raise e


    def isready_response(self, isready_resp):
        ''' handle isready response '''

        try:
            self.logger.info("DiscoveryAppln::isready_response")

            # self.mw_obj.is_ready_reply()
            # discovery service is not ready yet
            self.logger.debug("DiscoveryAppln::driver - Not ready yet; check again")
            time.sleep(10)  # sleep between calls so that we don't make excessive calls

            # else:
            #     # we got the go ahead
            #     # set the state to disseminate
            self.state = self.State.ISREADY

            # self.state = self.State.COMPLETED
            # return timeout of 0 so event loop calls us back in the invoke_operation
            # method, where we take action based on what state we are in.
            return 0

        except Exception as e:
            raise e

    def lookup_response(self, req):
        try:
            self.logger.info("DiscoveryAppln::lookup_response")

            #need write function to find the topic publisher info --- TODO
            for info in req.topiclist:
                self.logger.info("DiscoveryAppln::sub topic {}".format(info))
                for pub in self.pub_info:
                    self.logger.info("DiscoveryAppln::pub info {}".format(pub))
                    if info in pub.topiclist:
                        if pub.info not in self.infoToSub:
                            reg_info = discovery_pb2.RegistrantInfo()
                            reg_info.id = pub.info.id
                            reg_info.addr = pub.info.addr
                            reg_info.port = pub.info.port
                            self.infoToSub.append(reg_info)

            self.logger.info("DiscoveryAppln::list is {}".format(self.infoToSub))


            # self.mw_obj.is_ready_reply()
            # discovery service is not ready yet
            self.logger.debug("DiscoveryAppln::driver - Not ready yet; check again")
            time.sleep(10)  # sleep between calls so that we don't make excessive calls

            # else:
            #     # we got the go ahead
            #     # set the state to disseminate
            self.state = self.State.LOOKUP

            # self.state = self.State.COMPLETED
            # return timeout of 0 so event loop calls us back in the invoke_operation
            # method, where we take action based on what state we are in.
            return 0

        except Exception as e:
            raise e


    def invoke_operation(self):
        ''' Invoke operating depending on state  '''

        try:
            self.logger.info("DiscoveryAppln::invoke_operation")

            if (self.state == self.State.GETENTITY):


                self.logger.debug("DiscoveryAppln::invoke_operation - get pub and sub")

                #send the reply to sub/pub
                self.mw_obj.reply(self.name, "Register Successfully")

                self.logger.debug("DiscoveryAppln::invoke_operation - Reply the sub/ pub")
                #
                # # we are done. So we move to the completed state
                # self.state = self.State.ISREADY

                # return a timeout of zero so that the event loop sends control back to us right away.
                return None

            # elif (self.state == self.State.ISREADY):
            #
            #     self.logger.debug("DiscoveryAppln::invoke_operation - check if are ready to go")
            #     self.mw_obj.is_ready_reply()  # send the is_ready? request
            #
            #     # Remember that we were invoked by the event loop as part of the upcall.
            #     # So we are going to return back to it for its next iteration. Because
            #     # we have just now sent a isready request, the very next thing we expect is
            #     # to receive a response from remote entity. So we need to set the timeout
            #     # for the next iteration of the event loop to a large num and so return a None.
            #     return None

            elif(self.state == self.State.LOOKUP):
                self.logger.debug("DiscoveryAppln::invoke_operation - lookup response")
                self.mw_obj.lookup_reply(self.infoToSub)
                self.infoToSub = []
                return None

            # elif(self.state == self.State.SENDTOBROKER):
            #     self.mw_obj.sendToBroker(pub_info=self.pub_info)
            #     return None

            elif (self.state == self.State.COMPLETED):

                # we are done. Time to break the event loop. So we created this special method on the
                # middleware object to kill its event loop
                self.mw_obj.disable_event_loop()
                return None

            else:
                raise ValueError("Undefined state of the appln object")

        except Exception as e:
            raise e


    def dump(self):
        ''' Pretty print '''

        try:
            self.logger.info("**********************************")
            self.logger.info("PublisherAppln::dump")
            self.logger.info("------------------------------")
            self.logger.info("     Name: {}".format(self.name))
            self.logger.info("     Entity Nums: {}".format(self.entity_num))
            self.logger.info("     Iterations: {}".format(self.iters))
            self.logger.info("**********************************")

        except Exception as e:
            raise e

def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Discovery Application")

    parser.add_argument("-n", "--name", default="dis", help="The name of the discovery")

    parser.add_argument("-a", "--addr", default="localhost",
                        help="IP addr of this publisher to advertise (default: localhost)")

    parser.add_argument("-p", "--port", type=int, default=5576,
                        help="Port number on which our underlying publisher ZMQ service runs, default=5577")

    parser.add_argument("-d", "--discovery", default="localhost:5557",
                        help="IP Addr:Port combo for the discovery service, default localhost:5555")

    parser.add_argument("-e", "--disc_id", default="0",
                        help="discovery id number, default 0")

    parser.add_argument("-t", "--num_entity", type=int, choices=range(0, 5), default=0,
                        help="Number of entities registered")

    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

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
        logger = logging.getLogger("DiscoveryAppln")

        # first parse the arguments
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()

        # reset the log level to as specified
        logger.debug("Main: resetting log level to {}".format(args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format(logger.getEffectiveLevel()))

        # Obtain a publisher application
        logger.debug("Main: obtain the publisher appln object")
        dis_app = DiscoveryAppln(logger)

        # configure the object
        logger.debug("Main: configure the publisher appln object")
        dis_app.configure(args)
        # dis_app.do_election()
        # now invoke the driver program
        logger.debug("Main: invoke the publisher appln driver")
        dis_app.driver()

    except Exception as e:
        logger.error("Exception caught in main - {}".format(e))
        return


###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":
    # set underlying default logging capabilities
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    main()
