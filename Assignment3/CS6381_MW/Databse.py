from influxdb import InfluxDBClient

class Database():

    self.influx = InfluxDBClient()
    self.influx.create_database('pubsub_results')
    self.influx.switch_database('pubsub_results')
    self.logger.info("PublisherMW::configure completed")