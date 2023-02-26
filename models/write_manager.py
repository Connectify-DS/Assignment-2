import psycopg2
from config import *
from broker import broker
from in_memory_structures import ConsumerTable, ProducerTable, TopicTable, MessageTable
from database_structures import ConsumerDBMS, ProducerDBMS, TopicDBMS, MessageDBMS
# TODO:
# Move list_topics register producer and register consumer 
# Write a MyWriteManager class that sends requests to flask server

# Note:
# Use MyBroker class from myqueue folder to create, publish, consume, list topics as that class
# requests the broker server. Initialise it with the broker url. 
class writeManager:
    def __init__(self, config,init_brokers = 1):
        self.partition_broker = {}
        self.topic_numPartitions = {}
        self.broker_port = {}
        self.brokers=[] ## List of instances of MyBroker Class
        self.num_brokers = init_brokers
        self.ispersistent = config['IS_PERSISTENT']

        if self.ispersistent:
            # Connect to the database
            self.conn = psycopg2.connect(database = config['DATABASE'], user = config['USER'], password = config['PASSWORD'], 
                                host = config['HOST'], port = config['PORT'])
            self.cur=self.conn.cursor()

            # Create the tables if they don't exist
            # self.consumer_table = ConsumerDBMS(self.conn, self.cur) In Read Manager
            # self.message_table = MessageDBMS(self.conn, self.cur) In Broker
            self.producer_table = ProducerDBMS(self.conn, self.cur)
            # self.topic_table = TopicDBMS(self.conn, self.cur) In Broker

        else:
            # Create the tables if they don't exist in memory
            # self.consumer_table = ConsumerTable() 
            # self.message_table = MessageTable()
            self.producer_table = ProducerTable()
            # self.topic_table = TopicTable()
        
    def add_broker(self,port):
        ## Note: You will have to request read manager to add this broker too. 
        # port = insert random port
        self.num_brokers += 1
        # Create new broker server here: By Running Flask App
        # Create Instance of MyBroker Class with base url (LOCALHOST:PORT) to send requests
        # new_broker = MyBroker(LOCALHOST:PORT) -> self.brokers.append(new_broker)
        self.broker_port[self.num_brokers] = port

    def add_topic(self, topic_name):
        ## Need to send request to read manager too. 
        # Check if topic already exists
        # Create first partition for the topic by requesting a MyBroker instance (create_topic method)
        # Handle Metadata of Write Manager
        pass

    def add_partition(self,topic_name):
        ## Need to send request to read manager too.
        # Choose a Broker (Round Robin / Random)
        # Create the partition by calling create_topic of MyBroker instance
        pass

    def list_topics(self):
        """
        Returns a list of all the topics in the system.
        """
        # Return from Metadata of Write Manager
        pass

    def register_producer(self,topic_name):
        # Check if Topic Exists. If not Create Topic
        # Add to Producer Table (register_new_producer_to_topic Function) -> Returns ID
        # Handle Metadata

        ## Health Check: 
        #       Add new producer to the healthcheck list with the ID
        #       Save the current time (time.datetime) as the time of creation
        #       You will also have to maintain the last use time (currently empty)
        pass

    def produce_message(self,producer_id,topic_name,message):
        # Check if Producer can publish to the topic.
        # Assign / Create Partition (Round Robin)
        # Call the appropriate broker publish_message function

        ## Health Check: 
        #       Update the last use time of the producer based on the producer id
        pass

    def health_check(self):
        # This function will check the last use time of the producers and log whether 
        # any producer has not produced a message for a long time (set arbitrary threshold for now)
        pass
