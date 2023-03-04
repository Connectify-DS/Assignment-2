import psycopg2
from config import *
import random
from myqueue import MyBroker
from in_memory_structures import ConsumerTable, ProducerTable, TopicTable, MessageTable
from database_structures import ConsumerDBMS, ProducerDBMS, TopicDBMS, MessageDBMS
# TODO:
# Move list_topics register producer and register consumer
# Write a MyReadManager class that sends requests to flask server

# Note:
# Use MyBroker class from myqueue folder to create, publish, consume, list topics as that class
# requests the broker server. Initialise it with the broker url. 
class readManager:
    def __init__(self, config):
        self.topics = []
        self.partition_broker = {}
        self.topic_numPartitions = {}
        self.consumer_topic = {}
        self.broker_port = {}
        self.offsets = {}       #offsets of each producer id

        self.num_consumers = 1
        self.ispersistent = config['IS_PERSISTENT']

        ###HARD CODING BROKERS

        if self.ispersistent:
            # Connect to the database
            self.conn = psycopg2.connect(database = config['DATABASE'], user = config['USER'], password = config['PASSWORD'], 
                                host = config['HOST'], port = config['PORT'])
            self.cur=self.conn.cursor()

            # Create the tables if they don't exist
            self.consumer_table = ConsumerDBMS(self.conn, self.cur) 
            # self.message_table = MessageDBMS(self.conn, self.cur) In Broker
            # self.producer_table = ProducerDBMS(self.conn, self.cur) In Write Manager
            # self.topic_table = TopicDBMS(self.conn, self.cur) In Broker

        else:
            # Create the tables if they don't exist in memory
            self.consumer_table = ConsumerTable() 
            # self.message_table = MessageTable()
            # self.producer_table = ProducerTable()
            # self.topic_table = TopicTable()

    def add_topic(self, topic_name):
        # Handle Metadata of Read Manager -> Do not request broker
        self.topics.append(topic_name)
        self.topic_numPartitions[topic_name] = 1

    def add_partition(self, topic_name, partition_name, broker_id):

        # Handle Metadata of Read Manager -> Do not request broker
        # This function may not be useful. 
        self.partition_broker[partition_name] = broker_id
        self.topic_numPartitions[topic_name] += 1



    def list_topics(self):
        """
        Returns a list of all the topics in the system.
        """
        # Return from Metadata of Read Manager
        return self.topics

    def register_consumer(self,topic_name):
        # Check if Topic Exists. If not Return Error
        # Add to Consumer Table (register_to_topic Function) -> Returns ID
        # Handle Metadata

        ## Health Check: 
        #       Add new consumer to the healthcheck list with the ID
        #       Save the current time (time.datetime) as the time of creation
        #       You will also have to maintain the last use time (currently empty)
        try:
            if topic_name not in self.topics:
                raise Exception("Topic doesn't exist")
        except Exception as e:
            raise e
    
        consumer_id = self.num_consumers
        self.num_consumers += 1
        self.consumer_topic[consumer_id]
        self.offsets[consumer_id] = {}
        for i in range(1,self.topic_numPartitions[topic_name]+1):
            partition_name = topic_name + "." + str(i)
            self.offsets[consumer_id][partition_name] = 0


    def consume_message(self,consumer_id,topic_name):
        # Check if Topic is subscribed by consumer can publish to the topic.
        # Assign Partition (Round Robin)
        # Retrieve offset : consumer table increase_offset function
        # Call the appropriate broker consume_message function

        ## Health Check: 
        #       Update the last use time of the consumer based on the consumer id
        try:
            if self.consumer_topic[consumer_id] != topic_name:
                raise Exception("Topic not subscribed")
        except Exception as e:
            raise e

        #assign partition
        partition_no = random.randbytes(1,self.topic_numPartitions[topic_name])
        partition_name = topic_name + "." + partition_no
        curr_id = self.partition_broker[partition_name]
        broker_port = self.broker_port[curr_id]
        url = "https://127.0.0.1:" + str(broker_port)
        offset = self.offsets[consumer_id][partition_name]

        return MyBroker.consume_message(url, partition_name, offset)


    def health_check(self):
        # This function will check the last use time of the consumers and log whether 
        # any consumer has not produced a message for a long time (set arbitrary threshold for now)
        pass
