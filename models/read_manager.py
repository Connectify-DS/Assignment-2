import time
import psycopg2
from config import *
import random
from database_structures.health_dbms import HealthDBMS
from models.write_manager import HEALTH_DELAY_THRESHOLD
from myqueue import MyBroker
from in_memory_structures import ConsumerTable, ProducerTable, TopicTable, MessageTable
from database_structures import BrokerDBMS, TopicDBMS_WM, PartitionDMBS, ConsumerDBMS
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

# Note:
# Use MyBroker class from myqueue folder to create, publish, consume, list topics as that class
# requests the broker server. Initialise it with the broker url.


class readManager:
    def __init__(self, config):
        self.ispersistent = config['IS_PERSISTENT']
        self.init_brokers = config['INIT_BROKERS']
        self.curr_port = 1000
        self.rms = config['READ_MANAGER_PORT']
        self.own_port = config["SERVER_PORT"]

        if self.ispersistent:
            engine = create_engine(
                f"postgresql://{config['USER']}:{config['PASSWORD']}@{config['HOST']}:{config['PORT']}/{config['DATABASE']}")
            if not database_exists(engine.url):
                create_database(engine.url)
            if (database_exists(engine.url)):
                print(f"Database {config['DATABASE']} Created/Exists")
            else:
                raise Exception("Database Could not be created")
            self.topic_dbms = TopicDBMS_WM(config)
            self.broker_dbms = BrokerDBMS(config)
            self.partition_dbms = PartitionDMBS(config)
            self.consumer_dbms = ConsumerDBMS(config)
            self.health_logger = HealthDBMS(config)
            self.drop_tables()
            self.create_tables()
        else:
            self.topics = []
            self.topics_offset = {}
            self.topic_consumerid = {}
            self.partition_broker = {}  # Partition -> Broker ID
            self.topic_numPartitions = {}  # Topic -> num_partition
            self.broker_port = {}  # List of id to broker_port
            self.brokerId = []
            self.consumer_topic = {}
            self.offsets = {}  # 2d map consumer_id, partition

            self.num_consumers = 0
            self.num_brokers = 0

        # self.topics = []
        # self.partition_broker = {}
        # self.topic_numPartitions = {}
        #
        # self.broker_port = {}
        #        #offsets of each producer id

        # self.num_consumers = 1
        # self.ispersistent = config['IS_PERSISTENT']

        # HARD CODING BROKERS
        for i in range(self.init_brokers):
            if self.ispersistent:
                self.broker_dbms.add_new_broker(str(self.curr_port))
            else:
                self.brokerId.append(i)
                self.broker_port[i] = self.curr_port
                self.num_brokers += 1

            self.curr_port += 100

        # if self.ispersistent:
        #     # Connect to the database
        #     self.conn = psycopg2.connect(database = config['DATABASE'], user = config['USER'], password = config['PASSWORD'],
        #                         host = config['HOST'], port = config['PORT'])
        #     self.conn.autocommit = True
        # self.cur=self.conn.cursor() 

        #     # Create the tables if they don't exist
        #     self.consumer_table = ConsumerDBMS(self.conn, self.cur)
        #     # self.message_table = MessageDBMS(self.conn, self.cur) In Broker
        #     # self.producer_table = ProducerDBMS(self.conn, self.cur) In Write Manager
        #     # self.topic_table = TopicDBMS(self.conn, self.cur) In Broker

        # else:
        #     # Create the tables if they don't exist in memory
        #     self.consumer_table = ConsumerTable()
        #     # self.message_table = MessageTable()
        #     # self.producer_table = ProducerTable()
        #     # self.topic_table = TopicTable()

    def create_tables(self):
        self.broker_dbms.create_table()
        self.topic_dbms.create_table()
        self.partition_dbms.create_table()
        self.consumer_dbms.create_table()

    def drop_tables(self):
        self.broker_dbms.cur.execute("""
            DROP TABLE IF EXISTS BROKERS, CONSUMERS, PARTITIONS, TOPICS_WM;
        """)

        self.broker_dbms.conn.commit()

    def add_broker(self, port):
        # Handle Metadata of Read Manager -> Do not request broker
        if self.ispersistent:
            port = self.curr_port
            self.curr_port += 100
            broker_id = self.broker_dbms.add_new_broker(str(port))
        else:
            broker_id = self.num_brokers
            self.brokerId.append(broker_id)
            port = self.curr_port
            self.curr_port += 100
            self.broker_port[broker_id] = port

    def add_topic(self, topic_name):
        # Adds topic without initialising any partition
        if self.ispersistent:
            self.topic_dbms.add_topic(topic_name)
        else:
            self.topics.append(topic_name)
            self.topic_consumerid[topic_name] = []
            self.topics_offset[topic_name] = 0
            self.topic_numPartitions[topic_name] = 0


    def add_partition(self, topic_name, partition_name, broker_id):

        # Adds a partition to the topic and assign it to the broker id
        if self.ispersistent:
            partition_id = self.topic_dbms.add_partition(topic_name)
            partition_name = topic_name + "." + str(partition_id)

            self.partition_dbms.add_partition(partition_name, broker_id)
            self.consumer_dbms.add_partition(topic_name, partition_name)
        else:
            self.partition_broker[partition_name] = broker_id
            self.topic_numPartitions[topic_name] += 1

            for i in self.topic_consumerid[topic_name]:
                self.offsets[i][partition_name] = 0

    def list_topics(self):
        """
        Returns a list of all the topics in the system.
        """
        # Return from Metadata of Read Manager
        if self.ispersistent:
            return self.topic_dbms.list_topics()
        else:
            return self.topics


    def register_consumer(self,topic_name, sync=1):
        # Check if Topic Exists. If not Return Error
        # Add to Consumer Table (register_to_topic Function) -> Returns ID
        # Handle Metadata

        # Health Check:
        #       Add new consumer to the healthcheck list with the ID
        #       Save the current time (time.datetime) as the time of creation
        #       You will also have to maintain the last use time (currently empty)
        if topic_name not in self.list_topics():
            raise Exception("Topic doesn't exist")

        if self.ispersistent:
            num_partitions = self.topic_dbms.get_num_partitions(topic_name)
            consumer_id = self.consumer_dbms.add_consumer(
                topic_name, num_partitions)
            self.health_logger.add_update_health_log(
                'consumer', consumer_id, time.time())
        else:
            consumer_id = self.num_consumers
            self.num_consumers += 1
            self.consumer_topic[consumer_id]
            self.topic_consumerid[topic_name].append(consumer_id)
            self.offsets[consumer_id] = {}
            for i in range(1, self.topic_numPartitions[topic_name]+1):
                partition_name = topic_name + "." + str(i)
                self.offsets[consumer_id][partition_name] = 0

        if sync==1:
            MyBroker.register_consumer(topic_name, self.own_port, self.rms)
            self.health_logger.add_update_health_log('broker', self.own_port, time.time())
        return consumer_id


    def consume_message(self, consumer_id, topic_name, sync=1):
        # Check if Topic is subscribed by consumer can publish to the topic.
        # Assign Partition (Round Robin)
        # Retrieve offset : consumer table increase_offset function
        # Call the appropriate broker consume_message function

        # Health Check:
        #       Update the last use time of the consumer based on the consumer id
        if self.ispersistent:
            if not self.consumer_dbms.check_consumer_id(consumer_id):
                raise Exception("Invalid ConsumerId")
            if not self.consumer_dbms.check_consumer_topic_link(consumer_id, topic_name):
                raise Exception("ConsumerId is not subscribed to the topic")

            curr_partition = self.topic_dbms.get_current_partition(
                topic_name)  # round robin
            partition_name = topic_name + "." + str(curr_partition)

            broker_port = self.partition_dbms.get_broker_port_from_partition(
                partition_name)
            offset = self.consumer_dbms.get_offset(consumer_id, partition_name)
            self.health_logger.add_update_health_log(
                'consumer', consumer_id, time.time())
        else:
            try:
                if self.consumer_topic[consumer_id] != topic_name:
                    raise Exception("Topic not subscribed")
            except Exception as e:
                raise e

            # assign partition
            # curr_partition = random.randint(1,self.topic_numPartitions[topic_name])
            curr_partition = self.topics_offset[topic_name] % self.topic_numPartitions[topic_name] + 1
            self.topics_offset[topic_name] = (
                self.topics_offset[topic_name] + 1) % self.topic_numPartitions[topic_name]

            partition_name = topic_name + "." + str(curr_partition)
            curr_id = self.partition_broker[partition_name]
            broker_port = self.broker_port[curr_id]

            offset = self.offsets[consumer_id][partition_name]
            self.offsets[consumer_id][partition_name] += 1
        
        url = "http://127.0.0.1:" + str(broker_port)

        broker_consumer_data = MyBroker.consume_message(url, partition_name, offset, consumer_id, self.own_port, self.rms, sync)
        self.health_logger.add_update_health_log(
            'broker', broker_port, time.time())
        return broker_consumer_data

    def health_check(self):
        # This function will check the last use time of the consumers and log whether
        # any consumer has not produced a message for a long time (set arbitrary threshold for now)
        inactive_consumer = self.health_logger.get_inactive_actors(
            'consumer', HEALTH_DELAY_THRESHOLD)
        inactive_brokers=self.health_logger.get_inactive_actors('broker',HEALTH_DELAY_THRESHOLD)
        if inactive_consumer!=None or len(inactive_consumer)>0:
            print("Consumers who have been inactive for ",HEALTH_DELAY_THRESHOLD, "have the following consumer IDs",inactive_consumer)
        if inactive_brokers!=None or len(inactive_brokers)>0:
            print("Brokers who have been inactive for ",HEALTH_DELAY_THRESHOLD, "have the following brokers IDs",inactive_brokers)
        return inactive_consumer, inactive_brokers
