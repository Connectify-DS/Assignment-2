import time
from flask import jsonify
import psycopg2
from config import *
# from broker import broker
from in_memory_structures import ConsumerTable, ProducerTable, TopicTable, MessageTable
from database_structures import BrokerDBMS, TopicDBMS_WM, PartitionDMBS, ProducerDBMS, HealthDBMS
from myqueue import MyBroker
import random
import yaml
import os
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


HEALTH_DELAY_THRESHOLD = 5*60  # 5 mins
PARTITION_THRESHOLD = 10


class writeManager:
    def __init__(self, config):
        self.ispersistent = config['IS_PERSISTENT']
        self.init_brokers = config['INIT_BROKERS']
        self.read_manager_ports = config['READ_MANAGER_PORT']

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
            self.producer_dbms = ProducerDBMS(config)
            self.health_logger = HealthDBMS(config)

            self.drop_tables()
            self.create_tables()
        else:
            self.topics = []
            self.topics_offset = {}
            self.partition_broker = {}  # Partition -> Broker ID
            self.topic_numPartitions = {}  # Topic -> num_partition
            self.topic_numMsgs = {}
            self.broker_port = {}  # List of id to broker_port
            self.brokerId = []
            self.producer_topic = {}

            self.num_producers = 0
            self.num_brokers = 0

        self.curr_port = 1000

        # HARD CODING BROKERS
        for i in range(self.init_brokers):
            if self.ispersistent:
                self.broker_dbms.add_new_broker(str(self.curr_port))
            else:
                self.brokerId.append(i)
                self.broker_port[i] = self.curr_port
                self.num_brokers += 1

            self.curr_port += 100

    def create_tables(self):
        self.broker_dbms.create_table()
        self.topic_dbms.create_table()
        self.partition_dbms.create_table()
        self.producer_dbms.create_table()
        self.health_logger.create_table()

    def drop_tables(self):
        self.broker_dbms.cur.execute("""
            DROP TABLE IF EXISTS BROKERS, PRODUCERS, PARTITIONS, TOPICS_WM;
        """)

        self.broker_dbms.conn.commit()


        
    def add_broker(self, port):
        ## Note: You will have to request read manager to add this broker too.
        if self.ispersistent:
            # port = self.curr_port
            self.curr_port += 100
            broker_id = self.broker_dbms.add_new_broker(str(port))
        else:
            broker_id = self.num_brokers
            self.brokerId.append(broker_id)
            # port = self.curr_port
            self.curr_port += 100
            self.broker_port[broker_id] = port
        return broker_id

    def add_topic(self, topic_name):
        # Need to send request to read manager too.
        # Check if topic already exists
        # Create first partition for the topic by requesting a MyBroker instance (create_topic method)
        # Handle Metadata of Write Manager

        if topic_name in self.list_topics():
            raise Exception("Topic already exists")

        if self.ispersistent:
            self.topic_dbms.add_topic(topic_name)

            broker_id, broker_port = self.broker_dbms.get_random_broker()

            partition_name = topic_name + ".1"
            self.partition_dbms.add_partition(partition_name, broker_id)
        else:
            self.topics.append(topic_name)
            self.topics_offset[topic_name] = 0
            self.topic_numPartitions[topic_name] = 0

        MyBroker.add_topic(topic_name, self.read_manager_ports)
        self.health_logger.add_update_health_log('broker', broker_port, time.time())
        return self.add_partition(topic_name)

            
        resp = MyBroker.add_topic(topic_name, self.read_manager_ports)
        self.add_partition(topic_name)
        # print(resp)
        return resp
    
    def add_partition(self, topic_name):
        # Need to send request to read manager too.
        # Choose a Broker (Round Robin / Random)
        # Create the partition by calling create_topic of MyBroker instance

        if self.ispersistent:
            broker_id, broker_port = self.broker_dbms.get_random_broker()
            partition_id = self.topic_dbms.add_partition(topic_name)

            partition_name = topic_name + "." + str(partition_id)

            self.partition_dbms.add_partition(partition_name, broker_id)
        else:
            broker_id = random.choice(self.brokerId)
            broker_port = self.broker_port[broker_id]

            self.topic_numPartitions[topic_name] += 1
            partition_name = topic_name + "." + \
                str(self.topic_numPartitions[topic_name])

            self.partition_broker[partition_name] = broker_id

        url = "http://127.0.0.1:" + str(broker_port)
        resp = MyBroker.create_partition(
            url, topic_name, partition_name, broker_id, self.read_manager_ports)
        self.health_logger.add_update_health_log('broker', broker_port, time.time())
        return resp

    def list_topics(self):
        """
        Returns a list of all the topics in the system.
        """
        # Return from Metadata of Write Manager
        if self.ispersistent:
            return self.topic_dbms.list_topics()
        else:
            return self.topics

    def register_producer(self, topic_name):
        # Check if Topic Exists. If not Create Topic
        # Add to Producer Table (register_new_producer_to_topic Function) -> Returns ID
        # Handle Metadata

        if topic_name not in self.list_topics():
            self.add_topic(topic_name)
        # Health Check:
            #  Add new producer to the healthcheck list with the ID
            #  Save the current time (time.datetime) as the time of creation
            #  You will also have to maintain the last use time (currently empty)
        if self.ispersistent:
            producer_id = self.producer_dbms.add_producer(topic_name)
        else:
            self.num_producers += 1
            self.producer_topic[self.num_producers] = topic_name
            producer_id = self.num_producers
        self.health_logger.add_update_health_log('producer', producer_id, time.time())
        return producer_id

    def produce_message(self, producer_id, topic_name, message):
        # Check if Producer can publish to the topic.
        # Assign / Create Partition (Round Robin)
        # Call the appropriate broker publish_message function

        if self.ispersistent:
            if not self.producer_dbms.check_producer_id(producer_id):
                raise Exception("Invalid ProducerId")
            if not self.producer_dbms.check_producer_topic_link(producer_id, topic_name):
                raise Exception("ProducerId is not subscribed to the topic")

            curr_partition, num_partition, num_msgs = self.topic_dbms.get_current_partition(
                topic_name, 1)  # round robin
            num_msgs += 1
            partition_name = topic_name + "." + str(curr_partition)
            broker_port = self.partition_dbms.get_broker_port_from_partition(
                partition_name)
        else:
            if producer_id not in self.producer_topic:
                raise Exception("Invalid ProducerId")
            if self.producer_topic[producer_id] != topic_name:
                raise Exception("ProducerId is not subscribed to the topic")

            curr_partition = random.randint(
                1, self.topic_numPartitions[topic_name])
            curr_partition = self.topics_offset[topic_name] % self.topic_numPartitions[topic_name] + 1
            self.topics_offset[topic_name] = (
                self.topics_offset[topic_name] + 1) % self.topic_numPartitions[topic_name]
            if topic_name not in self.topic_numMsgs:
                self.topic_numMsgs[topic_name] = 1
            else:
                self.topic_numMsgs[topic_name] += 1

            partition_name = topic_name + "." + str(curr_partition)
            curr_id = self.partition_broker[partition_name]
            broker_port = self.broker_port[curr_id]
            num_partition = self.topic_numPartitions[topic_name]
            num_msgs = self.topic_numMsgs[topic_name]
        url = "http://127.0.0.1:" + str(broker_port)
        resp = MyBroker.publish_message(url, partition_name, message)
        self.health_logger.add_update_health_log('producer', producer_id, time.time())
        self.health_logger.add_update_health_log('broker', broker_port, time.time())
        if (num_msgs/num_partition) > PARTITION_THRESHOLD:
            print(f"Threshold exceeded. Adding new partition for topic {topic_name}")
            prs = self.add_partition(topic_name)
            print(prs['message'])
        
        return resp

    def health_check(self):
        # This function will check the last use time of the producers and log whether
        # any producer has not produced a message for a long time (set arbitrary threshold for now)
        inactive_producers = self.health_logger.get_inactive_actors(
            'producer', HEALTH_DELAY_THRESHOLD)
        inactive_brokers=self.health_logger.get_inactive_actors('broker',HEALTH_DELAY_THRESHOLD)
        if inactive_producers!=None or len(inactive_producers)>0:
            print(f"Producers who have been inactive for {HEALTH_DELAY_THRESHOLD} seconds have the following producer IDs",inactive_producers)
        if inactive_brokers!=None or len(inactive_brokers)>0:
            print(f"Brokers who have been inactive for {HEALTH_DELAY_THRESHOLD} seconds have the following brokers IDs",inactive_brokers)
        return inactive_producers,inactive_brokers
