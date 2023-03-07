import sys
import psycopg2
import requests
from database_structures.health_dbms import HealthDBMS
from in_memory_structures import TopicTable, MessageTable
from database_structures import TopicDBMS, MessageDBMS

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

# Make Different Database for Each Server: Send a config object


class Broker:
    """
    This class is the main class of the system. It is responsible for the creation of topics,
    registering producers and consumers, and the enqueueing and dequeueing of messages.
    """

    def __init__(self,config):
        self.persistent=config['IS_PERSISTENT']
        if config['IS_PERSISTENT']:
            engine = create_engine(
                f"postgresql://{config['USER']}:{config['PASSWORD']}@{config['HOST']}:{config['PORT']}/{config['DATABASE']}")
            if not database_exists(engine.url):
                create_database(engine.url)
            if (database_exists(engine.url)):
                print(f"Database {config['DATABASE']} Created/Exists")
            else:
                raise Exception("Database Could not be created")
            # Connect to the database

            self.conn = psycopg2.connect(database = config['DATABASE'], user = config['USER'], password = config['PASSWORD'], 
                                host = config['HOST'], port = config['PORT'])
            self.conn.autocommit = True
            self.cur=self.conn.cursor()

            # Create the tables if they don't exist
            self.message_table = MessageDBMS(config)
            self.topic_table = TopicDBMS(config)
            self.health_logger = HealthDBMS(config)
        else:
            # Create the tables if they don't exist in memory
            self.message_table = MessageTable()
            self.topic_table = TopicTable()
        
        wm_url = "http://127.0.0.1:" + str(config["WRITE_MANAGER_PORT"]) +  "/broker"
        data = {"port" : config["SERVER_PORT"]}
        r = None

        try:
            
            r = requests.post(wm_url, json = data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print ("Http Error:",errh)
        except requests.exceptions.ConnectionError as errc:
            print ("Error Connecting:",errc)

        if r is None:
            print(f"Null Response")

        response = r.json()

        if response["status"] == "success":
            print("Broker Listed")

    def reset_dbms(self):
        self.cur.execute("""
            DROP TABLE IF EXISTS MESSAGES, TOPICS;
        """)

        self.conn.commit()

        self.message_table.create_table()
        self.topic_table.create_table()

        self.conn.commit()

    def create_topic(self, topic_name: str):
        """
        Creates a new topic with the given name.
        """
        try:
            topics = self.list_topics()
            if topic_name in topics:
                raise Exception("Topic already exists")
            return self.topic_table.create_topic_queue(topic_name)
        except Exception as e:
            raise e

    def list_topics(self):
        """
        Returns a list of all the topics in the system.
        """
        return self.topic_table.get_topic_list()

    def enqueue(self, topic_name: str, message: str):
        """
        Enqueues a new message to the given topic.
        """
        try:
            topics = self.list_topics()
            # print(topics, topic_name)
            if topics is None:
                raise Exception("No topic registered")
            if topic_name not in topics:
                raise Exception("Topic does not exist")
        except Exception as e:
            raise e

        try:
            topic_queue = self.topic_table.get_topic_queue(topic_name)
            if topic_queue is None:
                raise Exception(f"Topic name {topic_name} not found")
            message_id = self.message_table.add_message(message)
            topic_queue.enqueue(message_id)
        except Exception as e:
            raise e

    def dequeue(self, topic_name: str, offset: int):
        """
        Removes the next message from the given topic.
        """
        try:
            # This is redundant as when we get the topic queue the DBMS will look
            # at all the topics and if it does not exist there will be an error
            # topics = self.list_topics()
            # if topics is None:
            #     raise Exception("No topic registered in this broker")
            # if topic_name not in topics:
            #     raise Exception("Topic does not exist in this broker")

            topic_queue = self.topic_table.get_topic_queue(topic_name)
            if topic_queue is None:
                raise Exception(f"Topic name {topic_name} not found")

            size_rem = topic_queue.size() - offset
            if size_rem <= 0:
                raise Exception("No messages left to retrieve")

            if self.persistent:
                message_id = topic_queue.get_at_offset(offset+1)
            else:
                message_id = topic_queue.get_at_offset(offset)
            if message_id:
                message_data = self.message_table.get_message(message_id)
                return message_data
            else:
                raise Exception("Could not retrieve message")
        except Exception as e:
            raise e
