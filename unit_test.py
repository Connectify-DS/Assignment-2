from database_structures import *
import psycopg2


def create_tables():
    consumer_dbms=ConsumerDBMS()
    consumer_dbms.create_table()

    producer_dbms=ProducerDBMS()
    producer_dbms.create_table()

    message_dbms=MessageDBMS()
    message_dbms.create_table()

    topic_dbms=TopicDBMS()
    topic_dbms.create_table()

def drop_tables():
    conn = psycopg2.connect(database = "mqsdb", user = "postgres", password = "mayank", 
                                host = "127.0.0.1", port = "5432")
    cur=conn.cursor()

    cur.execute("""
        DROP TABLE CONSUMERS, PRODUCERS, MESSAGES, TOPICS;
    """)

    conn.commit()

def test_consumer_dbms():
    consumer_dbms=ConsumerDBMS()
    id=consumer_dbms.register_to_topic("INFO")
    print("Consumer ID: ",id)

    consumer=consumer_dbms.get_consumer(id)
    print("Consumer Topic: ",consumer.topic_name)

def test_producer_dbms():
    producer_dbms=ProducerDBMS()
    id=producer_dbms.register_new_producer_to_topic("INFO")
    print("Producer ID: ",id)

    producer=producer_dbms.get_producer(id)
    print("Producer Topic: ",producer.producer_topic)

def test_message_dbms():
    message_dbms=MessageDBMS()
    id=message_dbms.add_message("THIS IS A TEST MESSAGE")
    print("Message ID: ",id)

    message=message_dbms.get_message(id)
    print("Message: ",message.message)



if __name__=="__main__":
    drop_tables()
    create_tables()

    # test_consumer_dbms()
    # test_producer_dbms()
    # test_message_dbms()
