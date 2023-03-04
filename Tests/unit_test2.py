"""
Test for then DB Structure for Consumers.
"""
from database_structures import *
from config import *
import psycopg2

conn = psycopg2.connect(database = DATABASE, user = USER, password = PASSWORD, 
                                host = HOST, port = PORT)
cur=conn.cursor()

def create_tables():
    consumer_dbms=ConsumerDBMS(conn, cur)
    consumer_dbms.create_table()

    producer_dbms=ProducerDBMS(conn, cur)
    producer_dbms.create_table()

    message_dbms=MessageDBMS(conn, cur)
    message_dbms.create_table()

    topic_dbms=TopicDBMS(conn, cur)
    topic_dbms.create_table()

def drop_tables():
    
    cur.execute("""
        DROP TABLE IF EXISTS CONSUMERS, PRODUCERS, MESSAGES, TOPICS;
    """)

    conn.commit()

def test_consumer_dbms():
    consumer_dbms=ConsumerDBMS(conn, cur)
    id=consumer_dbms.register_to_topic("INFO")
    print("Consumer ID: ",id)

    consumer=consumer_dbms.get_consumer(id)
    print("Consumer Topic: ",consumer.topic_name)

def test_producer_dbms():
    producer_dbms=ProducerDBMS(conn, cur)
    id=producer_dbms.register_new_producer_to_topic("INFO")
    print("Producer ID: ",id)

    producer=producer_dbms.get_producer(id)
    print("Producer Topic: ",producer.producer_topic)

def test_message_dbms():
    message_dbms=MessageDBMS(conn, cur)
    id=message_dbms.add_message("THIS IS A TEST MESSAGE")
    print("Message ID: ",id)

    message=message_dbms.get_message(id)
    print("Message: ",message.message)

def test_topic_dbms():
    topic_dbms=TopicDBMS(conn, cur)
    id1=topic_dbms.create_topic_queue("INFO")
    print("Topic ID: ",id1)

    # Creating topic with same name again is not allowed
    try:
        id=topic_dbms.create_topic_queue("INFO")
        print("Topic ID: ",id)
    except:
        print("Caught Excaption")
        topic_dbms.conn.rollback()
    
    id2=topic_dbms.create_topic_queue("DEBUG")
    print("Topic ID: ",id2)

    print("Topic List:")
    for val in topic_dbms.get_topic_list():
        print(val)
    
    topic_queue=topic_dbms.get_topic_queue("INFO")
    print("Topic Obtained: ",topic_queue.topic_name)

def test_topic_queue_dbms():
    topic_dbms=TopicDBMS(conn, cur)
    topic_queue=topic_dbms.get_topic_queue("INFO")
    
    topic_queue.enqueue(0)
    topic_queue.enqueue(1)
    topic_queue.enqueue(2)

    print("Get top message: ",topic_queue.get_at_offset(1))

    sz=topic_queue.size()
    print("Queue Size: ",sz)

    print("Get last message: ",topic_queue.get_at_offset(sz))



if __name__=="__main__":
    drop_tables()
    create_tables()

    test_consumer_dbms()
    test_producer_dbms()
    test_message_dbms()
    test_topic_dbms()
    test_topic_queue_dbms()
