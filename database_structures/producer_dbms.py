from models import Producer
import psycopg2
import sys
sys.path.append("..")
from config import *
import threading

class ProducerDBMS:
    def __init__(self,config):
        self.conn = psycopg2.connect(database = config['DATABASE'], user = config['USER'], password = config['PASSWORD'], 
                                host = config['HOST'], port = config['PORT'])
        self.cur=self.conn.cursor()
        self.lock=threading.Lock()

    def create_table(self):
        try:
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS PRODUCERS(
                ID SERIAL PRIMARY KEY NOT NULL,
                TOPIC TEXT NOT NULL);
            """)

            self.conn.commit()
        except:
            self.conn.rollback()

    def register_new_producer_to_topic(self,topic_name):
        self.lock.acquire()
        try:
            self.cur.execute("""
                INSERT INTO PRODUCERS (TOPIC) 
                VALUES (%s)
                RETURNING ID
            """,(topic_name,))

            producer_id=self.cur.fetchone()[0]
            
            self.conn.commit()
            self.lock.release()
            return producer_id
        except:
            # print("Error while registering producer")
            self.conn.rollback()
            self.lock.release()

    def get_producer(self,producer_id):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT * FROM PRODUCERS
                WHERE ID = %s
            """,(producer_id,))

            try:
                row=self.cur.fetchone()
                if row is None:
                    raise Exception("Invalid Producer Id")
            except Exception as e:
                raise e
            
            p= Producer(
                    producer_id=row[0],
                    producer_topic=row[1]
                )
            self.lock.release()
            return p
        except Exception as e:
            # print(e)
            self.conn.rollback()
            self.lock.release()
