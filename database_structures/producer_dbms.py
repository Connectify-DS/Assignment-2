import psycopg2
import sys
sys.path.append("..")
from config import *
import threading

class ProducerDBMS:
    def __init__(self,config):
        self.conn = psycopg2.connect(database = config['DATABASE'], user = config['USER'], password = config['PASSWORD'], 
                                host = config['HOST'], port = config['PORT'])
        self.conn.autocommit = True
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

    def add_producer(self, topic_name):
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
        except Exception as e:
            # print("Error while registering producer")
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS Error: Could not add producer with topic name: {topic_name}: {str(e)}")

    def get_num_producers(self):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT COUNT(*) FROM PRODUCERS
            """)
            try:
                row=self.cur.fetchone()
            except Exception as e:
                raise e
            if row is None:
                raise Exception("No producers present in database")
            self.lock.release()
            return row[0]
        except Exception as e:
            # print(e)
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS Error: Could not get no. of producers: {str(e)}")
    
    def check_producer_id(self, producer_id):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM PRODUCERS 
                    WHERE ID = %s)
            """, (producer_id,))
            try:
                row=self.cur.fetchone()
            except Exception as e:
                raise e
            
            if row is None:
                raise Exception("Could not execute query")
            self.lock.release()
            return row[0]
        except Exception as e:
            # print(e)
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS Error: Could not check producer_id {producer_id}: {str(e)}")
    
    def check_producer_topic_link(self, producer_id, topic_name):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM PRODUCERS 
                    WHERE ID = %s AND
                    TOPIC = %s)
            """, (producer_id, topic_name,))
            try:
                row=self.cur.fetchone()
            except Exception as e:
                raise e
            
            if row is None:
                raise Exception("Could not execute query")
            self.lock.release()
            return row[0]
        except Exception as e:
            # print(e)
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS Error: Could not check link between producer_id {producer_id} - topic_name {topic_name}: {str(e)}")