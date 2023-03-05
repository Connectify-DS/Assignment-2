# from models import Consumer
import psycopg2
import sys, json
sys.path.append("..")
from config import *
import threading

class ConsumerDBMS:
    def __init__(self,config):
        self.conn = psycopg2.connect(database = config['DATABASE'], user = config['USER'], password = config['PASSWORD'], 
                                host = config['HOST'], port = config['PORT'])
        self.cur=self.conn.cursor()
        self.lock=threading.Lock()

    def create_table(self):
        try:
            self.cur.execute("""
            CREATE TABLE IF NOT EXISTS CONSUMERS(
            ID SERIAL PRIMARY KEY NOT NULL,
            TOPIC TEXT NOT NULL,
            OFSET JSONB NOT NULL);
        """)

            self.conn.commit()
        except:
            self.conn.rollback()

    def add_consumer(self,topic_name,num_partitions):
        self.lock.acquire()
        try:
            data={}
            for i in range(num_partitions):
                pname = topic_name+"."+str(i+1)
                data[pname] = 0
            data = json.dumps(data)
            self.cur.execute("""
                INSERT INTO CONSUMERS (TOPIC,OFSET) 
                VALUES (%s,%s)
                RETURNING ID
            """, (topic_name, data,))

            consumer_id=self.cur.fetchone()[0]
            # print(consumer_id)
            
            self.conn.commit()
            self.lock.release()
            return consumer_id
        except Exception as e:
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS Error: Could not add consumer with topic name: {topic_name}: {str(e)}")
    
    def get_offset(self, consumer_id, partition_name):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT OFSET FROM CONSUMERS
                WHERE ID=%s
            """, (consumer_id,))
            row=self.cur.fetchone()[0]
            x = row[partition_name]+1
            data = {partition_name: x}
            data = json.dumps(data)
            self.cur.execute("""
                UPDATE CONSUMERS
                SET OFSET = OFSET || %s
                WHERE ID=%s
            """,(data, consumer_id,))

            self.conn.commit()
            self.lock.release()
            return x-1
        except Exception as e:
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS Error: Could not get offset of consumer with partition name: {partition_name}: {str(e)}")
    
    def add_partition(self, topic_name, partition_name):
        self.lock.acquire()
        try:
            data = {partition_name: 0}
            data = json.dumps(data)
            self.cur.execute("""
                UPDATE CONSUMERS
                SET OFSET = OFSET || %s
                WHERE TOPIC=%s
            """,(data, topic_name,))

            self.conn.commit()
            self.lock.release()
        except Exception as e:
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS Error: Could not add partition name: {partition_name} for topic name {topic_name}: {str(e)}")

    def get_num_consumers(self):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT COUNT(*) FROM CONSUMERS
            """)
            try:
                row=self.cur.fetchone()
            except Exception as e:
                raise e
            
            if row is None:
                raise Exception("No consumers present in database")
            self.lock.release()
            return row[0]
        except Exception as e:
            # print(e)
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS Error: Could not get no. of consumers: {str(e)}")
    
    def check_consumer_id(self, consumer_id):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM CONSUMERS 
                    WHERE ID = %s)
            """, (consumer_id,))
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
            raise Exception(f"DBMS Error: Could not check consumer_id {consumer_id}: {str(e)}")
    
    def check_consumer_topic_link(self, consumer_id, topic_name):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM CONSUMERS 
                    WHERE ID = %s AND
                    TOPIC = %s)
            """, (consumer_id, topic_name,))
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
            raise Exception(f"DBMS Error: Could not check link between consumer_id {consumer_id} - topic_name {topic_name}: {str(e)}")