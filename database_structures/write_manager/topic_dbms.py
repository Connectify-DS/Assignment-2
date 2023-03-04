import psycopg2
import sys
sys.path.append("..")
from config import *
import threading

class TopicDBMS_WM:
    def __init__(self,config):
        self.conn = psycopg2.connect(database = config['DATABASE'], user = config['USER'], password = config['PASSWORD'], 
                                host = config['HOST'], port = config['PORT'])
        self.cur=self.conn.cursor()
        self.lock=threading.Lock()

    def create_table(self):
        try:
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS TOPICS_WM(
                ID SERIAL UNIQUE NOT NULL,
                NAME TEXT PRIMARY KEY NOT NULL,
                OFSET INT NOT NULL,
                NUM_PARTITIONS INT NOT NULL);
            """)

            self.conn.commit()
        except Exception as e:
            self.conn.rollback()

    def add_topic(self,topic_name)->int:
        self.lock.acquire()
        try:
            self.cur.execute("""
                INSERT INTO TOPICS_WM (NAME,OFSET,NUM_PARTITIONS)
                VALUES (%s,0,1)
                RETURNING ID
            """,(topic_name,))

            id=self.cur.fetchone()[0]

            self.conn.commit()
            self.lock.release()
            return id
        except Exception as e:
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS ERROR: Could not add topic: {topic_name}: {str(e)}")

    def add_partition(self,topic_name):
        self.lock.acquire()
        try:
            self.cur.execute("""
                UPDATE TOPICS_WM
                SET NUM_PARTITIONS = NUM_PARTITIONS + 1 
                WHERE NAME=%s
                RETURNING NUM_PARTITIONS
            """,(topic_name,))

            num_partitions=self.cur.fetchone()[0]
            if num_partitions==None:
                raise Exception("DBMS ERROR: Invalid Topic Name")

            self.conn.commit()
            self.lock.release()
            return num_partitions
        except Exception as e:
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS ERROR: Could not add partition {topic_name}: {str(e)}")

    def list_topics(self):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT NAME FROM TOPICS_WM
            """)

            row = self.cur.fetchall()
            topics = []
            for val in row:
                topics.append(val[0])

            self.conn.commit()
            self.lock.release()
            return topics
        except Exception as e:
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS ERROR: Could not list topics: {str(e)}")
        
    def get_current_partition(self,topic_name):
        self.lock.acquire()
        try:
            self.cur.execute("""
                UPDATE TOPICS_WM
                SET OFSET = MOD( ( OFSET + 1 ), NUM_PARTITIONS )
                WHERE NAME=%s
                RETURNING OFSET, NUM_PARTITIONS
            """,(topic_name,))

            offset,num_partition = self.cur.fetchone()
            offset=(offset-1+num_partition)%num_partition

            self.conn.commit()
            self.lock.release()
            return offset+1
        except Exception as e:
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS ERROR: Could not get current partition: {str(e)}")

    