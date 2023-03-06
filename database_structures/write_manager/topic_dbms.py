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
                NUM_MSGS INT NOT NULL,
                OFSET INT NOT NULL,
                PARTITIONS INT[],
                NUM_PARTITIONS INT NOT NULL);
            """)

            self.conn.commit()
        except Exception as e:
            self.conn.rollback()

    def add_topic(self,topic_name)->int:
        self.lock.acquire()
        try:
            self.cur.execute("""
                INSERT INTO TOPICS_WM (NAME,NUM_MSGS,OFSET,PARTITIONS,NUM_PARTITIONS)
                VALUES (%s,0,0,%s,0)
                RETURNING ID
            """,(topic_name,'{}',))

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
                SELECT PARTITIONS, NUM_PARTITIONS
                FROM TOPICS_WM
                WHERE NAME=%s
            """, (topic_name,))
            partitions, num_partitions = self.cur.fetchone()
            if num_partitions==0:
                pid = 0
            else:
                pid = partitions[num_partitions-1]
            self.cur.execute("""
                UPDATE TOPICS_WM
                SET PARTITIONS = PARTITIONS || %s,
                NUM_PARTITIONS = NUM_PARTITIONS + 1 
                WHERE NAME=%s
            """,(pid+1, topic_name,))

            self.conn.commit()
            self.lock.release()
            return pid
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
        
    def get_current_partition(self,topic_name,x=0):
        self.lock.acquire()
        try:
            # Producer
            if x == 1:
                self.cur.execute("""
                    UPDATE TOPICS_WM
                    SET OFSET = MOD( ( OFSET + 1 ), NUM_PARTITIONS ),
                    NUM_MSGS = NUM_MSGS + 1
                    WHERE NAME=%s
                    RETURNING OFSET, PARTITIONS, NUM_PARTITIONS, NUM_MSGS
                """,(topic_name,))
            # Consumer
            else:
                self.cur.execute("""
                    UPDATE TOPICS_WM
                    SET OFSET = MOD( ( OFSET + 1 ), NUM_PARTITIONS )
                    WHERE NAME=%s
                    RETURNING OFSET, PARTITIONS, NUM_PARTITIONS, NUM_MSGS
                """,(topic_name,))

            offset, partitions, num_partition, num_msgs = self.cur.fetchone()
            offset=(offset-1+num_partition)%num_partition

            self.conn.commit()
            self.lock.release()
            if x == 1:
                return partitions[(offset+1)%num_partition], num_partition, num_msgs
            else:
                return partitions[(offset+1)%num_partition]
        except Exception as e:
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS ERROR: Could not get current partition: {str(e)}")

    def get_num_partitions(self,topic_name):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT NUM_PARTITIONS FROM TOPICS_WM
                WHERE NAME=%s
            """,(topic_name,))

            num_partition = self.cur.fetchone()[0]

            self.conn.commit()
            self.lock.release()
            return num_partition
        except Exception as e:
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS ERROR: Could not get current partition: {str(e)}")

