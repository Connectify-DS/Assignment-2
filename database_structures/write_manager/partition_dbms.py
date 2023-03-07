import psycopg2
import sys
sys.path.append("..")
from config import *
import threading

class PartitionDMBS:
    def __init__(self,config):
        self.conn = psycopg2.connect(database = config['DATABASE'], user = config['USER'], password = config['PASSWORD'], 
                                host = config['HOST'], port = config['PORT'])
        self.conn.autocommit = True
        self.cur=self.conn.cursor()
        self.lock=threading.Lock()

    def create_table(self):
        try:
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS PARTITIONS(
                ID SERIAL PRIMARY KEY NOT NULL,
                BROKER_ID INT NOT NULL,
                NAME TEXT NOT NULL);
            """)

            self.conn.commit()
        except:
            self.conn.rollback()

    def add_partition(self,partition_name,broker_id):
        self.lock.acquire()
        try:
            self.cur.execute("""
                INSERT INTO PARTITIONS (NAME,BROKER_ID)
                VALUES (%s,%s)
                RETURNING ID
            """,(partition_name,broker_id,))

            id=self.cur.fetchone()[0]

            self.conn.commit()
            self.lock.release()
            return id
        except Exception as e:
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS ERROR: Could not add partition {partition_name} to broker {broker_id}: {str(e)}")
        
    def get_broker_port_from_partition(self,partition_name):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT PORT
                FROM BROKERS
                INNER JOIN PARTITIONS
                ON BROKERS.ID = PARTITIONS.BROKER_ID
                WHERE PARTITIONS.NAME=%s
            """,(partition_name,))

            id=self.cur.fetchone()[0]

            self.conn.commit()
            self.lock.release()
            return id
        except Exception as e:
            self.conn.rollback()
            self.lock.release()
            raise Exception(f"DBMS ERROR: Could not find broker port for partition {partition_name}: {str(e)}")
        
        
    