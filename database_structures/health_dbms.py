import threading
from config import *
import psycopg2
import sys
sys.path.append("..")


class HealthDBMS:
    def __init__(self, config):
        self.conn = psycopg2.connect(database=config['DATABASE'], user=config['USER'], password=config['PASSWORD'],
                                     host=config['HOST'], port=config['PORT'])
        self.cur = self.conn.cursor()
        self.lock = threading.Lock()

    def create_table(self):
        try:
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS HEALTHLOG (
                TYPE VARCHAR(20) NOT NULL,
                ACTORID SERIAL NOT NULL,
                LASTUPDATEDTIME DATETIME NOT NULL,
                PRIMARY KEY (TYPE, ACTORID));
            """)

            self.conn.commit()
        except:
            self.conn.rollback()

    def add_update_health_log(self, type, actorid, last_updated_time):
        self.lock.acquire()
        try:
           self.cur.execute("""
            INSERT INTO HEALTHLOG (TYPE, ACTORID, LASTUPDATEDTIME)
            VALUES (%s, %s, %s)
            ON CONFLICT (TYPE, ACTORID)
            DO UPDATE SET LASTUPDATEDTIME = excluded.LASTUPDATEDTIME;
            """, (type, actorid, last_updated_time))
           message_id = self.cur.fetchone()[0]

           self.conn.commit()
           self.lock.release()
           return message_id
        except:
            self.conn.rollback()
            self.lock.release()

    def get_health_log(self, message_id):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT * FROM MESSAGES
                WHERE ID = %s
            """, (message_id,))

            row = self.cur.fetchone()

            m = Message(
                message=row[1]
            )
            self.lock.release()
            return m
        except:
            self.conn.rollback()
            self.lock.release()