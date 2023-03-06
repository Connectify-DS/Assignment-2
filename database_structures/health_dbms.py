import threading
import time
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
            add_update_result = self.cur.fetchone()

            self.conn.commit()
            self.lock.release()
            return add_update_result
        except:
            self.conn.rollback()
            self.lock.release()

    def get_last_active_time_stamp(self, type, actorid):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT LASTUPDATEDTIME FROM HEALTHLOG
                WHERE TYPE = %s AND ACTORID = %s
            """, (type,actorid,))

            row = self.cur.fetchone()
            last_active_timestamp = None
            if row is not None:
                last_active_timestamp = row[0]
            self.lock.release()
            return last_active_timestamp
        except:
            self.conn.rollback()
            self.lock.release()

    def get_inactive_actors(self,type,timedelta_threshold):
        self.lock.acquire()
        current_time = time.time()
        try:
            #Fetch. all the actors with time less than current_time-timedelta_threshold
            self.cur.execute("""
                SELECT ACTORID FROM HEALTHLOG
                WHERE TYPE = %s AND LASTUPDATEDTIME < (%s)
            """, (type,current_time-timedelta_threshold,))

            inactive_actors = []
            for row in self.cur.fetchall():
                inactive_actors.append(row[0])
            self.lock.release()
            return inactive_actors
        except:
            self.conn.rollback()
            self.lock.release()