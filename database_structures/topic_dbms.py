import psycopg2
import sys
sys.path.append("..")
from config import *
import threading

class TopicDBMS:
    def __init__(self, conn, cur,lock):
        self.conn = psycopg2.connect(database = DATABASE, user = USER, password = PASSWORD, 
                                host = HOST, port = PORT)
        self.cur=self.conn.cursor()
        self.lock=threading.Lock()

    def create_table(self):
        try:
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS TOPICS(
                ID SERIAL PRIMARY KEY NOT NULL,
                NAME TEXT NOT NULL UNIQUE,
                MESSAGES INT[]);
            """)

            self.conn.commit()
        except:
            self.conn.rollback()

    def create_topic_queue(self,name):
        self.lock.acquire()
        try:
            self.cur.execute("""
                INSERT INTO TOPICS (NAME) 
                VALUES (%s)
                RETURNING ID
            """,(name,))

            id=self.cur.fetchone()[0]
            
            self.conn.commit()
            self.lock.release()
            return id
        except:
            self.conn.rollback()
            self.lock.release()

    def get_topic_list(self):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT NAME FROM TOPICS
            """)

            row = self.cur.fetchall()
            topics = []
            for val in row:
                topics.append(val[0])
            self.lock.release()
            return topics
        except:
            self.conn.rollback()
            self.lock.release()

    def get_topic_queue(self,topic_name):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT * FROM TOPICS
                WHERE NAME = %s
            """,(topic_name,))

            row=self.cur.fetchone()
            # print("In Get Topic Queue, Topic Name = ",row)
            
            self.lock.release()
            tq= TopicQueueDBMS(
                topic_name=row[1],
                cur=self.cur,
                conn=self.conn,
                lock=self.lock
            )
            
            return tq
        except:
            self.conn.rollback()
            self.lock.release()

class TopicQueueDBMS:
    def __init__(self, topic_name,cur,conn,lock):
        self.topic_name = topic_name
        self.cur=cur
        self.conn=conn
        self.lock=lock

    def enqueue(self, message):
        sz=self.size()
        self.lock.acquire()
        try:
            if sz==0:
                self.cur.execute("""
                    UPDATE TOPICS 
                    SET MESSAGES = ARRAY[%s]
                    WHERE NAME=%s
                """,(message,self.topic_name,))
            else:
                self.cur.execute("""
                    UPDATE TOPICS 
                    SET MESSAGES = ARRAY_APPEND(MESSAGES,%s) 
                    WHERE NAME=%s
                """,(message,self.topic_name,))

            self.conn.commit()
            self.lock.release()
        except:
            self.conn.rollback()
            self.lock.release()
        

    def get_at_offset(self, offset):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT MESSAGES[%s] FROM TOPICS 
                WHERE NAME=%s
            """,(str(offset),self.topic_name,))
            
            r= self.cur.fetchone()
            self.lock.release()
            return r
        except:
            self.conn.rollback()
            self.lock.release()

    def size(self):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT MESSAGES FROM TOPICS 
                WHERE NAME=%s
            """,(self.topic_name,))

            row=self.cur.fetchone()
            # print(row[0])
            
            if row[0]==None:
                res= 0
            else:
                res= len(row[0])
            self.lock.release()
            return res
        except:
            self.conn.rollback()
            self.lock.release()
