import psycopg2
import sys
sys.path.append("..")
from config import *
import threading

class Message:
    def __init__(self, message):
        self.message = message

class MessageDBMS:
    def __init__(self,config):
        self.conn = psycopg2.connect(database = config['DATABASE'], user = config['USER'], password = config['PASSWORD'], 
                                host = config['HOST'], port = config['PORT'])
        self.cur=self.conn.cursor()
        self.lock=threading.Lock()

    def create_table(self):
        try:
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS MESSAGES(
                ID SERIAL PRIMARY KEY NOT NULL,
                MESSAGE TEXT NOT NULL);
            """)

            self.conn.commit()
        except:
            self.conn.rollback()

    def add_message(self,message):
        self.lock.acquire()
        try:
            self.cur.execute("""
                INSERT INTO MESSAGES (MESSAGE) 
                VALUES (%s)
                RETURNING ID
            """,(message,))

            message_id=self.cur.fetchone()[0]
            
            self.conn.commit()
            self.lock.release()
            return message_id
        except:
            self.conn.rollback()
            self.lock.release()

    def get_message(self,message_id):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT * FROM MESSAGES
                WHERE ID = %s
            """,(message_id,))

            row=self.cur.fetchone()

            m= Message(
                    message=row[1]
                )
            self.lock.release()
            return m
        except:
            self.conn.rollback()
            self.lock.release()
