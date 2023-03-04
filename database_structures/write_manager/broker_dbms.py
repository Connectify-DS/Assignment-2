import psycopg2
import threading

class BrokerDBMS:
    def __init__(self,config):
        self.conn = psycopg2.connect(database = config['DATABASE'], user = config['USER'], password = config['PASSWORD'], 
                                host = config['HOST'], port = config['PORT'])
        self.cur=self.conn.cursor()
        self.lock=threading.Lock()

    def create_table(self):
        try:
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS BROKERS(
                ID SERIAL PRIMARY KEY NOT NULL,
                PORT TEXT NOT NULL);
            """)

            self.conn.commit()
        except:
            self.conn.rollback()
    
    def add_new_broker(self, port):
        self.lock.acquire()
        try:
            self.cur.execute("""
                INSERT INTO BROKERS (PORT) 
                VALUES (%s)
                RETURNING ID
            """,(port,))

            broker_id=self.cur.fetchone()[0]
            
            self.conn.commit()
            self.lock.release()
            return broker_id
        except:
            # print("Error while registering broker")
            self.conn.rollback()
            self.lock.release()
    
    def get_num_brokers(self):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT COUNT(*) FROM BROKERS
            """)
            try:
                row=self.cur.fetchone()
            except Exception as e:
                raise e
            self.lock.release()
            if row is None:
                raise Exception("No brokers present in database")
            return row[0]
        except Exception as e:
            # print(e)
            self.conn.rollback()
            self.lock.release()

    def get_random_broker(self):
        self.lock.acquire()
        try:
            self.cur.execute("""
                SELECT * FROM BROKERS
                ORDER BY RANDOM() LIMIT 1
            """)

            try:
                row=self.cur.fetchone()
                if row is None:
                    raise Exception("No brokers present in database")
            except Exception as e:
                raise e
            
            id, port = row[0], row[1]
            self.lock.release()
            return id, port
        except Exception as e:
            # print(e)
            self.conn.rollback()
            self.lock.release()

