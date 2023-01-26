import psycopg2

from in_memory_structures import Producer

class ProducerDBMS:
    def __init__(self):
        self.conn = psycopg2.connect(database = "mqsdb", user = "postgres", password = "mayank", 
                                host = "127.0.0.1", port = "5432")
        self.cur=self.conn.cursor()

    def create_table(self):
        self.cur.execute("""
            CREATE TABLE PRODUCERS
            ID INT PRIMARY KEY NOT NULL,
            TOPIC TEXT NOT NULL,
        """)

        self.conn.commit()

    def create_producer(self,topic_name):
        self.cur.execute("""
            INSERT INTO PRODUCERS (TOPIC) 
            VALUES (%s)
        """,topic_name)

        id=self.cur.fetchone()[0]
        
        self.conn.commit()

        return id

    def select_producer(self,producer_id):
        self.cur.execute("""
            SELECT * FROM PRODUCERS
            WHERE ID = %s
        """,producer_id)

        row=self.cur.fetchone()

        return Producer(
                producer_id=row[0],
                producer_topic=row[1]
            )

if __name__=="__main__":
    dbms=ProducerDBMS()
    dbms.create_table()