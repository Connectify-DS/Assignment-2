from models import Producer

class ProducerDBMS:
    def __init__(self, conn, cur):
        self.conn = conn
        self.cur=cur

    def create_table(self):
        try:
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS PRODUCERS(
                ID SERIAL PRIMARY KEY NOT NULL,
                TOPIC TEXT NOT NULL);
            """)

            self.conn.commit()
        except:
            self.conn.rollback()

    def register_new_producer_to_topic(self,topic_name):
        try:
            self.cur.execute("""
                INSERT INTO PRODUCERS (TOPIC) 
                VALUES (%s)
                RETURNING ID
            """,(topic_name,))

            producer_id=self.cur.fetchone()[0]
            
            self.conn.commit()

            return producer_id
        except:
            print("Error while registering producer")
            self.conn.rollback()

    def get_producer(self,producer_id):
        try:
            self.cur.execute("""
                SELECT * FROM PRODUCERS
                WHERE ID = %s
            """,(producer_id,))

            try:
                row=self.cur.fetchone()
                if row is None:
                    raise Exception("Invalid Producer Id")
            except Exception as e:
                raise e
            
            return Producer(
                    producer_id=row[0],
                    producer_topic=row[1]
                )
        except Exception as e:
            print(e)
            self.conn.rollback()
