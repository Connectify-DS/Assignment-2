import psycopg2

from in_memory_structures import Message

class MessageDBMS:
    def __init__(self):
        self.conn = psycopg2.connect(database = "mqsdb", user = "postgres", password = "mayank", 
                                host = "127.0.0.1", port = "5432")
        self.cur=self.conn.cursor()

    def create_table(self):
        self.cur.execute("""
            CREATE TABLE MESSAGES(
            ID SERIAL PRIMARY KEY NOT NULL,
            MESSAGE TEXT NOT NULL);
        """)

        self.conn.commit()

    def add_message(self,message):
        self.cur.execute("""
            INSERT INTO MESSAGES (MESSAGE) 
            VALUES (%s)
            RETURNING ID
        """,(message,))

        message_id=self.cur.fetchone()[0]
        
        self.conn.commit()

        return message_id

    def get_message(self,message_id):
        self.cur.execute("""
            SELECT * FROM MESSAGES
            WHERE ID = %s
        """,(message_id,))

        row=self.cur.fetchone()

        return Message(
                message=row[1]
            )

if __name__=="__main__":
    dbms=MessageDBMS()
    dbms.create_table()