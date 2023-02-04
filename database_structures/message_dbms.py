from models import Message

class MessageDBMS:
    def __init__(self, conn, cur):
        self.conn = conn
        self.cur=cur

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
        try:
            self.cur.execute("""
                INSERT INTO MESSAGES (MESSAGE) 
                VALUES (%s)
                RETURNING ID
            """,(message,))

            message_id=self.cur.fetchone()[0]
            
            self.conn.commit()

            return message_id
        except:
            self.conn.rollback()

    def get_message(self,message_id):
        try:
            self.cur.execute("""
                SELECT * FROM MESSAGES
                WHERE ID = %s
            """,(message_id,))

            row=self.cur.fetchone()

            return Message(
                    message=row[1]
                )
        except:
            self.conn.rollback()
