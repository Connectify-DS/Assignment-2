from models import Message

class MessageTable:
    """
    This class is a simple in-memory database for storing messages.
    """
    def __init__(self):
        self.message_table_entries = {}
        self.last_id = 0

    def get_new_id_auto_inc(self):
        """
        Auto-incrementing ID for messages.
        """
        self.last_id += 1
        return self.last_id

    def get_message(self, message_id):
        """
        Get a message by ID.
        """
        return self.message_table_entries[message_id]

    def add_message(self, message):
        """
        Add a message to the database with a new ID.
        """
        message_id = self.get_new_id_auto_inc()
        self.message_table_entries[message_id] = Message(message)
        return message_id
