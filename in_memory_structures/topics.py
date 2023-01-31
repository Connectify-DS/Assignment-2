class TopicTable:
    def __init__(self):
        self.topic_queues = {}
        self.last_id = 0

    def get_topic_queue(self, topic_name):
        """
        Get the topic queue for the given topic name. If the topic queue does not exist, create it.
        """
        return self.topic_queues[topic_name]

    def get_topic_list(self):
        """
        Get a list of all the topic names.
        """
        return list(self.topic_queues.keys())

    def create_topic_queue(self, topic_name):
        """
        Create a topic queue for the given topic name.
        """
        self.topic_queues[topic_name] = TopicQueue(topic_name)


class TopicQueue:
    """
    TopicQueue is a queue of messages for a given topic.
    """
    def __init__(self, topic_name):
        self.topic_name = topic_name
        self.topic_queue = []

    def enqueue(self, message):
        """
        Enqueue a message to the topic queue.
        """
        self.topic_queue.append(message)

    def get_at_offset(self, offset):
        """
        Get the message at the given offset.
        """
        if offset >= len(self.topic_queue):
            return None
        return self.topic_queue[offset]

    def size(self):
        """
        Size of the topic queue.
        """
        return len(self.topic_queue)

