class TopicTable:
    def __init__(self):
        self.topic_queues = {}
        self.last_id = 0

    def get_topic_queue(self, topic_name):
        return self.topic_queues[topic_name]

    def get_topic_list(self):
        return list(self.topic_queues.keys())

    def create_topic_queue(self, topic_name):
        self.topic_queues[topic_name] = TopicQueue(topic_name)


class TopicQueue:
    def __init__(self, topic_name):
        self.topic_name = topic_name
        self.topic_queue = []

    def enqueue(self, message):
        self.topic_queue.append(message)

    def get_at_offset(self, offset):
        if offset >= len(self.topic_queue):
            return None
        return self.topic_queue[offset]

    def size(self):
        return len(self.topic_queue)

    def can_write(self):
        return True