import redis
import json
import threading
from Queue import Queue

class Worker(object):
    def __init__(self, namespace, host='localhost', port=6379, db=0, semaphoreKey=False, semaphoreTTL=60):
        self.namespace = namespace
        self.r = redis.StrictRedis(host=host, port=port, db=db)
        self.p = redis.StrictRedis(host=host, port=port, db=db)
        self.q = Queue()

        self.semaphoreKey = semaphoreKey
        self.semaphoreTTL = semaphoreTTL
        self.checkIn = semaphoreTTL / 2

    def listen(self):
        print 'listening on namespace: %s' % self.namespace
        while True:
            o = self.r.brpop(self.namespace, timeout=60)
            if o is not None:
                print o

class Thread(threading.Thread):
    def __init__(self, callback, data):
        threading.Thread.__init__(self)
        self.callback = callback
        self.data = data

        def run(self):
            self.callback(self.data)
