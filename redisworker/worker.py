import redis
import json
import threading
import traceback
from Queue import Queue

class Worker(object):
    def __init__(self, namespace, callback, host='localhost', port=6379, db=0, semaphoreKey=False, semaphoreTTL=60, trace=True):
        self.namespace = namespace
        self.callback = callback
        self.trace = trace

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
                print 'Received: %s' % o[0]
                envelope = json.loads(o[1])
                t = Thread(self.run, envelope)
                t.start()
                self.pub()

    def run(self, data):
        res = {}
        res['request'] = data
        try:
            res['result'] = self.callback(data)
        except:
            if self.trace:
                err = traceback.format_exc()
                res['error'] = err

            res['result'] = {}

        self.q.put(res)

    def pub(self):
        s = self.q.get()
        self.p.publish(s['request']['return_key'], json.dumps(s))
        print 'published to: %s' % s['request']['return_key']

class Thread(threading.Thread):
    def __init__(self, callback, data):
        threading.Thread.__init__(self)
        self.callback = callback
        self.data = data

    def run(self):
        self.callback(self.data)
