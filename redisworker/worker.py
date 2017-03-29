import redis
import json
import threading
import traceback
from Queue import Queue

class Worker(object):
    # Constructor function takes the worker's redis queue namespace, sets
    # a queue on itself and takes a callback to be called on the
    # data received from redis. Also has optional parameters for redis connection
    # info, setting a semaphore, and whether to return the trace to the client
    # in the event of an error
    # @param str redis queue namespace
    # @param function callback
    # @param str redis host name
    # @param int redis port number
    # @param int redis db number
    # @param str redis semaphore key
    # @param int TTL for semaphore in seconds
    # @param bool whether to return stack trace in even of error
    # @returns None
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

    # Constantly executing that attempts to pop a value off of the
    # redis queue, if successful it passes the data to a sub thread using
    # the run function as the callback
    # @returns None
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

    # This method is passed to the sub thread as a callback. It sets up the
    # response object and calls the provided callback function. It then sets
    # the response object on the queue for consumption by the master thread.
    # @param JSON the request object
    # @returns None
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

    # This method pulls results off the worker's queue, stringifies it,
    # and publishes it back to the supplied return key
    def pub(self):
        s = self.q.get()
        self.p.publish(s['request']['return_key'], json.dumps(s))
        print 'published to: %s' % s['request']['return_key']

# A basic extension of the native thread class that takes a callback and
# overloads the run method to call the supplied callback
class Thread(threading.Thread):
    def __init__(self, callback, data):
        threading.Thread.__init__(self)
        self.callback = callback
        self.data = data

    def run(self):
        self.callback(self.data)
