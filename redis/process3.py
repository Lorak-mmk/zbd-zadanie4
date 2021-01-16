import redis
import time
import random

def execute(host, port, barrier, end):
    print('Starting process type 3')
    r = redis.Redis(host='localhost', port=6379, db=0)
    p = r.pubsub(ignore_subscribe_messages=True)
    p.subscribe('2_finished')
    barrier.wait()
    
    while not end.value:
        res = r.blpop('queue_3', 0.1)
        if not res:
            continue
        request_id = res[1]
        
        chance = random.random()
        # Immediately send ad
        if chance < 0.5:
            r.hset(request_id, mapping={'shown': 0, 'time_3': time.time()})
            continue
        # Wait for info from process 2
        while True:
            message = p.get_message()
            if end.value:
                print('Ending process type 3')
                return
            if message and message['data'] == request_id:
                # Received information from process 2
                break
        if chance < 0.75:
            r.hset(request_id, mapping={'shown': 1, 'time_3': time.time()})
        else:
            r.hset(request_id, mapping={'shown': 2, 'time_3': time.time()})

    print('Ending process type 3')
