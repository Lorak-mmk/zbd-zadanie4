import redis
import time
import random


def execute(host, port, barrier, end):
    print('Starting process type 3')
    all_req = 0
    r = redis.Redis(host='localhost', port=6379, db=0)
    barrier.wait()
    
    while not end.value:
        res = r.blpop('queue_3', 0.1)
        if not res:
            continue
        all_req += 1
        request_id = res[1].decode()
        
        chance = random.random()
        # Immediately send ad
        if chance < 0.5:
            r.hset(request_id, mapping={'shown': 0, 'time_3': time.time()})
            continue
        # Wait for info from process 2
        started = time.time()
        name = 'finished-' + request_id
        while not end.value:
            msg = r.blpop(name, 0.1)
            if not msg:
                continue
            else:
                break
        if chance < 0.75:
            r.hset(request_id, mapping={'shown': 1, 'time_3': time.time()})
        else:
            pass

    print('Ending process type 3')
