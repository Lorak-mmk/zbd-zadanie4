#!/usr/bin/env python

from multiprocessing import Process, Barrier, Value
import process1
import process2
import process3
import time
import redis
import analyze
import sys


REDIS_HOST = 'localhost'
REDIS_PORT = 6379

def main():
    (PROC1_COUNT, PROC2_COUNT, PROC3_COUNT) = (int(sys.argv[i]) for i in range(1, 4))
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    r.flushall()
    b = Barrier(PROC1_COUNT + PROC2_COUNT + PROC3_COUNT)
    end_1 = Value('b', False)
    processes_1 = [Process(target=process1.execute, 
                              args=(REDIS_HOST, REDIS_PORT, b, end_1, 0)) 
                        for _ in range(PROC1_COUNT)]
    end_2 = Value('b', False)
    processes_2 = [Process(target=process2.execute, 
                              args=(REDIS_HOST, REDIS_PORT, b, end_2)) 
                        for _ in range(PROC2_COUNT)]
    end_3 = Value('b', False)
    processes_3 = [Process(target=process3.execute, 
                              args=(REDIS_HOST, REDIS_PORT, b, end_3)) 
                        for _ in range(PROC3_COUNT)]
    for p in processes_1:
        p.start()
    for p in processes_2:
        p.start()
    for p in processes_3:
        p.start()

    time.sleep(5)
    
    print('Stopping workers')
    
    end_3.value = True
    for p in processes_3:
        p.join()
    end_2.value = True
    for p in processes_2:
        p.join()
    end_1.value = True
    for p in processes_1:
        p.join()
    
    results = []
    all_requests = 0
    for key in r.keys('*'):
        if not key.decode().startswith('request-'):
            continue
        all_requests += 1
        (time_1, time_2, time_3, is_shown) = r.hmget(key, 'time_1', 'time_2', 'time_3', 'shown')
        (time_1, time_2, time_3) = (float(time_1 or 0), float(time_2 or 0), float(time_3 or 0))
        if not is_shown is None:
            results.append((time_1, time_2, time_3, int(is_shown.decode())))
    results = sorted(results, key=lambda el: el[0])
    
    analyze.show_results(results, all_requests)

if __name__ == '__main__':
    main()
