import redis
import uuid
import socket
import struct
import random
import time

def int2ip(addr):
    return socket.inet_ntoa(struct.pack("!I", addr))


script_add = '''
redis.call('HSET', KEYS[1], 'client_id', ARGV[1], 'ip', ARGV[2], 'time_1', ARGV[3])
redis.call('RPUSH', KEYS[3], KEYS[1])
redis.call('RPUSH', KEYS[2], KEYS[1])
'''

def execute(host, port, barrier, end, sleep_time):
    print('Starting process type 1')
    r = redis.Redis(host='localhost', port=6379, db=0)
    add_request = r.register_script(script_add)
    barrier.wait()
    
    while not end.value:
        request_id = str(uuid.uuid4())
        client_id = str(uuid.uuid4())
        ip = int2ip(random.randrange(0, 2**32 - 1))
        add_request(keys=[f'request-{request_id}', 'queue_2', 'queue_3'], args=[client_id, ip, time.time()])
        if sleep_time:
            time.sleep(sleep_time)
    print('Ending process type 1')
        
