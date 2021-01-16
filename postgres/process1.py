import psycopg2
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

def add_request(conn, request_id, client_id, ip):
    cursor = conn.cursor()
    cursor.execute('''INSERT INTO requests (request_id, client_id, ip, time_1) VALUES(%s, %s, %s, NOW())''', (request_id, client_id, ip))
    cursor.execute('''INSERT INTO queue_3 (request_id) VALUES (%s);''', (request_id, ))
    cursor.execute('''INSERT INTO queue_2 (request_id) VALUES (%s);''', (request_id, ))
    

def execute(host, user, pwd, barrier, end):
    print('Starting process type 1')
    conn = psycopg2.connect(host=host, user=user, password=pwd, dbname='zbd4')
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    barrier.wait()
    
    while not end.value:
        request_id = str(uuid.uuid4())
        client_id = str(uuid.uuid4())
        ip = int2ip(random.randrange(0, 2**32 - 1))
        add_request(conn, request_id , client_id, ip)
    print('Ending process type 1')
        
