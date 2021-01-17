import psycopg2
import time
import random
import select


query_claim = '''
UPDATE queue_3
SET status='taken'
WHERE request_id IN (
    SELECT request_id
    FROM queue_3
    WHERE status='new'
    LIMIT 1
    FOR NO KEY UPDATE SKIP LOCKED
) RETURNING request_id;
'''

def get_notify(conn, end):
    if conn.notifies:
        return conn.notifies.pop(0)
    while not end.value:
        # Wait for new request to process
        if select.select([conn],[],[],0.5) == ([],[],[]):
            continue
        # Fetch notifications from server
        conn.poll()
        if conn.notifies:
            return conn.notifies.pop(0)
    return None

def execute(host, user, pwd, barrier, end):
    print('Starting process type 3')
    conn = psycopg2.connect(host=host, user=user, password=pwd, dbname='zbd4')
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    curs = conn.cursor()
    curs.execute('LISTEN "new_request_3";')
    curs.execute('LISTEN "2_finished";')
    barrier.wait()
    
    while not end.value:
        notify = get_notify(conn, end)
        if notify is None:
            return
        if notify.channel != "new_request_3":
                continue

        # Try to claim request
        curs.execute(query_claim)
        res = curs.fetchall()
        
        # If we didn't claim any request - wait for more notifications
        if len(res) != 1:
            conn.notifies.clear()
            continue
        request_id = res[0][0]
        
        chance = random.random()
        # Immediately send ad
        if chance < 0.5:
            curs.execute('UPDATE requests SET type = 0, time_3 = NOW() WHERE request_id = %s;', (request_id, ))
            continue
        while not end.value:
            notify = get_notify(conn, end)
            if notify is None:
                return
            if notify.channel != '2_finished':
                continue
            #print(f'Received: {notify.payload}, expected: {request_id}')
            if notify.payload != request_id:
                continue
            if chance < 0.75:
                curs.execute('UPDATE requests SET type = 1, time_3 = NOW() WHERE request_id = %s;', (request_id, ))
            else:
                #curs.execute('UPDATE requests SET type = 2, time_3 = NOW() WHERE request_id = %s;', (request_id, ))
                pass
            break

    print('Ending process type 3')
