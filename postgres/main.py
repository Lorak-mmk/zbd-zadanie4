#!/usr/bin/env python

from multiprocessing import Process, Barrier, Value
import process1
import process2
import process3
import time
import analyze
import psycopg2
from datetime import timedelta


PGSQL_HOST = 'localhost'
PGSQL_USER = 'postgres'
PGSQL_PWD = 'docker'


PROC1_COUNT = 1
PROC2_COUNT = 1
PROC3_COUNT = 1

def setup_tables():
    conn = psycopg2.connect(host=PGSQL_HOST, user=PGSQL_USER, password=PGSQL_PWD, dbname='')
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute('DROP DATABASE IF EXISTS zbd4')
    cur.execute('CREATE DATABASE zbd4')
    conn.commit()
    conn = psycopg2.connect(host=PGSQL_HOST, user=PGSQL_USER, password=PGSQL_PWD, dbname='zbd4')
    cur = conn.cursor()
    cur.execute('''CREATE TABLE requests (
        request_id UUID,
        client_id UUID,
        ip VARCHAR(16),
        country VARCHAR,
        time_1 TIMESTAMP,
        time_2 TIMESTAMP,
        time_3 TIMESTAMP,
        type INTEGER,
        PRIMARY KEY("request_id")
    )''')
    cur.execute('''CREATE TABLE queue_2 (
        request_id UUID,
        status VARCHAR(5) DEFAULT 'new',
        PRIMARY KEY("request_id")
    )''')
    cur.execute('''CREATE TABLE queue_3 (
        request_id UUID,
        status VARCHAR(5) DEFAULT 'new',
        PRIMARY KEY("request_id")
    )''')
    cur.execute('''CREATE OR REPLACE RULE "request_notification_2" AS
        ON INSERT TO queue_2 DO 
        NOTIFY "new_request_2";
    ''')
    cur.execute('''CREATE OR REPLACE RULE "request_notification_3" AS
        ON INSERT TO queue_3 DO 
        NOTIFY "new_request_3";
    ''')
    conn.commit()


def fetch_results():
    conn = psycopg2.connect(host=PGSQL_HOST, user=PGSQL_USER, password=PGSQL_PWD, dbname='zbd4')
    curs = conn.cursor()
    curs.execute('SELECT MIN(time_1) FROM requests')
    minimal = curs.fetchone()[0]
    curs.execute('SELECT time_1 - %s, time_3 - time_1, type FROM requests WHERE time_3 is not null', (minimal, ))
    ret = curs.fetchall()
    for i in range(len(ret)):
        ret[i] = list(ret[i])
        ret[i][0] = ret[i][0] / timedelta(microseconds=1000)
        ret[i][1] = ret[i][1] / timedelta(microseconds=1000)
    print(ret)
    curs.execute('SELECT COUNT(*) FROM requests')
    allnum = curs.fetchone()[0]
    return ret, allnum

def main():
    setup_tables()
    b = Barrier(PROC1_COUNT + PROC2_COUNT + PROC3_COUNT)
    end_1 = Value('b', False)
    processes_1 = [Process(target=process1.execute, 
                              args=(PGSQL_HOST, PGSQL_USER, PGSQL_PWD, b, end_1)) 
                        for _ in range(PROC1_COUNT)]
    end_2 = Value('b', False)
    processes_2 = [Process(target=process2.execute, 
                              args=(PGSQL_HOST, PGSQL_USER, PGSQL_PWD, b, end_2)) 
                        for _ in range(PROC2_COUNT)]
    end_3 = Value('b', False)
    processes_3 = [Process(target=process3.execute, 
                              args=(PGSQL_HOST, PGSQL_USER, PGSQL_PWD, b, end_3)) 
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
    
    results, allnum = fetch_results()
    analyze.show_results(results, allnum)

if __name__ == '__main__':
    main()
