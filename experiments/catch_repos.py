import threading
import queue

import requests
import json
import time
import sqlite3
from tqdm import tqdm

sleep = 10
count = 500

site = 'https://api.github.com/search/repositories'
query = {
    'q':'stars:>0+is:public',
    'sort':'stars',
    'order':'desc',
    'per_page':100
}

login = #логин гитхаба
token = #токен гитхаба
auth = requests.auth.HTTPBasicAuth(login, token)

connection = sqlite3.connect(f'test_repos_{sleep}_sec.db')
cursor = connection.cursor()

qs = [
    '''
    CREATE TABLE IF NOT EXISTS "repos" (
        "start_time" REAL NOT NULL,
        "end_time" REAL NOT NULL,
        "repos" TEXT NOT NULL,
        "rate_limit" INTEGER NOT NULL
    );
    '''
]

for q in qs:
    cursor.execute(q)
connection.commit()

def get_data(q):
    start_time = time.time()
    
    try:
        response = json.loads(requests.get(site, params = query, auth = auth).text)
        repos = [{'id':repo['id'], 'name':repo['name'], 'stars':repo['stargazers_count'],} for repo in response['items']]
        result = (start_time, time.time(), json.dumps(repos), 0)
    except (KeyError, requests.exceptions.ConnectionError): # превышение кол-ва допустимых запросов
        result = (start_time, time.time(), 'Rate limit', 1)
    
    q.put(result)
    
    
q = queue.Queue(maxsize = count)
threads = [threading.Thread(target = get_data, args = (q,)) for i in range(count)]

for t in tqdm(threads, desc = 'Start threads', ascii = True):
    t.start()
    time.sleep(sleep)
    
for t in tqdm(threads, desc = 'Join threads', ascii = True):
    t.join()
    
with tqdm(desc = 'Commit to DB', ascii = True) as pb:
    while True:
        try:
            data = q.get(timeout = 5)
        except queue.Empty:
            break
            
        cursor.execute('INSERT INTO repos VALUES (?, ?, ?, ?)', data)
        connection.commit()
        pb.update(1)