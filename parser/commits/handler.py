import os
import asyncio

import aiohttp
import json

from datetime import date
import psycopg2

connection = psycopg2.connect(
    dbname = os.environ['database'],
    user = os.environ['database_user'],
    password = os.environ['database_password'],
    host = os.environ['database_host'],
    port = os.environ['database_port']
)
cursor = connection.cursor()

#корутина получения коммитов за определенный день (с автодозвоном)
async def fetch(full_name, date, session): 
    params = {'q':' '.join([f'repo:{full_name}', f'committer-date:{date}']), 'sort':'committer-date', 'per_page':100,}
    while True:
        async with session.get('https://api.github.com/search/commits', params = params) as response:
            result =  json.loads(await response.text())

            if result.get('status') == '403':
                await asyncio.sleep(5)
            else:
                return result

async def handler():
    #получаем некоторый список заявок на получение данных
    cursor.execute(f'''
        WITH all_count AS (SELECT count(*) as c FROM commits WHERE status = False)

        SELECT full_name, date
        FROM commits
        WHERE status = False AND random() <= 30.0/(SELECT c from all_count)
    ''')
    date_repos = cursor.fetchall()

    #парсим коммиты
    async with aiohttp.ClientSession(auth = aiohttp.BasicAuth(os.environ['github_login'], os.environ['github_token'])) as session:
        pages = []
        for full_name, date in date_repos:
            pages += [
                fetch(full_name, date, session)
            ]
        
        result = await asyncio.gather(*pages)
    
    #преобразуем полученные джейсоны в удобный для дальнейшей работы вид
    repos = {}
    for repo_commits, (full_name, date) in zip(result, date_repos):
        if repo_commits.get('total_count') == 0:
            repos[(full_name, str(date))] = []
            continue

        for commit in repo_commits['items']:
            if repos.get((full_name, str(date))) is None:
                repos[(full_name, str(date))] = []
            
            repos[(full_name, str(date))] += [commit['author']['login'] if commit['author'] is not None else 'Commit have no author']

    #записываем полученные коммиты в БД
    for (full_name, date) in repos:
        count = len(repos[(full_name, date)])
        authors = ', '.join(set(repos[(full_name, date)]))

        cursor.execute('''
            UPDATE commits
            SET commits_count = %s, authors = %s, status = True
            WHERE full_name = %s AND date = %s
        ''', (count, authors, full_name, date))

    connection.commit()

    return {'statusCode':200, 'body':'ok'}