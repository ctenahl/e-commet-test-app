import os

import requests
import json

import psycopg2
 
connection = psycopg2.connect(
    dbname = os.environ['database'],
    user = os.environ['database_user'],
    password = os.environ['database_password'],
    host = os.environ['database_host'],
    port = os.environ['database_port']
)
cursor = connection.cursor()

def handler(event, context):
    #парсим список с гитхаба
    response = json.loads(
        requests.get(
            'https://api.github.com/search/repositories', 
            params = {'q':' '.join(['stars:>0',]), 'sort':'stars', 'order':'desc', 'per_page':100}, 
            auth = requests.auth.HTTPBasicAuth(os.environ['github_login'], os.environ['github_token'])
        ).text
    )

    #получаем список имеющихся репозиториев
    cursor.execute('SELECT repo_id, stars FROM repos ORDER BY stars')
    existing_repos = {repo_id:stars for repo_id, stars in cursor.fetchall()}

    if response.get('items') is None:
        exit()

    #делим спаршеные репозитории на репо, требующие изменения, и репо, требующие добавления
    #это необходимо чтобы обработать случай, когда 101 репозиторий становится 100
    #попутно преобразуем полученные джейсоны в удобный для дальнейшей работы вид
    update_repos = []
    add_repos = []
    for repo in response['items']:
        repo_id = repo['id']
        stars = repo['stargazers_count']

        repo_data = {
            'repo_id':repo_id,
            'repo':repo['name'],
            'owner':repo['owner']['login'],
            'stars':stars,
            'watchers':repo['watchers_count'],
            'forks':repo['forks_count'],
            'open_issues':repo['open_issues_count'],
            'language':repo['language'],
        }

        if repo_id in existing_repos:
            if existing_repos[repo_id] != stars:
                update_repos += [repo_data]
        else:
            add_repos += [repo_data]

    #добавляем данные в БД
    for repo in add_repos:
        cursor.execute('''
            INSERT INTO repos (repo_id, repo, owner, stars, watchers, forks, open_issues, language)
            VALUES (%(repo_id)s, %(repo)s, %(owner)s, %(stars)s, %(watchers)s, %(forks)s, %(open_issues)s, %(language)s)
        ''', repo)

    for repo in update_repos:
        cursor.execute('''
            UPDATE repos 
            SET stars = %(stars)s, watchers = %(watchers)s, forks = %(forks)s, open_issues = %(open_issues)s
            WHERE repo_id = %(repo_id)s;
        ''', repo)

    #запрос необходим для поиска и обновления места в рейтинге только для позиций, которые это место поменяли
    #таким образом, предыдущие места будут меняться только при изменении рейтинга, а не при каждом запросе парсинга
    cursor.execute('''
        WITH
            temp_poses AS (
                SELECT repo_id, row_number() OVER(ORDER BY stars DESC) as temp_pos 
                FROM repos
            ),
            sum_poses AS (
                SELECT repos.repo_id, temp_pos, sum(position_cur - temp_pos) OVER() as sum
                FROM repos
                JOIN temp_poses ON repos.repo_id = temp_poses.repo_id
            )

        UPDATE repos
        SET 
            position_prev = CASE WHEN sum_poses.sum <> 0 THEN position_cur ELSE position_prev END,
            position_cur = temp_pos
        FROM sum_poses
        WHERE repos.repo_id = sum_poses.repo_id;
    ''')

    connection.commit()

    return {'statusCode':200, 'body':'ok'}