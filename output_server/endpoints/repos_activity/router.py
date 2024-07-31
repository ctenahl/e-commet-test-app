from typing import Optional
from datetime import date, timedelta

import asyncio
from fastapi import APIRouter, Request, HTTPException

router = APIRouter()
columns = ['full_name', 'date', 'commits_count', 'authors', 'status']

@router.get('/api/repos/{owner}/{repo}')
async def repos_activity(request : Request, owner : str, repo : str, since : date, until : date):
    db = request.app.state.DB

    #проверка правильности введенных дат
    if since > until:
        raise HTTPException(status_code = 400, detail = 'Bad dates')

    #проверка правильности введенных owner и repo
    await db.execute('''
        SELECT EXISTS(
            SELECT 1 
            FROM repos
            WHERE repo = %s AND owner = %s
            LIMIT 100
        )
    ''', (repo, owner))

    if (await db.fetchone())[0] == False:
        raise HTTPException(status_code = 400, detail = 'Entity not found')
    
    #поиск всех коммитов за определенные даты, для определенного репо
    await db.execute(f'''
        SELECT {', '.join(columns)}
        FROM commits
        WHERE full_name = %(owner)s || '/' || %(repo)s
        AND %(since)s <= date AND date <= %(until)s
        ORDER BY date
    ''', {'repo':repo, 'owner':owner, 'since':since, 'until':until})

    commits = await db.fethcall()

    #добавление заявок на парсинг для отсутствующих в БД дат
    output = []

    i = 0
    for d in range((until - since).days):
        commit = {n:d for n, d in zip(columns, commits[i] if len(commits) != 0 else [])} if i < len(commits) else {};
        commit_date = since + timedelta(d)

        if commit.get('status') == True and commit_date == commit.get('date'):
            output += [{
                'date':commit['date'],
                'commits':commit['commits_count'],
                'authors':commit['authors']
            }]
            i += 1
            continue
        
        output += [{'date':commit_date, 'status':'Data is not updated'}]

        if commit.get('status') == False and commit_date == commit.get('date'):
            i += 1
            continue

        await db.execute('''
            INSERT INTO commits (full_name, date)
            VALUES (%s || '/' || %s, %s)
        ''', (owner, repo, commit_date))

    await db.commit()

    return output