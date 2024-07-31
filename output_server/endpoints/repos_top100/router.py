from typing import Optional

import asyncio
from fastapi import APIRouter, Request, HTTPException

router = APIRouter()
columns = ['repo', 'owner', 'position_cur', 'position_prev', 'stars', 'watchers', 'forks', 'open_issues', 'language']

@router.get('/api/repos/top100')
async def top100_repos(request : Request, sort : Optional[str] = None):
    db = request.app.state.DB

    if sort is not None and sort not in columns:
        raise HTTPException(status_code = 400, detail = 'Column for sort not found')

    #получение и сортировка первых 100 репозиториев
    q = f'''
            SELECT {', '.join(columns)}
            FROM repos
            ORDER BY {sort if sort is not None else 'position_cur'}
            LIMIT 100
    '''
    await db.execute(q)
    result = await db.fethcall()

    output = []
    for i in result:
        output += [{n:d for n, d in zip(columns, i)}]

    return output