from concurrent.futures import ProcessPoolExecutor
import asyncio

import psycopg2

def init(params):
    global connection
    global cursor

    connection = psycopg2.connect(**params)
    cursor = connection.cursor()

def s_execute(args):
    global connection
    global cursor

    cursor.execute(*args)

def s_fethcall():
    global connection
    global cursor

    return cursor.fetchall()

def s_fetchone():
    global connection
    global cursor

    return cursor.fetchone()

def s_commit():
    global connection
    global cursor

    connection.commit()

class DB:
    def __init__(self, **params):
        self.pool = ProcessPoolExecutor(max_workers = 1, initializer = init, initargs = (params,))

    async def execute(self, *args):
        await asyncio.get_event_loop().run_in_executor(self.pool, s_execute, args)

    async def fethcall(self):
        result = await asyncio.get_event_loop().run_in_executor(self.pool, s_fethcall)
        return result

    async def fetchone(self):
        result = await asyncio.get_event_loop().run_in_executor(self.pool, s_fetchone)
        return result

    async def commit(self):
        await asyncio.get_event_loop().run_in_executor(self.pool, s_commit)

