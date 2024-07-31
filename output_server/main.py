import os

import uvicorn
from fastapi import FastAPI

from database.database import DB

from endpoints.repos_top100.router import router as repos_top100_router
from endpoints.repos_activity.router import router as repos_activity_router

app = FastAPI()
app.state.DB = DB(
    dbname = os.environ['database'],
    user = os.environ['database_user'],
    password = os.environ['database_password'],
    host = os.environ['database_host'],
    port = os.environ['database_port']
)

app.include_router(repos_top100_router)
app.include_router(repos_activity_router)