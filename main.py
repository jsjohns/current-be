#!/usr/bin/env python3
from fastapi import FastAPI

app = FastAPI()

@app.get("/hello")
def hello():
    return {"message": "hello"}
