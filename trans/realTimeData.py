from fastapi import APIRouter, Depends, Request, Response
from fastapi.responses import StreamingResponse
from sse_starlette.sse import EventSourceResponse
import asyncio
from manger import manger
# print(manger)

router = APIRouter()

@router.get("/sse")
def sse(req:Request):
    # print(req.headers)
    return EventSourceResponse(manger.add_connection(req))

@router.get("/clients")
def clients(req:Request):
    return manger.connections

@router.get("/test_newDataPushed")
def test_newDataPushed():
    manger.notify("",event="newDataPushed")
    return "ok"

@router.get("/test_blacklistAlert/{phoneNum}")
def test_blacklistAlert(phoneNum:str):
    manger.notify(phoneNum,event="blacklistAlert")
    return "ok"

@router.get("/test/{event}/{msg}")
def test(req:Request,event:str,msg:str):
    manger.notify(msg,event=event)
    return "ok"