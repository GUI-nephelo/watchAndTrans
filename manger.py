
from fastapi import FastAPI, Request, Response, Depends
from sse_starlette.sse import EventSourceResponse,ServerSentEvent
import asyncio,time
import pandas as pd
from typing import Union
from elasticsearch import Elasticsearch 
import json

conn = Elasticsearch(hosts="http://localhost:9200")

class AlertManger:
    """
    存放黑名单
    并提供检测是否有黑名单的数据被推送
    并广播给用户
    统计每一登录用户的信息
    """

    def __init__(self):
        self.connections = dict()
        self.event = asyncio.Event()
        self.broadcastData : Union[str,dict] = None

        self.blackList = []

    def getKey(self,req:Request):
        # if 'x-forwarded-host' in req.headers and len(req.headers['x-forwarded-host'])>9:
        #     return req.headers['x-forwarded-host']
        # else:
        #     return req.client.__repr__()
        print( req.cookies)
        return  req.cookies.get("next-auth.session-token") if "next-auth.session-token" in  req.cookies.keys() else req.client.port
    # 添加连接
    def add_connection(self,request: Request):
        self.connections[self.getKey(request)] = \
            (request.headers.get('x-forwarded-for'),time.time(),request.headers.get('user-agent'))
        return self.async_generate(request)
    
    def notify(self,msg: str,event:str=None):
        self.event.set()
        self.broadcastData = ServerSentEvent(json.dumps({"msg":msg,"event":event}),event="message")
        self.event.clear()

    async def async_generate(self,request: Request):
        yield ServerSentEvent(json.dumps({"msg":"start SSE","event":""}),event="message")
        try:
            while not await request.is_disconnected():
                await self.event.wait()
                yield self.broadcastData
                # await asyncio.sleep(1)
        # except asyncio.CancelledError:
        #     self.connections.remove(request.client)
        finally:
            # print("remove connection"+request.client.__repr__())
            del self.connections[self.getKey(request)]

    def pullBlackList(self):
        self.blackList = list(
            map(
                lambda x:x['_source']['phoneNum'],
                conn.search(index="blacklist",size=10000)["hits"]["hits"]
            )
        )

    def checkDataframe(self,df: pd.DataFrame):
        def check(phoneNum):
            if phoneNum in self.blackList:
                self.notify(phoneNum,event="blacklistAlert")
        df["phoneNum"].apply(check)

manger = AlertManger()