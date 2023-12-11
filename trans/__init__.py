from fastapi import FastAPI, Request, Response, Depends
from sse_starlette.sse import EventSourceResponse
from trans import reloadBWList,realTimeData
from fastapi.encoders import jsonable_encoder


app = FastAPI()

@app.get("/")
async def hello(req: Request,rep: Response):
    return Response("welcome visit this transite")

@app.get("/_header")
async def hello(req: Request,rep: Response):
    return Response(req.headers.__repr__())
app.include_router(reloadBWList.router, prefix="/reloadBWList")
app.include_router(realTimeData.router, prefix="/realTimeData")