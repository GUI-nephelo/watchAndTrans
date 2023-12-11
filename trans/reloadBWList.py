from fastapi import APIRouter, Depends,Request
from manger import manger


router = APIRouter()

@router.get("")
async def reloadBWList(request: Request):
    manger.pullBlackList()
    # print(manger.blackList)