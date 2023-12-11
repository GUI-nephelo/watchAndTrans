import time,os,sys
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import shutil
import pandas as pd
from hashlib import md5
from time import sleep

import logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', \
                    datefmt='%Y/%d/%m %I:%M:%S %p',level=logging.INFO)

from .db import dataFrame2db

logger = logging.getLogger(__name__)

prefix = "qh_zrt-beijing-"


# 要监测的文件夹路径
folder_path = os.path.expanduser(f"~dataRecv")

class MyEventHandler(FileSystemEventHandler):
    def on_any_event(self, event):
        path = event.src_path
        if event.is_directory:
            return
        
        # print(event.event_type,path)
        if path.endswith(".tmp") and prefix in path:
            if event.event_type=="moved":
                sleep(0.001)
                path = path[:-4]
                hold(path)
            return
        if event.event_type=="closed":
            if path.endswith(".txt") and prefix in path:
                hold(path)
                return
            drop(path)
                

def hold(path):
    logger.info(f"检测到围栏数据{path.split('/')[-1]}")
    df = trans(path.split('/')[-1])
    if dataFrame2db(df):
        shutil.move(path,folder_path+"/../backups/")
def drop(path):
    logger.info(f"检测到垃圾文件{path.split('/')[-1]}")
    # shutil.move(path,folder_path+"/../junk/")
    os.unlink(path)

col_name = ["IMSI","IMEI","time","phoneNum","model"]

# 将文件夹全部文件进行转换
def swape():
    dfs = []
    for i in os.listdir(folder_path):
        path = os.path.join(folder_path,i)
        if i.endswith(".txt") and prefix in i:
            dfs.append(trans(i))
            # shutil.move(path,folder_path+"/../backups/")
        else:
            # shutil.move(path,folder_path+"/../junk/")
            os.unlink(path)
    if dfs:
        return pd.concat(dfs)
    return []

def processLine(i):
    spl = [None if j=="" else j for j in i.split("\t")]
    for _ in range(len(col_name)-len(spl)): 
        spl+=[None]
    return spl

# 转换特定文件
def trans(fname):
    path = os.path.join(folder_path,fname)
    with open(path,"r") as f:
        lines = f.read().split("\n")[:-1]
    spaceSegList = [i for i in map(processLine,lines)]
    logger.info(lines)
    index = [md5(i.encode("utf8")).hexdigest() for i in lines] # 以MD5码当作index和es数据库的_id
    df = pd.DataFrame(spaceSegList,columns=col_name,index=index) 
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d%H%M%S")
    # print(df)
    return df

def main():
    logger.info("程序启动")
    # 清理目录
    data = swape()
    # print(data)
    if len(data)>0: # 若目录里有东西就转入数据库
        if dataFrame2db(data):
            os.system(f"mv {folder_path}/{prefix}* {folder_path}/../backups/")
    logger.info("清扫完成")
    # 创建一个 Observer 实例
    observer = Observer()
    # 创建一个事件处理器
    event_handler = MyEventHandler()
    # 将事件处理器添加到 Observer 中
    observer.schedule(event_handler, folder_path, recursive=True)
    # 启动 Observer
    observer.start()
    

    # 等待 Observer 线程结束
    return observer


