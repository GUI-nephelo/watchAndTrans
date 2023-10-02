import time,os,sys
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import shutil
import pandas as pd
from hashlib import md5

import logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', \
                    datefmt='%Y/%d/%m %I:%M:%S %p',level=logging.INFO)

from db import dataFrame2db

logger = logging.getLogger(__name__)


# 要监测的文件夹路径
folder_path = os.path.expanduser(f"~dataRecv")

class MyEventHandler(FileSystemEventHandler):
    def on_any_event(self, event):
        # 处理文件变动的逻辑
        if event.event_type=="closed":
            path = event.src_path
            
            if event.src_path.endswith(".txt") and "qh_zrt-beijing-" in path:
                logger.info(f"检测到围栏数据{path.split('/')[-1]}")
                df = trans(path.split('/')[-1])
                dataFrame2db(df)
                shutil.move(path,folder_path+"/../backups/")
            else:
                logger.info(f"检测到垃圾文件{path.split('/')[-1]}")
                shutil.move(path,folder_path+"/../junk/")


col_name = ["IMSI","IMEI","time","phoneNum"]

# 将文件夹全部文件进行转换
def swape():
    dfs = []
    for i in os.listdir(folder_path):
        dfs.append(trans(i))
        shutil.move(os.path.join(folder_path,i),folder_path+"/../backups/")
    if dfs:
        return pd.concat(dfs)
    return []

# 转换特定文件
def trans(fname):
    path = os.path.join(folder_path,fname)
    with open(path,"r") as f:
        lines = f.read().split("\n")[:-1]
    spaceSegList = [i.split("\t") for i in lines] # 空格分隔的二维表
    index = [md5(i.encode("utf8")).hexdigest() for i in lines] # 以MD5码当作index和es数据库的_id
    df = pd.DataFrame(spaceSegList,columns=col_name,index=index) 
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d%H%M%S")
    # print(df)
    return df

# def main():
    
if __name__=="__main__":
    # main()
    logger.info("程序启动")
    # 清理目录
    data = swape()
    if len(data)>0: # 若目录里有东西就转入数据库
        dataFrame2db(data)
    logger.info("清扫完成")
    # 创建一个 Observer 实例
    observer = Observer()
    # 创建一个事件处理器
    event_handler = MyEventHandler()
    # 将事件处理器添加到 Observer 中
    observer.schedule(event_handler, folder_path, recursive=True)
    # 启动 Observer
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("stop...")
        observer.stop()

    # 等待 Observer 线程结束
    observer.join()

