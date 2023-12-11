import logging

import pandas as pd
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import ApiError
from manger import conn,manger
# print(manger)


logger = logging.getLogger(__name__)



def dataFrame2db(df : pd.DataFrame):
    actions = [{"_index":"elec-fence","_id":hs,"_source":data} for hs,data in df.T.to_dict().items()]
    # print(actions)
    try:
        ii,o = bulk(conn,actions)
        logger.info(f"成功推送{ii}条数据")
        manger.checkDataframe(df)
        manger.notify("",event="newDataPushed")
        return True
    except ApiError as e:
        logger.error(f"推送失败{e}")
        return False

if __name__=="__main__":
    bulk(conn,[{"_index":"elec-fence","_id":2,"_source":{"IMSI":1,"IMEI":1,"time":1,"phoneNum":None}}])