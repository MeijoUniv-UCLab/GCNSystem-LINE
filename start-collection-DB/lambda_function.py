#mongodb
import json
import configparser
from pymongo import MongoClient
from bson import json_util
import os
import logging
import boto3
from datetime import date, datetime
from dateutil import tz

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 設定ファイルの読み込み
CONFIG_PATH = os.path.dirname(os.path.abspath(__file__)) # このソースコードの上位ディレクトリのパスを取得
config = configparser.ConfigParser()
config.read(CONFIG_PATH + '/config.ini')


#mongodb
config = configparser.ConfigParser()
config.read('config.ini')
print('configファイルを読み取りました')

# MongoDBの接続情報
MONGO_URI = config["MongoDB"]['url']
# MONGO_URI
MONGO_DB_NAME = config['MongoDB']['db_name']
#mongodb-gcp.json
MONGO_COLLECTION_NAME1 = config['MongoDB']['collection1']
MONGO_COLLECTION_NAME2 = config['MongoDB']['collection2']
MONGO_COLLECTION_NAME3 = config['MongoDB']['collection3']



def lambda_handler(event, context):
    logger.info(event)

    try:
        # eventがJSON文字列の場合を考慮
        if isinstance(event, str):
            event = json.loads(event)
        elif hasattr(event, 'get'):
            pass
        else:
            raise ValueError("Invalid event format")

        # DistrictCodeとRegionを取得
        district_code = event.get('DistrictCode')
        region = event.get('Region')
        geofencelocation = event.get('GeofenceLocation')
        garbagetruckid = event.get('GarbageTruckId')
        sendcount = event.get('SendCount')
        print(sendcount)
        print(geofencelocation)
        print(garbagetruckid)
        if not district_code or not region:
            raise ValueError("DistrictCode or Region is required")
        
        client = MongoClient(MONGO_URI,connectTimeoutMS=30000,socketTimeoutMS=45000,serverSelectionTimeoutMS=30000)
        db = client[MONGO_DB_NAME]
        collection1 = db[MONGO_COLLECTION_NAME1]
        collection2 = db[MONGO_COLLECTION_NAME2]
        collection3 = db[MONGO_COLLECTION_NAME3]
        print('ok')
        

        # document = collection1.find({'DistrictCode': event})
        document = collection1.find({'DistrictCode': district_code, 'Region': region})
        #for document in documents:
        #    print(document)
        data = list(document)
        logger.info(f"collection1 data: {data}")
        print(data)

        if not data:
            logger.info("No document found with DistrictCode and Region")
            return []        

        # status = data[0]["Status"]
        # district_code = data[0]["DistrictCode"]
        document = data[0]
        status = document["Status"]
        district_code = document["DistrictCode"]
        print(status)
        print(district_code)

        d_today = date.today()
        s_today = d_today.isoformat()
        jst_time = tz.gettz('Asia/Tokyo')
        t_now = datetime.now(jst_time).time().replace(microsecond=0)
        s_now = t_now.isoformat()


        #通知ステータス変更
        if status == "unnotified":
            print("未通知")

        else:
            print("通知済み")
            # collection1.update_one({'DistrictCode': district_code, 'Region': region}, {'$set': {'Status': 'unnotified', 'Date': s_today, 'StartTime': s_now, 'GeofenceLocation': geofencelocation, 'GarbageTruckId': garbagetruckid}})
            return []

        
        if sendcount == 1:
            collection1.update_one({'DistrictCode': district_code, 'Region': region}, {'$set': {'Status': 'notified', 'Date': s_today, 'StartTime': s_now, 'GeofenceLocation': geofencelocation, 'GarbageTruckId': garbagetruckid}})
            print("更新完了")
            return []


        print("継続")

        #地区コード検索
        query = {"properties.DistrictCode":district_code,
                 "properties.Region": region}
        point_results = collection2.find(query, {"properties.PointId": 1, "_id": 0})
        #for result in results:
        #    print(result)

        point_numbers = [result['properties']['PointId'] for result in point_results]
        logger.info(point_numbers)
        logger.info(f"point_numbers: {point_numbers}")


        #ユーザーID検索
        query2 = {"GarbageCollectionPointId": {"$in": point_numbers}}
        line_user_results = collection3.find(query2, {"LineUserId": 1, "_id": 0})

        user_ids = [result['LineUserId'] for result in line_user_results]
        logger.info(user_ids)
        logger.info(f"user_ids: {user_ids}")

        return user_ids


    
    except Exception as e:
        logger.error(f"MongoDBの接続でエラーが発生しました: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing data: {str(e)}')
        }
    
    finally:
        # MongoDB接続を閉じる
        client.close()


