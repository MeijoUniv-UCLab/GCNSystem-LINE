#mongodb
import json
import configparser
from pymongo import MongoClient
from bson import json_util
import os
import logging
import boto3

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
        client = MongoClient(MONGO_URI,connectTimeoutMS=30000,socketTimeoutMS=45000,serverSelectionTimeoutMS=30000)
        db = client[MONGO_DB_NAME]
        collection1 = db[MONGO_COLLECTION_NAME1]
        collection2 = db[MONGO_COLLECTION_NAME2]
        collection3 = db[MONGO_COLLECTION_NAME3]
        print('ok')
        

        document = collection1.find({'DistrictCode': event})
        #for document in documents:
        #    print(document)
        data = list(document)
        print(data)

        status = data[0]["Status"]
        district_code = data[0]["DistrictCode"]
        print(status)
        print(district_code)

        #通知ステータス変更
        if status == "unnotified":
            print("未通知")
            collection1.update_one({'DistrictCode': district_code}, {'$set': {'Status': 'notified'}})


        else:
            print("通知済み")
            return 0

        print("継続")

        #地区コード検索
        query = {"properties.DistrictCode":district_code}
        ledger_results = collection2.find(query, {"properties.LedgerNo": 1, "_id": 0})
        #for result in results:
        #    print(result)

        ledger_numbers = [result['properties']['LedgerNo'] for result in ledger_results]
        logger.info(ledger_numbers)


        #ユーザーID検索
        query2 = {"GarbageCollectionPointId": {"$in": ledger_numbers}}
        line_user_results = collection3.find(query2, {"LineUserId": 1, "_id": 0})

        user_ids = [result['LineUserId'] for result in line_user_results]
        logger.info(user_ids)

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


