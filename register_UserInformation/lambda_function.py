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
MONGO_COLLECTION_NAME = config['MongoDB']['collection']



def lambda_handler(event, context):
    #引き取った情報の整形
    logger.info(event)
    LineUserId = event["LineUserId"]
    GarbageCollectionPointId = event["GarbageCollectionPointId"]
    logger.info(LineUserId)
    logger.info(GarbageCollectionPointId)

    try:
        client = MongoClient(MONGO_URI,connectTimeoutMS=30000,socketTimeoutMS=45000,serverSelectionTimeoutMS=30000)
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]
        print('ok')

        #データ挿入
        collection.insert_one({'LineUserId': LineUserId,'GarbageCollectionPointId': GarbageCollectionPointId})
        
        documents = collection.find()
        for document in documents:
            print(document)
    
    except Exception as e:
        print(f"MongoDBの接続でエラーが発生しました: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing data: {str(e)}')
        }
    
    finally:
        # MongoDB接続を閉じる
        client.close()


