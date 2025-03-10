#mongodb
import json
import configparser
from pymongo import MongoClient
from bson import json_util

#line
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

    try:
        client = MongoClient(MONGO_URI,connectTimeoutMS=30000,socketTimeoutMS=45000,serverSelectionTimeoutMS=30000)
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]
        print('ok')
        
        logger.info(event)
        
        documents = collection.find(filter={'GarbageCollectionPointId':event})
        
        #documents = collection.find()
        #for document in documents:
        #    print(document)
        
            
        json_data = list(documents)
        logger.info(json_data)
        res = json_util.dumps(json_data)
        logger.info(res)
        
        return {
            'statusCode': 200,
            'body': res
        }

    
    
    except Exception as e:
        print(f"MongoDBの接続でエラーが発生しました: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing data: {str(e)}')
        }
    
    finally:
        # MongoDB接続を閉じる
        client.close()

'''
def lambda_handler(event, context):

    try:
        client = MongoClient(MONGO_URI,connectTimeoutMS=30000,socketTimeoutMS=45000,serverSelectionTimeoutMS=30000)
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]
        print('ok')
        
        documents = collection.find({"geometry":{"$near":{"$geometry":{"type":"Point","coordinates":[137.033741, 35.144708]}}}}).limit(50)
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
        '''

        

