#mongodb
import json
import configparser
from pymongo import MongoClient
from bson import json_util
import os
import logging
from collections import defaultdict

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
MONGO_COLLECTION_NAME = config['MongoDB']['gcp']
MONGO_COLLECTION_NAME1 = config['MongoDB']['notified']

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# mongodbからアクセスしたファイルをjson形式で読み込む
def get_json_data(collection):
    cursor = collection.find()
    # カーソルから文書を取得し、BSON から JSON に変換
    json_data = json_util.dumps(list(cursor))
    # JSON 文字列を Python オブジェクトにパース
    return json.loads(json_data)


def lambda_handler(event, context):
    logger.info(event)

    try:
        client = MongoClient(MONGO_URI,connectTimeoutMS=30000,socketTimeoutMS=45000,serverSelectionTimeoutMS=30000)
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]
        print('ok')
        collection1 = db[MONGO_COLLECTION_NAME1]

        print("MongoDBへの接続完了")

        gcp_data = get_json_data(collection1)
        print("MongoDBのファイルーjson形式での読み込み完了")

        features = []
        for row in gcp_data:
            data = {
                'DistrictCode' : row['DistrictCode'],
                'Region' : row['Region'],
                'Status' : row['Status'],
                'StartTime' : "",
            }
            # data['Status'] = 'kyohei'
            print(row)
            print(data)
            features.append(data)





        # # 地区ごとに集約
        # district_map = defaultdict(list)
        # for row in gcp_data:
        #     district_code = row['properties'].get('DistrictCode')
        #     region = row['properties'].get('Region')
        #     # 集積所ごとのStatusがなければ'uncollected'とする
        #     status = row['properties'].get('Status', 'uncollected')
        #     district_map[(district_code, region)].append(status)

        # print

        # # 地区ごとのStatusを判定
        # district_docs = []
        # for (district_code, region), status_list in district_map.items():
        #     if all(s == "uncollected" for s in status_list):
        #         overall_status = "unnotified"
        #     else:
        #         overall_status = "notified"
        #     district_docs.append({
        #         "DistrictCode": district_code,
        #         "Region": region,
        #         "Status": overall_status
        #     })

        print("データの更新開始")
        # collection1.delete_many({})
        
        # 新しいデータを挿入
        # collection2.insert_many(features)
        # if district_docs:
        #     collection1.insert_many(district_docs)
        #     print("地区ごとのデータを挿入しました")
        # else:
        #     print("挿入するデータがありません")
        # print("データの更新完了")

        if collection1.count_documents({}) > 0:
            collection1.delete_many({})
        
        collection1.insert_many(features)
        print("データの挿入が完了しました")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Data successfully processed and updated in MongoDB')
        }

        # collection.update_many({},{ "$set": { "Status": "unnotified" } })

        # documents = collection.find()
        # for document in documents:
        #     print(document)

    
    except Exception as e:
        import traceback
        print(f"MongoDBの接続でエラーが発生しました: {e}")
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing data: {str(e)}')
        }
    
    finally:
        # MongoDB接続を閉じる
        client.close()


