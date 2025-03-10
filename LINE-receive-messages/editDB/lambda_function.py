import configparser
import csv
import json
import logging
import os
import pymysql
import sys
import urllib.parse
import boto3

"""
初期設定
"""
# 設定ファイルの読み込み
CONFIG_PATH = os.path.dirname(os.path.abspath(__file__)) # このソースコードの上位ディレクトリのパスを取得
config = configparser.ConfigParser()
config.read(CONFIG_PATH + '/config.ini')

#rds & DB
RDS_HOST  = config.get("rds", "rds_host")
DB_USER_NAME = config.get("rds", "db_username")
PW = config.get("rds", "db_password")
DB_NAME = config.get("rds", "db_name")
TABLE_NAME = config.get("rds", "table_name")

# s3 
BUCKET_NAME = config.get("s3", "bucket_name")
OBJECT_KEY_NAME = config.get("s3", "object_key_name")
REGION_NAME = config.get("s3", "region_name")

#loggger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

"""
RDS_MySQLに接続
"""
try:
    conn = pymysql.connect(host=RDS_HOST, user=DB_USER_NAME, passwd=PW, db=DB_NAME, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()
    
cur = conn.cursor()
cursor=conn.cursor(pymysql.cursors.DictCursor)
logger.info("SUCCESS: Connection to RDS MySQL instance")

"""
s3に接続
"""
try:
    s3 = boto3.resource('s3', region_name=REGION_NAME)
except Exception as e:
    logger.error("ERROR: Unexpected error: Could not connect to s3.")
    logger.error(e)
    sys.exit()
logger.info("SUCCESS: Connect s3")

"""
テーブルを作成する関数
"""
def createTable():
    try:
        message = 'create table '+DB_NAME+'.'+TABLE_NAME+' (UserID int, lat float, lon float, ledgerNum varchar(50), address varchar(50), district varchar(10), type varchar(50), citizen varchar(256))'
        cur.execute(message)
        conn.commit()
    except Exception as e:
        logger.error("ERROR: Could not create table.")
        logger.error(e)
        sys.exit()
    logger.info("SUCCESS: Create table")
    
"""
テーブルを削除する関数
"""
def deleteTable():
    try:
        message = 'drop table '+TABLE_NAME
        cur.execute(message)
        conn.commit()
    except Exception as e:
        logger.error("ERROR: Could not delete table.")
        logger.error(e)
        sys.exit()
    logger.info("SUCCESS: delete table.")

"""
テーブル内の全てのデータを削除する関数
"""
def deleteData():
    try:
        message = 'delete from '+TABLE_NAME
        cur.execute(message)
        conn.commit()
    except Exception as e:
        logger.error("ERROR: Could not delete data.")
        logger.error(e)
        sys.exit()
    logger.info("SUCCESS: delete data")

"""
DBにcsvファイルを更新する関数
"""
def updateDB(contents):
    tmp_csv = '/tmp/test.csv'   #一時的に格納するためのファイル
    garbagePoints=[]    #一時的に格納するための配列
    
    #csvファイルに一時的にデータを格納
    try:
        with open(tmp_csv, 'a') as csv_data:
            csv_data.write(contents)
        with open(tmp_csv) as csv_data:
            csv_reader = csv.DictReader(csv_data)
            for csv_row in csv_reader:
                garbagePoints.append(csv_row)
        os.remove(tmp_csv)
    except Exception as e:
        print(e)
        print('ERROR: Could not create csv.')
        raise e
    logger.info("SUCCESS: create csv.")
    logger.info(garbagePoints[0])
       
    #DBにアップデート
    try:
        for row in garbagePoints:
            message="INSERT INTO "+TABLE_NAME+" (UserID, lat, lon, ledgerNum, address, district, type) VALUES(%s,%s,%s,%s,%s,%s,%s)"
            cur.execute(message ,(int(row['UserID']),float(row['lat']),float(row['lon']),row['台帳番号'],row['所在地'],row['地区割'],row['収集品目']))
            conn.commit()
            #message="UPDATE "+TABLE_NAME+" SET citizen="+profile+" WHERE lat="+data["lat"]+" lon="+data["lon"]
            
    except Exception as e:
        logger.error("ERROR: Could not insert.")
        logger.error(e)
        sys.exit()
    logger.info("SUCCESS: succeeded to insert!")
    

def lambda_handler(event, context):
    logger.info(event)
    """
    # Get the object from the event and show its content type
    #bucket = event['Records'][0]['s3']['bucket']['name']
    #key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        object = s3.Object(BUCKET_NAME, OBJECT_KEY_NAME)
        response = object.get()
        body = response['Body'].read()
        contents = body.decode('utf-8')
    except Exception as e:
        logger.error("ERROR: Could not get contents.")
        logger.error(e)
        raise e
    logger.info("SUCCESS: Get contents!")
    """ 
    #deleteData()
    #deleteTable()
    #createTable()
    
    bucket = s3.Bucket(BUCKET_NAME)
    logger.info(bucket)
    response = bucket.Object(OBJECT_KEY_NAME).get()
    logger.info(response)
    body = response['Body'].read()
    contents = body.decode('utf-8')
    logger.info("SUCCESS: Get contents!")
    
    updateDB(contents)
    
    return 0

    cursor.close()
    cur.close()
    conn.close()