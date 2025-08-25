import os
import logging
import configparser
import json
import boto3
import base64

logger = logging.getLogger()
logger.setLevel(logging.INFO)
from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, StickerSendMessage, QuickReply, QuickReplyButton, MessageAction, PostbackAction,
)

# 設定ファイルの読み込み
CONFIG_PATH = os.path.dirname(os.path.abspath(__file__)) # このソースコードの上位ディレクトリのパスを取得
config = configparser.ConfigParser()
config.read(CONFIG_PATH + '/config.ini')

#LINE
LINE_CHANNEL_ACCESS_TOKEN = config.get("LINE_Messaging_API", "access_token")
LINE_CHANNEL_SECRET = config.get("LINE_Messaging_API", "secret")
LINE_BOT_API = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
LINE_HANDLER = WebhookHandler(LINE_CHANNEL_SECRET)

FUNCTION_NAME = config.get("lambda","function_name")
logger.info(FUNCTION_NAME)


def send_message(user_id):
  text_msg = 'あなたの地区でのゴミ回収が開始されました。'
  messages = TextSendMessage(text = text_msg)
  # for user in user_ids:
  #   LINE_BOT_API.push_message(user, messages=messages)
  LINE_BOT_API.push_message(user_id, messages=messages)
  print("ok")


def lambda_handler(event, context):
  logger.info(event)

  # DistrictCode = event['query']["properties"]["DistrictCode"]
  # print(DistrictCode)
  # payload = json.dumps(DistrictCode)
  # logger.info(payload)

  # try:
  #     # body(bodyにはBase64エンコードされた文字列が入っている)を取得する
  #     body = event.get('body')
  #     print("Raw body:", body)
  #     if not body:
  #         logger.error("No body in event.")
  #         return {"statusCode": 400, "body": "No LedgerNo provided."}
        
  #     # Base64デコード
  #     try:
  #         decoded_bytes = base64.b64decode(body)
  #         DistrictCode = decoded_bytes.decode('utf-8')
  #     except Exception as e:
  #         logger.error(f"Base64 decode error: {e}", exc_info=True)
  #         return {"statusCode": 400, "body": "Invalid Base64 string."}
        
  #     # JSON文字列への変換とデバッグ
  #     print("Decoded DistrictCode:", DistrictCode)
  #     payload = json.dumps(DistrictCode, ensure_ascii=False)
  #     logger.info(payload)
  #     print(payload)
  # except Exception as e:
  #     logger.error(f"Error extracting LedgerNo: {e}", exc_info=True)
  #     return {"statusCode": 500, "body": "Internal Server Error"}

  # event['body']はJSON文字列（Node-REDからapplication/jsonで送信）
  logger.info(f"event['body']: {event['body']}")
  payload = json.loads(event['body'])
  district_code = payload.get('DistrictCode')
  region = payload.get('Region')
  logger.info(district_code)
  logger.info(region)

  # Lambda関数の呼び出し
  response = boto3.client('lambda').invoke(
    FunctionName = FUNCTION_NAME,
    InvocationType='RequestResponse',
    Payload=json.dumps({
      "DistrictCode": district_code,
      "Region": region
    })
  )
  print(response)

  user_ids = json.loads(response['Payload'].read())
  logger.info(f"user_ids: {user_ids} (type: {type(user_ids)})")
  logger.info(user_ids)

  # エラー発生時は処理を中断
  if isinstance(user_ids, dict) and user_ids.get('statusCode') != 200:
    logger.error(f"Lambda function error: {user_ids}")
    return user_ids  # エラー内容をそのまま返す

  # user_idsがリストでない場合はリスト化
  if not isinstance(user_ids, list):
    user_ids = [user_ids]

  for user_id in user_ids:
    if user_id:  # user_idが空でない場合のみ送信
      logger.info(f"Sending message to: {user_id}")
      send_message(user_id)

  return {"statusCode": 200, "body": "Messages sent successfully."}

  # for user_id in user_ids:
  #   logger.info(f"Sending message to: {user_id}")
  #   send_message([user_id])

  # body = user_ids['body']
  # print(body)
  # data = json.loads(body)


  # for user in user_ids:
  #   line_id = user["LineUserId"]
  #   logger.info(f"Sending message to: {line_id}")
  #   send_message(line_id)