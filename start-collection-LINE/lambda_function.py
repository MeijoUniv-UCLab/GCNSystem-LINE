import os
import logging
import configparser
import json
import boto3

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


def send_message(user_ids):
  text_msg = '回収が開始されました。'
  messages = TextSendMessage(text = text_msg)
  for user_id in user_ids:
    LINE_BOT_API.push_message(user_id, messages=messages)


def lambda_handler(event, context):
  logger.info(event)

  DistrictCode = event['query']["properties"]["DistrictCode"]
  print(DistrictCode)
  payload = json.dumps(DistrictCode)
  logger.info(payload)

  # Lambda関数の呼び出し
  response = boto3.client('lambda').invoke(
    FunctionName = FUNCTION_NAME,
    InvocationType='RequestResponse',
    Payload = payload
    )
  print(response)

  user_ids = json.loads(response['Payload'].read())
  logger.info(user_ids)

  body = user_id['body']
  print(body)
  data = json.loads(body)


  for user in data:
    line_id = user["LineUserId"]
    logger.info(f"Sending message to: {line_id}")
    collected_message(line_id)

  #send_message(user_ids)