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

'''
USER_ID = 'U56660aab90a7e083b56c5ea8b591b37f'
text_msg = '回収が完了いたしました。'
messages = TextSendMessage(text=text_msg)
LINE_BOT_API.push_message(USER_ID, messages=messages)
'''

def collected_message(user_id):
  text_msg = '回収が完了いたしました。'
  messages = TextSendMessage(text = text_msg)
  LINE_BOT_API.push_message(user_id, messages=messages)


def lambda_handler(event, context):
    
    
  #body = event['body']
  logger.info(event)
  '''
  data = json.loads(event)
  print(data)
  ledger_no = data[0]["properties.LedgerNo"]
  print(ledger_no)
  payload = json.dumps(ledger_no)
  logger.info(payload)
  '''
  LedgerNo = event['query']["properties"]["LedgerNo"]
  print(LedgerNo)
  payload = json.dumps(LedgerNo)
  logger.info(payload)
        
  
  # Lambda関数の呼び出し
  response = boto3.client('lambda').invoke(
    FunctionName = FUNCTION_NAME,
    InvocationType='RequestResponse',
    Payload = payload
    )
  print(response)
    
  user_informations = json.loads(response['Payload'].read())
  print(user_informations)
    
  body = user_informations['body']
  print(body)
  data = json.loads(body)
  '''
  line_id = data[0]["LineUserId"]
  logger.info(line_id)
  collected_message(line_id)
  '''

  for user in data:
    line_id = user["LineUserId"]
    logger.info(f"Sending message to: {line_id}")
    collected_message(line_id)