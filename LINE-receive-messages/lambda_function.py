import os
import logging
import configparser
import json
from bson import json_util
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, StickerSendMessage, QuickReply,
    QuickReplyButton, MessageAction, PostbackAction,
    LocationMessage, LocationSendMessage,
    ButtonComponent, BubbleContainer, FlexSendMessage, BoxComponent, TextComponent, ButtonsTemplate,
    PostbackEvent
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

FUNCTION_NAME1 = config.get("lambda","function_name1")
FUNCTION_NAME2 = config.get("lambda","function_name2")
logger.info(FUNCTION_NAME1)
logger.info(FUNCTION_NAME2)

#位置情報メッセージ作成
def createLocationMessages(collection_address,i):
    return LocationSendMessage(title="集積所"+str(collection_address[i][0]), address=collection_address[i][1], latitude=collection_address[i][2], longitude=collection_address[i][3])

#ボタンメッセージ作成
def createButtonMessages(ledger_nos):
    contents = BubbleContainer(
        direction='ltr',
        header=BoxComponent(
            layout='vertical',
            contents=[
                TextComponent(text='現在地から最寄りの集積所一覧', weight='bold', size='md'),
                TextComponent(text='集積所をクリックで登録', size='xs', color='#888888')
            ]
        ),
        body=BoxComponent(
            layout='vertical',
            contents=[
                ButtonComponent(
                    style='link',
                    height='sm',
                    action={
                        "type": "postback",
                        "label": "集積所1",
                        "data": ledger_nos[0]
                    }
                ),
                ButtonComponent(
                    style='link',
                    height='sm',
                    action={
                        "type": "postback",
                        "label": "集積所2",
                        "data": ledger_nos[1]
                    }
                ),
                ButtonComponent(
                    style='link',
                    height='sm',
                    action={
                        "type": "postback",
                        "label": "集積所3",
                        "data": ledger_nos[2]
                    }
                )
            ]
        )
    )

    return FlexSendMessage(alt_text="最寄りの集積所一覧", contents=contents)

def lambda_handler(event, context):
    logger.info(event)
    signature = event["headers"]["x-line-signature"]
    body = event["body"]

    @LINE_HANDLER.add(MessageEvent, message=LocationMessage)
    def on_message(line_event):
        #ユーザID取得
        profile = LINE_BOT_API.get_profile(line_event.source.user_id)
        user_id = profile.user_id
        logger.info(profile)
        logger.info(user_id)

        #位置情報取得
        lat = line_event.message.latitude
        lon = line_event.message.longitude
        location = [lon, lat]
        logger.info(location)
        
        #集積所関数呼び出し
        payload = {"lon":lon, "lat":lat}
        payload_json = json.dumps(payload)
        logger.info(payload_json)
        response = boto3.client('lambda').invoke(
            FunctionName = FUNCTION_NAME1,
            InvocationType='RequestResponse',
            Payload = payload_json
            )
        print(response)

        #呼び出し結果
        collection_points = json.loads(response['Payload'].read().decode('utf-8'))
        collection_points = json_util.loads(collection_points['body'])
        print(collection_points)

        #検索結果を送信するための情報
        collection_address = [] #集積所情報
        ledger_nos = [] #集積所ID

        for i, item in enumerate(collection_points):
            location = item['properties']['Location']
            lon, lat = item['geometry']['coordinates']
            ledger_no = item['properties']['LedgerNo']
            collection_address.append([i+1, location, lat, lon])
            ledger_nos.append(ledger_no)
        # 結果の表示
        for row in collection_address:
            print(row)
        for row in ledger_nos:
            print(row)


        # 位置情報メッセージのリストを作成
        location_messages = []
        for i in range(3):  # 3箇所の位置情報を作成
            location_messages.append(createLocationMessages(collection_address, i))

        #ボタン作成
        location_messages.append(createButtonMessages(ledger_nos))

        # LINEボットのクライアントを使用してメッセージを送信
        LINE_BOT_API.reply_message(line_event.reply_token, location_messages)


    @LINE_HANDLER.add(PostbackEvent)
    def on_postback(line_event):
        logger.info(line_event)

        # LineUserIdとGarbageCollectionPointIdの取得
        LineUserId = line_event.source.user_id
        GarbageCollectionPointId = line_event.postback.data
        # 結果の表示
        print("LineUserId:", LineUserId)
        print("GarbageCollectionPointId:", GarbageCollectionPointId)


        #関数呼び出し引数
        payload = {"LineUserId":LineUserId, "GarbageCollectionPointId":GarbageCollectionPointId}
        payload_json = json.dumps(payload)
        logger.info(payload_json)


        #登録関数呼び出し
        response = boto3.client('lambda').invoke(
            FunctionName = FUNCTION_NAME2,
            InvocationType='RequestResponse',
            Payload = payload_json
            )
        print(response)

        #登録完了メッセージ
        request_message = TextSendMessage("登録が完了しました。")
        LINE_BOT_API.reply_message(line_event.reply_token, request_message)





    LINE_HANDLER.handle(body, signature)
    return 0

