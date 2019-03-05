import boto3
import json

BUCKET='librivox-db'
KEY='books.json'

#dev = boto3.session.Session(profile_name='ranjitiyer')

#boto3.setup_default_session(dev)

dynamo = boto3.client('dynamodb')
s3 = boto3.client('s3')

def get_one_book():
    r = s3.select_object_content(Bucket=BUCKET,
            Key=KEY,
            ExpressionType='SQL',
            Expression="select * from s3object limit 1",
            InputSerialization = {'JSON': {"Type": "LINES"}},
            OutputSerialization = {'JSON': {"RecordDelimiter": "|"}},
    )

    for event in r['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            record=records.rsplit("|")[0]
            return json.loads(record)
        elif 'Stats' in event:
            statsDetails = event['Stats']['Details']
            print("Stats details bytesScanned: ")
            print(statsDetails['BytesScanned'])
            print("Stats details bytesProcessed: ")
            print(statsDetails['BytesProcessed'])

    return None

# {
#   "BookId": "AV6LQI-JM_sCNr6moHzZ",
#   "Offset": 0,
#   "SectionNumber": 1,
#   "Title": "Apocrypha",
#   "Url": "http://ia800508.us.archive.org/29/items/apocrypha_1605_librivox/apocrypha_01_plato_hippiasmajor_64kb.mp3",
#   "UserId": "amzn1.ask.account.AFIR2V4QJCOVDJOXY7N76XM4KXINZO4U2WHNVY7SAM6FVCKFXVXBOL2BDL6ZAR3WID3LRDZTQ6FE76YQNWSSFYKMNF3WMC6GWHYB4FIZM3YVHTIPHYTIJXVU3ZWNUGNT4UYVD4EO3JHVIUSSKK7DBIB4HJZM3BGCK6C2TJAFSKHMIXWDUHMQSSJ2ZRWYSOSTPCE7V7VKLWTZBYQ"
# }
def save_to_dynamo(state):
    print("In save to dynamo {} ".format(state))
    response=dynamo.put_item(
        TableName='AudioBooksUserState',
        Item={
            'UserId': {
                'S': state["UserId"]
            },
            'BookId': {
                'S': state["BookId"]
            },
            'Offset': {
                'N': str(state["Offset"])
            },
            'SectionNumber': {
                'N': str(state["SectionNumber"])
            },
            'Title': {
                'S': state["Title"]
            },
            'Url': {
                'S': state["Url"]
            }
        })
    print(response)

def update_offset_in_dynamo(state):
    print("In Update offset ")
    response=dynamo.update_item(
        TableName='AudioBooksUserState',
        ExpressionAttributeNames={
            '#O': 'Offset'
        },
        ExpressionAttributeValues={
            ':o': {
                'N': str(state['Offset'])
            }
        },
        Key= {
            'UserId': {
                'S': state['UserId']
            }
        },
        UpdateExpression='SET #O = :o')
    print (response)

def get_offset(user_id):
    print("Getting offset for {}".format(user_id))
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('AudioBooksUserState')
    response = table.get_item(
        Key = {
            'UserId': user_id
        }
    )
    offset = response['Item']['Offset']
    print("Returning offset {}".format(offset))
    return response['Item']

def execute_s3_select_query(query: str):
    print("Running query {}".format(query))
    r = s3.select_object_content(Bucket=BUCKET,
            Key=KEY,
            ExpressionType='SQL',
            Expression=query,
            InputSerialization = {'JSON': {"Type": "LINES"}},
            OutputSerialization = {'JSON': {"RecordDelimiter": "|"}},
    )

    for event in r['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            for record in records.rsplit("|")[:-1]:
                print(record)
                yield json.loads(record)

def query_by_title(title):
    return execute_s3_select_query("select t.id, t.title "
                                   "from s3object t "
                                   "where t.title LIKE '%{}%'".format(title))

def get_book_by_title(title):
    return execute_s3_select_query("select * "
                                   "from s3object t "
                                   "where t.title LIKE '%{}%'".format(title))

if __name__ == '__main__':
    print("Running queries")
    recs = query_by_title('secret garden')
    for rec in recs:
        print(rec)
