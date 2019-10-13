import json
import urllib.parse
import boto3
import pandas as pd

print('Loading function')

s3 = boto3.client('s3')

# hi alyssa
# adding other documentation
def scan_dynamo_table(table_name='practice_drill'):
    dynamo_resource = boto3.resource('dynamodb')
    drill_table = dynamo_resource.Table(table_name)

    response = drill_table.scan()
    return response


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    # bucket = event['Records'][0]['s3']['bucket']['name']
    # key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    # try:
    #     response = s3.get_object(Bucket=bucket, Key=key)
    #     print("CONTENT TYPE: " + response['ContentType'])
    #     return response['ContentType']
    # except Exception as e:
    #     print(e)
    #     print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
    #     raise e

    practice_drill_data = scan_dynamo_table()
    return practice_drill_data
