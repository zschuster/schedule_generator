import json
import urllib.parse
import boto3
import pandas as pd


def dynamo_scan_to_df(dict_list):
    base_dict = {k: [v] for k, v in dict_list[0].items()}
    keys = base_dict.keys()
    for i in range(1, len(dict_list)):
        for key in keys:
            base_dict[key].append(dict_list[i][key])
    return base_dict


def scan_dynamo_table(table_name='practice_drill'):
    dynamo_resource = boto3.resource('dynamodb')
    drill_table = dynamo_resource.Table(table_name)

    response = drill_table.scan()
    return dynamo_scan_to_df(response['Items'])


def lambda_handler(event, context):

    s3 = boto3.client('s3')
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they\
         exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    input_data = pd.read_csv(response['Body'])
    print(input_data.info())
    practice_drill_data = scan_dynamo_table()
    practice_drill_data = pd.DataFrame(practice_drill_data)
    print(practice_drill_data.info())

    return


if __name__ == '__main__':
    with open('update_test_event.json', 'r') as f:
        test_event = json.load(f)

    print(lambda_handler(test_event, None))

