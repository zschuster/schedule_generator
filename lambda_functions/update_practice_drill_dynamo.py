import json
import urllib.parse
import boto3
import pandas as pd
import decimal
from io import StringIO
from datetime import date
from lambda_functions.dynamo_utils import scan_dynamo_table

s3_resource = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamo_table = boto3.resource('dynamodb').Table('practice_drill')


def read_s3_csv(client, bucket, key):
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        return response
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they\
         exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e


def generate_drill_id(db_df, length):
    id_start = db_df.drill_id.max() + 1
    return [id_start + i for i in range(length)]


def pd_left_not_right(left_df, right_df, on):
    joined = left_df.merge(right_df, how='left', on=on, indicator=True)
    joined = joined.loc[joined._merge == 'left_only']
    del joined['_merge']
    return joined


def decimalize_dict(dictionary):
    for key, val in dictionary.items():
        if isinstance(val, (int, float)):
            dictionary[key] = decimal.Decimal(val)
    print(json.dumps({'action': 'decimalize_dict',
                      'result': 'success'})
          )


def upload_df_dynamo(table, pd_df):
    for index_id in pd_df.index:
        upload_dict = pd_df.loc[index_id].to_dict()
        decimalize_dict(upload_dict)
        put_response = table.put_item(Item=upload_dict)
        print(json.dumps(put_response, indent=4))


def del_df_dynamo(table, pd_df, key='drill_id'):
    for index_id in pd_df.index:
        try:
            key_val = pd_df.loc[index_id, key]
            del_dict = {key: key_val}
            response = table.delete_item(Key=del_dict)
        except Exception as e:
            print(e)
            raise e
        else:
            print("DeleteItem succeeded:")
            print(json.dumps(response, indent=4))


def lambda_handler(event, context):
    print('EVENT:')
    print(event)

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    response = read_s3_csv(s3_client, bucket, key)

    input_data = pd.read_csv(response['Body'])

    practice_drill_data = scan_dynamo_table()
    practice_drill_data = pd.DataFrame(practice_drill_data)

    join_cols = ['description', 'display_name', 'name', 'skill_level']

    # entries that will need to be uploaded
    to_upload = pd_left_not_right(input_data, practice_drill_data, join_cols)
    to_upload['drill_id'] = generate_drill_id(practice_drill_data, len(to_upload))

    to_delete = pd_left_not_right(practice_drill_data, input_data, join_cols)

    # add and remove rows to dynamo table
    upload_df_dynamo(dynamo_table, to_upload)
    del_df_dynamo(dynamo_table, to_delete)

    # write updated table to s3
    new_data = pd.DataFrame(scan_dynamo_table())
    new_data = new_data.loc[:, join_cols]
    today = date.today().strftime('%Y-%m-%d')
    key_string = 'dynamo_practice_drill_current/practice_drill_{}.csv'.format(today)
    csv_buffer = StringIO()
    new_data.to_csv(csv_buffer, index=False)
    s3_resource.Object(bucket, key_string).put(Body=csv_buffer.getvalue())

    print(json.dumps({'status': 'function completed!',
                      'rows_uploaded': str(len(to_upload)),
                      'rows_deleted': str(len(to_delete))})
          )
