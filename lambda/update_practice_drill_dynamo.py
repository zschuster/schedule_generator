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


def upload_df_dynamo(table, pd_df):
    for index_id in pd_df.index:
        put_response = table.put_item(Item=pd_df.loc[index_id].to_dict())
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

    s3 = boto3.client('s3')

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    response = read_s3_csv(s3, bucket, key)

    input_data = pd.read_csv(response['Body'])
    print(input_data.info())

    practice_drill_data = scan_dynamo_table()
    practice_drill_data = pd.DataFrame(practice_drill_data)
    print(practice_drill_data.info())

    join_cols = ['description', 'display_name', 'name', 'skill_level']

    # entries that will need to be uploaded
    to_upload = pd_left_not_right(input_data, practice_drill_data, join_cols)
    to_upload['drill_id'] = generate_drill_id(practice_drill_data, len(to_upload))

    to_delete = pd_left_not_right(practice_drill_data, input_data, join_cols)

    # add and remove rows to dynamo table
    table = boto3.resource('dynamodb', region_name='us-east-1').Table('practice_drill')

    upload_df_dynamo(table, to_upload)
    del_df_dynamo(table, to_delete)

    return json.dumps({'status': 'success',
                       'rows_uploaded': str(len(to_upload)),
                       'rows_deleted': str(len(to_delete))})


if __name__ == '__main__':
    # test event
    with open('update_test_event.json', 'r') as f:
        event = json.load(f)

    print(lambda_handler(event, None))
