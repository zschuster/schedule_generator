import boto3


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
