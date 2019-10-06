from boto3 import resource
from boto3.dynamodb.conditions import Key

dynamo_resource = resource('dynamodb')
drill_table = dynamo_resource.Table('practice_drill')

print(
    {
        'num_items': drill_table.item_count,
        'primary_key_name': drill_table.key_schema[0],
        'status': drill_table.table_status,
        'bytes_size': drill_table.table_size_bytes,
        'global_secondary_indices': drill_table.global_secondary_indexes
    }
)
