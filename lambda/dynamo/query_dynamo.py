from boto3 import resource
from boto3.dynamodb.conditions import Key

dynamo_resource = resource('dynamodb')
drill_table = dynamo_resource.Table('practice_drill')

response = drill_table.query(
    IndexName='skill_level-index',
    Select='ALL_ATTRIBUTES',
    KeyConditionExpression=Key('skill_level').eq('beginner'))

print(response['Items'])
