import json
import decimal
import os
from boto3 import resource
from boto3.dynamodb.conditions import Key


def replace_decimals(obj):
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.keys():
            obj[k] = replace_decimals(obj[k])
        return obj
    elif isinstance(obj, decimal.Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj


def get(event, context):
    query_params = event["queryStringParameters"]

    if query_params["skill_level"]:
        skill_level = query_params["skill_level"].lower()
    else:
        return "Named parameter 'skill_level' not found"

    skill_levels = ["beginner", "intermediate", "advanced"]
    if skill_level not in skill_levels:
        return "No valid entry for 'skill_level' detected"

    dynamo_resource = resource("dynamodb")
    drill_table = dynamo_resource.Table(os.getenv("TABLE_NAME"))

    response = drill_table.query(
        IndexName="skill_level-index",
        Select="ALL_ATTRIBUTES",
        KeyConditionExpression=Key("skill_level").eq(skill_level),
    )

    # convert any decimal values to float/int
    body = replace_decimals(response["Items"])

    return {"statusCode": 200, "body": json.dumps(body)}
