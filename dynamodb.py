"""

@Author:Suresh
@Date:19-09-2024
@Last Modified By:Suresh
@Last Modified Date:19-09-2024
@Title: AWS DynamoDb CRUD operations using with Python and BOTO3.

"""


import boto3
from botocore.exceptions import ClientError
client = boto3.client("dynamodb", region_name = 'us-east-2')
import json

def create_table():

    table = client.create_table(
        TableName = 'Department',
        KeySchema = [
            {
                'AttributeName':'deptName',
                'KeyType':'HASH'
            },
            {
                'AttributeName':'block',
                'KeyType':'RANGE'
            }
        ],
        AttributeDefinitions = [
            {
                'AttributeName':'deptName',
                'AttributeType':'S'
            },
            {
                'AttributeName':'block',
                'AttributeType':'S'
            }
        ],
        ProvisionedThroughput = {
            'ReadCapacityUnits':10,
            'WriteCapacityUnits':10
        }
    )

    return table

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')


def put_item():

    table = dynamodb.Table('Department')

    response = table.put_item(
        Item={
            'deptName': 'Developer',
            'block': 'B',
            'location': 'Bangalore'
        }
    )
    return response


def get_item():

    table = dynamodb.Table('Department')

    response = table.get_item(
        Key = {
            'deptName':'DataEngineer',
            'block':'A'
        }
    )
    
    if 'Item' in response:
            item = response['Item']
            print("Item found:", item)


def update_item():

    table = dynamodb.Table('Department')
    try:
        response = table.update_item(
            Key={
                'deptName': 'DataEngineer',
                'block': 'A'
            },
            UpdateExpression='SET #loc = :val1',
            ExpressionAttributeNames={
                '#loc': 'location'  # Use a placeholder for the reserved keyword
            },
            ExpressionAttributeValues={
                ':val1': 'mangalore'
            }
        )
        print("Update succeeded:", response)
    except ClientError as e:
        print("Error updating item:", e.response['Error']['Message'])


def delete_key():

    table = dynamodb.Table('Department')
    response = table.delete_item(
        Key = {
            'deptName':'DataEngineer',
            'block': 'A'
        }
    )
    return response


def import_data(filename='dynamodbimport_exported_data.json'):

    table = dynamodb.Table('Department')

    with open(filename, 'r') as f:
        data = json.load(f)

    with table.batch_writer() as batch:
        for item in data:
            batch.put_item(Item=item)

    print(f"Imported {len(data)} items from {filename}")


def export_data(filename='dynamodb_exported_data.json'):

    table = dynamodb.Table('Department')
    response = table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

    print(f"Exported {len(data)} items to {filename}")


if __name__ == "__main__":

    res= create_table()
    print(f"Table created successfully{res}")
    
    res = put_item()
    print(f"Data is inserted Successfully")

    get_item()

    update_item()

    respo = delete_key()
    print(f"data is deleted {respo}")

    import_data(filename='dynamodbimport_exported_data.json')
 
    export_data(filename='dynamodb_exported_data.json')