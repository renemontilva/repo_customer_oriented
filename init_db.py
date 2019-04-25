#! /bin/python3.6

import boto3

# Conntect to dynamodb 
ddb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

# Create table customers
result = ddb.create_table(
        TableName='Customers',
        KeySchema=[
            {
                'AttributeName': 'username',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'email',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'username',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'email',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )

print("{}\n".format(result))
# Create packages table

result = ddb.create_table(
        TableName='PackagePolicies',
        KeySchema=[
            {
                'AttributeName': 'username',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'package',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'username',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'package',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )

print("{}\n".format(result))
# Insert customer data
table = ddb.Table('Customers')

with table.batch_writer() as batch:
    batch.put_item(
        Item={
                'username': 'jdoe',
                'email': 'jdoe@example.com',
                'password': 'test123'
            }
    )
    batch.put_item(
        Item={
                'username': 'pjackson',
                'email': 'pjackson@example.com',
                'password': 'test123'
            }
    )

# Insert package policy data
table = ddb.Table('PackagePolicies')

with table.batch_writer() as batch:
    batch.put_item(
        Item={
            'username': 'jdoe',
            'package': 'adduser_3.118_all.deb'
        }
    )
    batch.put_item(
        Item={
            'username': 'pjackson',
            'package': 'vim_8.1.0875-2_arm64.deb'
        }
    )

# return customers one get item results 

table = ddb.Table('Customers')
response = table.get_item(
                 Key={
                    'username': 'jdoe',
                    'email': 'jdoe@example.com'}
                )
print(response['Item'])

# return PackagePolicies data

table = ddb.Table('PackagePolicies')
response = table.get_item(
                 Key={
                    'username': 'pjackson',
                    'package': 'vim_8.1.0875-2_arm64.deb'}
                )
print(response['Item'])
