import os, boto3, json, time, sys, threading
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
import botocore.session
from botocore.httpsession import URLLib3Session
from datetime import datetime

from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['s3_bucket_name', 'token'])
print(args)
token = args["token"].lower()

region = 'us-east-1'
BACK_OFF_TIME = 0.3 # seconds
REQUEST_TRIES = 5
S3_BUCKET_NAME = args['s3_bucket_name']
PAGE_SIZE = 250

ssm = boto3.client('ssm')
s3 = boto3.resource('s3')

def set_parameter_store_value(token, value):
    response = ssm.put_parameter(
        Name=f"token-transfers-{token.lower()}",
        Value=json.dumps(value),
        Type='String',
        Overwrite=True
    )
    print("Info: Set parameter store", value)
    return response

def get_parameter_store_value(token):
    try:
        response = ssm.get_parameter(Name=f"token-transfers-{token.lower()}", WithDecryption=False)
        print("INFO: Get Parameter Store", response['Parameter']['Value'])
        return json.loads(response['Parameter']['Value'])
    except:
        print("INFO: Parameter does not exist remotely yet")
        return {
            'page_number': 0,
            'last_saved_tx_time': 0
        }

def signed_request(url, params=None, service='managedblockchain-query', region='us-east-1'):
    session = botocore.session.Session()
    sigv4 = SigV4Auth(session.get_credentials(), service, region)
    data = json.dumps(params, indent=4, sort_keys=True, default=str)
    request = AWSRequest(method='POST', url=url, data=data, headers=None)
    sigv4.add_auth(request)
    http_session = URLLib3Session()
    response_code = None
    tries = 0
    while (response_code != 200):
        if tries >= REQUEST_TRIES:
            raise Exception(f"Maximum request retries reached.\n\tURL: {url}\n\tParameters: {params}")

        try:
            response = http_session.send(request.prepare())
            response_code = response.status_code
            print(f"Response: {response.content.decode('utf-8')}")
        except botocore.exceptions.ReadTimeoutError:
            print(f"ERROR: Read timeout error. Trying again...")

        tries += 1

        if response_code != 200:
            print(f"ERROR: Response code {response.status_code}.\n\tResponse content: {response.content.decode('utf-8')}")

        # Too many requests. Backing-off
        if response_code == 429:
            print(f"INFO: Back-off {BACK_OFF_TIME * tries} seconds") # 0.3s to 1.5s
            time.sleep(BACK_OFF_TIME * tries)

    # print(response)
    return json.loads(response.content.decode('utf-8'))

def params_list_token_balances(address=None, network="ETHEREUM_MAINNET"):
    return {
        "tokenFilter": {
            "network": network,
            "contractAddress": address
        },
        "maxResults": PAGE_SIZE
    }

def params_list_transactions(address=None, next_page=None, from_time=None, to_time=None, max_results=10, sort_by="TRANSACTION_TIMESTAMP", sort_order="ASCENDING", network="ETHEREUM_MAINNET"):
    return {
        "address": address,
        "confirmationStatusFilter": {
            "include": ["FINAL"]
        },
        "fromBlockchainInstant": {
            "time": from_time
        },
        "maxResults": max_results,
        "network": network,
        "nextToken": next_page,
        "sort": {
            "sortBy": sort_by,
            "sortOrder": sort_order
        },
        "toBlockchainInstant": {
            "time": to_time
        }
    }

def params_list_transaction_events(next_page=None, max_results=10, transaction_hash=None, network="ETHEREUM_MAINNET"):
    return {
        "transactionHash": transaction_hash,
        "network": network,
        "nextToken": next_page,
        "maxResults": max_results
    }

def ListTokenBalances(params):
    return signed_request(f'https://managedblockchain-query.{region}.amazonaws.com/list-token-balances', params=params)

def ListTransactions(params):
    return signed_request(f'https://managedblockchain-query.{region}.amazonaws.com/list-transactions', params=params)

def ListTransactionEvents(params):
    # Example return:
    # {'events': [{
    #   'contractAddress': '0xdac17f958d2ee523a2206206994597c13d831ec7', 
    #   'eventType': 'ERC20_TRANSFER',
    #   'from': '0x5a9cf70048db6f7b1c96b651a9a8c58567d73b0c', 
    #   'network': 'ETHEREUM_MAINNET',
    #   'to': '0xef79e049d27f1d91d2ab81deab28da5f4934d8ed', 
    #   'transactionHash': '0x872f02e878628a75d7bf25ef93cc78eeae6ac49f8cd69d6203de737f68cdc4f6', 
    #   'value': '130000'
    # }]}
    return signed_request(f'https://managedblockchain-query.{region}.amazonaws.com/list-transaction-events', params=params)

def save_to_s3(filename, contents):
    print("Saving to S3...", filename, contents)
    s3.Object(S3_BUCKET_NAME, filename).put(Body=contents)

def get_last_processed_transaction_time(token):
    print(f"ListTransactions: {token}")
    
    txs = ListTransactions(params_list_transactions(
        address=token,
        sort_order="DESCENDING",
        max_results=1
    ))
    print(f"Transactions length: {len(txs['transactions'])}")
    print(f"Transactions: {txs}")
    
    if len(txs['transactions']) >= 1:
        return txs['transactions'][0]['transactionTimestamp']
    else:
        return None

def process():

    params = get_parameter_store_value(token)

    from_time = params['last_saved_tx_time']
    to_time = get_last_processed_transaction_time(token)
    
    print("INFO: Params:", params, to_time)
    
    params_list_transactions_obj = params_list_transactions(address=token, from_time=from_time, to_time=to_time, max_results=PAGE_SIZE)

    next_page = ''
    while next_page != None:
        print(f"INFO: Parameters {params}")
        transactions = ListTransactions(params_list_transactions_obj)
        print(f"INFO: Transactions: PAGE {params['page_number']} | Page size: {len(transactions['transactions'])}")

        if len(transactions['transactions']) > 0:
            params['last_saved_tx_time'] = transactions['transactions'][-1]['transactionTimestamp']

        rows = '"contractAddress","eventType","from","to","value","transactionHash","transactionTimestamp"\n'

        if "transactions" in transactions:
            for tx in transactions["transactions"]:
                tx_timestamp = int(tx["transactionTimestamp"] * 1000)
                tx_hash = tx["transactionHash"]
                print(f"\n{tx_hash}\t{tx_timestamp}")

                params_txe = params_list_transaction_events(transaction_hash = tx_hash)

                transaction_events = ListTransactionEvents(params_txe)
                print(f"INFO: {transaction_events}")
                if "events" in transaction_events:
                    for ev in transaction_events["events"]:
                        if (ev["contractAddress"] == token):
                            print("INFO:", ev["eventType"], ev["contractAddress"])                            
                            rows += f"\"{ev.get('contractAddress', '')}\",\"{ev.get('eventType', '')}\",\"{ev.get('from', '')}\",\"{ev.get('to', '')}\",{ev.get('value', 0)},\"{ev.get('transactionHash', '')}\",{tx_timestamp}\n"

            save_to_s3(f"{token}/events/{params['page_number']:0>9}.csv", rows)
            print(f"INFO: {params['page_number']}")

        if "nextToken" in transactions:
            next_page = transactions["nextToken"]
            params_list_transactions_obj["nextToken"] = next_page
            params["page_number"] += 1
            set_parameter_store_value(token, params)
        else:
            next_page = None
            print("This is the last page.")

process()
