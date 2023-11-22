import boto3, json, time, sys, botocore.session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.httpsession import URLLib3Session
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['s3_bucket_name', 'token'])
token = args["token"].lower()
region = 'us-east-1'
BACK_OFF_TIME = 0.3 # seconds
REQUEST_TRIES = 20
S3_BUCKET_NAME = args['s3_bucket_name']
PAGE_SIZE = 250
CONCURRENCY = 50

ssm = boto3.client('ssm')
s3 = boto3.resource('s3')

def set_parameter_store_value(token, value):
    response = ssm.put_parameter( Name=f"token-transfers-{token.lower()}", Value=json.dumps(value), Type='String', Overwrite=True )
    return response

def get_parameter_store_value(token):
    try:
        response = ssm.get_parameter(Name=f"token-transfers-{token.lower()}", WithDecryption=False)
        return json.loads(response['Parameter']['Value'])
    except:
        return { 'page_number': 0, 'last_saved_tx_time': 0 }

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
        if tries >= 5:
            raise Exception(f"Maximum request retries reached.\n\tURL: {url}\n\tParameters: {params}")
        try:
            response = http_session.send(request.prepare())
            response_code = response.status_code
        except botocore.exceptions.ReadTimeoutError:
            print(f"ERROR: Read timeout error. Trying again...")
        tries += 1
        if response_code != 200:
            print(f"ERROR: Response code {response.status_code}.\n\tResponse content: {response.content.decode('utf-8')}")
        if response_code == 429:
            print(f"INFO: Back-off {BACK_OFF_TIME * tries} seconds")
            time.sleep(BACK_OFF_TIME * tries)
    return json.loads(response.content.decode('utf-8'))

def params_list_token_balances(address=None, network="ETHEREUM_MAINNET"):
    return {
        "tokenFilter": { "network": network, "contractAddress": address },
        "maxResults": PAGE_SIZE
    }

def params_list_transactions(address=None, next_page=None, from_time=None, to_time=None, max_results=10, sort_by="TRANSACTION_TIMESTAMP", sort_order="ASCENDING", network="ETHEREUM_MAINNET"):
    return {
        "address": address,
        "network": network,
        "nextToken": next_page,
        "sort": { "sortBy": sort_by, "sortOrder": sort_order },
        "fromBlockchainInstant": { "time": from_time },
        "toBlockchainInstant": { "time": to_time },
        "maxResults": max_results
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
    return signed_request(f'https://managedblockchain-query.{region}.amazonaws.com/list-transaction-events', params=params)

def save_to_s3(filename, contents):
    s3.Object(S3_BUCKET_NAME, filename).put(Body=contents)

def get_last_processed_transaction_time(token):
    txs = ListTransactions(params_list_transactions( address=token, sort_order="DESCENDING", max_results=1))
    if len(txs['transactions']) >= 1:
        return txs['transactions'][0]['transactionTimestamp']
    else:
        return None

def process():
    params = get_parameter_store_value(token)
    from_time = params['last_saved_tx_time']
    to_time = get_last_processed_transaction_time(token)
    params_list_transactions_obj = params_list_transactions(address=token, from_time=from_time, to_time=to_time, max_results=PAGE_SIZE)
    next_page = ''
    while next_page != None:
        transactions = ListTransactions(params_list_transactions_obj)
        print(f"INFO: Transactions: PAGE {params['page_number']} | Page size: {len(transactions['transactions'])}")
        if len(transactions['transactions']) > 0:
            params['last_saved_tx_time'] = transactions['transactions'][-1]['transactionTimestamp']
        rows = '"contractAddress","eventType","from","to","value","transactionHash","transactionTimestamp"\n'
        if "transactions" in transactions:
            for tx in transactions["transactions"]:
                tx_timestamp = int(tx["transactionTimestamp"] * 1000)
                tx_hash = tx["transactionHash"]
                params_txe = params_list_transaction_events(transaction_hash = tx_hash)
                transaction_events = ListTransactionEvents(params_txe)
                if "events" in transaction_events:
                    for ev in transaction_events["events"]:
                        if (ev["contractAddress"] == token):
                            rows += f"\"{ev.get('contractAddress', '')}\",\"{ev.get('eventType', '')}\",\"{ev.get('from', '')}\",\"{ev.get('to', '')}\",{ev.get('value', 0)},\"{ev.get('transactionHash', '')}\",{tx_timestamp}\n"
            save_to_s3(f"{token}/events/{params['page_number']:0>9}.csv", rows)
        if "nextToken" in transactions:
            next_page = transactions["nextToken"]
            params_list_transactions_obj["nextToken"] = next_page
            params["page_number"] += 1
            set_parameter_store_value(token, params)
        else:
            next_page = None
            print("Info: This is the last page. Exiting...")
process()