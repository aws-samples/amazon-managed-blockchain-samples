import sys, json, time, boto3, botocore.session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.httpsession import URLLib3Session
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['s3_bucket_name', 'token'])
token = args["token"].lower()
s3 = boto3.resource('s3')

REGION = 'us-east-1'
BACK_OFF_TIME = 0.3 # seconds
REQUEST_TRIES = 20
S3_BUCKET_NAME = args['s3_bucket_name']
MAX_RESULTS = 250

def signed_request(url, params=None, service='managedblockchain-query', region=REGION):
    session = botocore.session.Session()
    sigv4 = SigV4Auth(session.get_credentials(), service, region)
    data = json.dumps(params)
    request = AWSRequest(method='POST', url=url, data=data, headers=None)
    sigv4.add_auth(request)
    http_session = URLLib3Session()
    response_code = None
    tries = 0
    while (response_code != 200):
        if tries >= 5:
            raise Exception(f"Maximum request retries reached.\n\tURL: {url}\n\tParameters: {params}")
        response = http_session.send(request.prepare())
        response_code = response.status_code
        tries += 1
        if response_code != 200:
            print(f"ERROR: Response code {response.status_code}.\n\tResponse content: {response.content.decode('utf-8')}")
        if response_code == 429:
            print(f"INFO: Back-off {BACK_OFF_TIME * tries} seconds")
            time.sleep(BACK_OFF_TIME * tries)
    return json.loads(response.content.decode('utf-8'))

def ListTokenBalances(params):
    return signed_request(f'https://managedblockchain-query.{REGION}.amazonaws.com/list-token-balances', params=params)

def save_to_s3(filename, contents):
    s3.Object(S3_BUCKET_NAME, filename).put(Body=contents)

params = {
    "tokenFilter": {
        "network":"ETHEREUM_MAINNET",
        "contractAddress": token
    },
    "maxResults": MAX_RESULTS
}
next_page = ''
page = 0
while next_page != None:
    response = ListTokenBalances(params)
    rows = '"token","address","balance","datetime"\n'
    for h in response["tokenBalances"]:
        column_token_address = token
        column_token_info = h["ownerIdentifier"]["address"]
        column_token_balance = h["balance"]
        column_timestamp = int(h["atBlockchainInstant"]["time"] * 1000)
        rows += f"\"{column_token_address}\",\"{column_token_info}\",{column_token_balance},{column_timestamp}\n"
    save_to_s3(f"{token}/snapshot/{page:0>9}.csv", rows)
    page += 1
    if "nextToken" in response:
        next_page = response["nextToken"]
        params["nextToken"] = next_page
    else:
        next_page = None
        print("INFO: Reached to the last page. Exiting.")
