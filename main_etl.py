import json
import boto3
from datetime import datetime, timedelta, timezone
import io
import os
from src.events_decoder.events_decoder import AaveV3RawEventsDecoder
from src.events_decoder.token_transfer_decoder import AaveV3TokenTransferDecoder


# Run parameters
output_path = None
raw_events_input_path = None

if raw_events_input_path is None:
    snapshot_day = datetime.today() - timedelta(days=14)
    snapshot_day = datetime(
        snapshot_day.year, snapshot_day.month, snapshot_day.day, tzinfo=timezone.utc
    ).strftime("%Y-%m-%d")
    raw_events_input_path = f"aave-raw-datasource/daily-raw-events/raw_events_snapshot_date={snapshot_day}/raw_events.json"
    atoken_events_input_path = f"aave-raw-datasource/daily-raw-events/raw_events_snapshot_date={snapshot_day}/raw_BalanceTransfer_events.json"
    output_path = f"aave-raw-datasource/daily-decoded-events/decoded_events_snapshot_date={snapshot_day}/"


AWS_ACCESS_KEY = os.environ["AWS_ACCESS_KEY"]
AWS_SECRET_KEY = os.environ["AWS_SECRET_KEY"]
AWS_API_ENDPOINT = "https://minio-simple.lab.groupe-genes.fr"
BUCKET = "projet-datalab-group-jprat"
VERIFY = False

print(f"Starting ETL...")

print("Step 0: Fetch Pool contract abi and initializing decoders...")

with open("src/abi/aavev3_mainnet.json") as file:
    pool_contract_abi = json.load(file)

decoder = AaveV3RawEventsDecoder(contract_abi=pool_contract_abi)
atoken_decoder = AaveV3TokenTransferDecoder()

print("Step 1: Fetching raw events data...")

ressource_s3 = boto3.resource(
    "s3",
    endpoint_url=AWS_API_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    verify=VERIFY,
)

print("   --> Pool Raw events")

obj = ressource_s3.Object(
    BUCKET,
    raw_events_input_path,
)
data = obj.get()["Body"].read().decode("utf-8")
json_data = json.loads(data)

print("   --> AToken BalanceTransfer() events")

obj = ressource_s3.Object(
    BUCKET,
    atoken_events_input_path,
)
data = obj.get()["Body"].read().decode("utf-8")
json_atoken_data = json.loads(data)
atoken_events = []
for list_events in json_atoken_data:
    atoken_events.extend(list_events)

print("Step 2: Fetching events signatures from abi...")

decoder.get_events_signatures_as_hex()

print("Step 3: Classifiying raw events...")

decoder.classify_raw_events(json_data)

print("Step 4: Decoding raw events...")

decoder.decode_raw_events()

print("Step 5: Finding all active users in pool...")

decoder.get_all_active_users(verbose=True)

print("STEP 7: Decoding AToken BalanceTransfer() events...")

atoken_decoder.decode_transfer_events(atoken_events)

print("STEP 8: Finding all users active in AToken BalanceTransfers...")

atoken_decoder.get_all_token_transfer_users()

print("Step 6: Uploading files to s3...")

client_s3 = boto3.client(
    "s3",
    endpoint_url=AWS_API_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    verify=VERIFY,
)

print("   --> Upload Pool events...")

for event_name, output_table in decoder.all_decoded_events_dict.items():
    buffer = io.StringIO()
    output_table.to_csv(buffer, index=False)
    client_s3.put_object(
        Body=buffer.getvalue(),
        Bucket=BUCKET,
        Key=output_path + f"decoded_{event_name}.csv",
    )

print("   --> Upload Pool active users...")

buffer = io.StringIO()
decoder.all_active_users.to_csv(buffer, index=False)
client_s3.put_object(
    Body=buffer.getvalue(),
    Bucket=BUCKET,
    Key=output_path + f"all_active_users.csv",
)

print("   --> Upload AToken Transfer events...")

buffer = io.StringIO()
atoken_decoder.all_decoded_events.to_csv(buffer, index=False)
client_s3.put_object(
    Body=buffer.getvalue(),
    Bucket=BUCKET,
    Key=output_path + f"decoded_transfers.csv",
)

print("   --> Upload AToken Transfer users...")

buffer = io.StringIO()
atoken_decoder.all_active_users.to_csv(buffer, index=False)
client_s3.put_object(
    Body=buffer.getvalue(),
    Bucket=BUCKET,
    Key=output_path + f"all_atoken_transfer_users.csv",
)

print(f"Outputs successfully generated at {output_path}")

print("Done!")
