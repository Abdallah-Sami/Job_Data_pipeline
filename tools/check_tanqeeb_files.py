"""Lists every Tanqeeb file in landing + archive with size, record count and last-modified time."""
import os
import json
from azure.storage.blob import BlobServiceClient

bsc = BlobServiceClient.from_connection_string(os.environ["AZURE_STORAGE_CONNECTION"])

for container in ["landing", "archive"]:
    print(f"\n=== {container} ===")
    client = bsc.get_container_client(container)
    blobs = sorted((b for b in client.list_blobs() if "tanqeeb" in b.name.lower()),
                   key=lambda b: b.last_modified)
    for b in blobs:
        records = json.loads(client.download_blob(b.name).readall())
        print(f"{b.last_modified:%Y-%m-%d %H:%M}  {b.size/1e6:7.2f} MB  {len(records):6d} records  {b.name}")

ckpt = json.loads(bsc.get_container_client("checkpoints").download_blob("tanqeeb/checkpoint.json").readall())
print(f"\ncheckpoint: {len(ckpt)} URLs")
