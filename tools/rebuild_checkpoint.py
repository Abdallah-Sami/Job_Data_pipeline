"""
يبني ملف checkpoint جديد من الروابط الموجودة فعليًا في ملفات landing
(مو من الـ checkpoint القديم اللي فيه روابط "معروفة" بس ضاع محتواها).

شغّله محلي بعد ما تحط:
    $env:AZURE_STORAGE_CONNECTION = "<connection string حقك>"

الناتج: checkpoint_rebuilt.json (وبيرفعه تلقائي يستبدل checkpoints/tanqeeb/checkpoint.json)
"""

import os
import json
from azure.storage.blob import BlobServiceClient

CONN_STR = os.environ["AZURE_STORAGE_CONNECTION"]
CONTAINER_CHECKPOINTS = "checkpoints"
CHECKPOINT_BLOB = "tanqeeb/checkpoint.json"

# نقرا من الحاويتين: landing (لسه ما تعالجت) و archive (اتعالجت وانقلت)
SOURCE_CONTAINERS = ["landing", "archive"]

bsc = BlobServiceClient.from_connection_string(CONN_STR)

job_urls = set()
file_count = 0

for container_name in SOURCE_CONTAINERS:
    try:
        client = bsc.get_container_client(container_name)
        blobs = list(client.list_blobs())
    except Exception as e:
        print(f"تحذير: ما قدرت أوصل لحاوية '{container_name}': {e}")
        continue

    for blob in blobs:
        if "tanqeeb" not in blob.name.lower():
            continue
        file_count += 1
        try:
            raw = client.download_blob(blob.name).readall()
            records = json.loads(raw)
        except Exception as e:
            print(f"  تحذير: تعذر قراءة {container_name}/{blob.name}: {e}")
            continue
        for r in records:
            url = r.get("url")
            if url:
                job_urls.add(url)
        print(f"  [{container_name}] {blob.name}: {len(records)} سجل")

print(f"\nعدد الملفات (landing + archive): {file_count}")
print(f"عدد الروابط الفعلية (بدون تكرار): {len(job_urls)}")

# احفظ نسخة محلية للمراجعة
with open("checkpoint_rebuilt.json", "w", encoding="utf-8") as f:
    json.dump(sorted(job_urls), f, ensure_ascii=False, indent=2)
print("\nتم الحفظ محليًا: checkpoint_rebuilt.json")

# ارفعه يستبدل الـ checkpoint السحابي القديم (10,250 رابط) بالروابط الفعلية بس
answer = input("\nترفعه الحين ويستبدل checkpoint السحابي؟ (y/n): ").strip().lower()
if answer == "y":
    checkpoints_client = bsc.get_container_client(CONTAINER_CHECKPOINTS)
    checkpoints_client.upload_blob(
        CHECKPOINT_BLOB,
        json.dumps(sorted(job_urls), ensure_ascii=False),
        overwrite=True,
    )
    print("تم الرفع. checkpoint الحين فيه بس الروابط الفعلية المحفوظة.")
else:
    print("ما تم الرفع. راجع checkpoint_rebuilt.json وارفعه يدويًا لما تتأكد.")
