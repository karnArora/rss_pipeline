from azure.storage.blob import BlobServiceClient
import os, io, pandas as pd, uuid
svc = BlobServiceClient.from_connection_string(os.getenv("AzureWebJobsStorage"))
container = svc.get_container_client("rss-output")
container.create_container()           # first run only

# upload a 1-row dummy xlsx
buf = io.BytesIO()
pd.DataFrame([{"hello": "world"}]).to_excel(buf, index=False)
buf.seek(0)
blob_name = f"test_{uuid.uuid4().hex}.xlsx"
container.upload_blob(blob_name, buf.getvalue(), overwrite=True)
print("✓ uploaded →", container.get_blob_client(blob_name).url)
