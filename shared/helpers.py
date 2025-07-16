import os
import pandas as pd
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv
from datetime import datetime, timezone
import pytz
import os, re
import io

load_dotenv(override=False)

_ILLEGAL_EXCEL_CHARS = re.compile(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]')

def _clean_cell(x):
    if isinstance(x, str):
        return _ILLEGAL_EXCEL_CHARS.sub('', x)
    return x

def get_blob_service_client() -> BlobServiceClient:
    """
    Returns an authenticated BlobServiceClient using the
    `AzureWebJobsStorage` environment variable.
    """
    conn_str = os.environ["AzureWebJobsStorage"]
    return BlobServiceClient.from_connection_string(conn_str)


def write_df_to_blob(
    df: pd.DataFrame,
    container: str,
    prefix: str = "rss_",
    content_type: str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
) -> str:
    
# Create a timezone-aware datetime in IST
    ist = pytz.timezone("Asia/Kolkata")
    timestamp = datetime.now(ist).strftime("%Y%m%dT%H%M%S%z")
    blob_name = f"{prefix}{timestamp}.xlsx"

    buffer = io.BytesIO()
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].map(_clean_cell)

    df.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)

    # upload
    svc = get_blob_service_client()
    container_client = svc.get_container_client(container)
    try:
        container_client.create_container()
    except Exception:
        pass  # already exists
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(
        buffer.getvalue(),
        overwrite=True,
        content_settings=ContentSettings(content_type=content_type),
    )

    return blob_client.url
