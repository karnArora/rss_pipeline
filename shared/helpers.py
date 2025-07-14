import os
import uuid
import pandas as pd
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv
import os, logging
logging.info("AzureWebJobsStorage=%s  AZURE_STORAGE_CONNECTION_STRING=%s",
             os.getenv("AzureWebJobsStorage"),
             os.getenv("AZURE_STORAGE_CONNECTION_STRING"))

load_dotenv(override=False)   # pulls in .env for dev/testing, but real env wins


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
    """
    Saves `df` to Excel in a memory buffer, uploads it to the given container,
    and returns the public/blob SAS URL.
    """
    blob_name = f"{prefix}{uuid.uuid4().hex}.xlsx"

    # ➊ save DataFrame to bytes
    import io
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)

    # ➋ upload
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
