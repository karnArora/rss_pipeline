import azure.functions as func
from pipeline_logic import run_rss_pipeline
from shared.helpers import write_df_to_blob
import os
from dotenv import load_dotenv
load_dotenv(override=False)   # pulls in .env for dev/testing, but real env wins

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered Azure Function:
      1. Fetch & process RSS feeds into a DataFrame.
      2. Upload the DataFrame as an Excel file to Blob Storage.
      3. Return the blob name in the response.
    """
    try:
        df = run_rss_pipeline()
        blob_name = write_df_to_blob(df,container=os.environ.get("OUTPUT_CONTAINER", "rss-output"))
        return func.HttpResponse(
            f"✅ RSS feed data uploaded to blob: {blob_name}",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(
            f"❌ Error: {e}",
            status_code=500
        )
