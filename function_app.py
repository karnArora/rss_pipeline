# function_app.py  (root of project)

import os, logging
from dotenv import load_dotenv

import azure.functions as func
from azure.functions.decorators import FunctionApp   # <-- new

from pipeline_logic import run_rss_pipeline
from shared.helpers import write_df_to_blob

load_dotenv(override=False)

app = FunctionApp()            # <-- TOP-LEVEL OBJECT THE WORKER WANTS

# -------- HTTP trigger ----------
@app.function_name("rssHttpTrigger")
@app.route(route="rss-pipeline", methods=["POST", "GET"])
def http_entry(req: func.HttpRequest) -> func.HttpResponse:
    try:
        df = run_rss_pipeline()
        blob = write_df_to_blob(df,
                                container=os.getenv("OUTPUT_CONTAINER", "rss-output"))
        return func.HttpResponse(f"✅ Uploaded: {blob}", status_code=200)
    except Exception as exc:
        logging.exception("Pipeline failed")
        return func.HttpResponse(f"❌ Error: {exc}", status_code=500)

# -------- Timer trigger to run automatically (optional) ----------
@app.function_name("rssTimerTrigger")
@app.schedule("0 */30 * * * *", arg_name="timer", run_on_startup=False)
def timer_entry(timer: func.TimerRequest) -> None:
    logging.info("Scheduled run")
    df = run_rss_pipeline()
    write_df_to_blob(df, container=os.getenv("OUTPUT_CONTAINER", "rss-output"))
