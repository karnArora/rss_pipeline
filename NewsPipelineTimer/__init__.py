import logging
import azure.functions as func
from shared.helpers import write_df_to_blob
import os
from dotenv import load_dotenv
load_dotenv(override=False)   # pulls in .env for dev/testing, but real env wins


# your heavy pipeline
from pipeline_logic import run_rss_pipeline


def main(mytimer: func.TimerRequest) -> None:
    """
    Timer-triggered entry-point.
    The CRON schedule is defined in function.json.
    """
    logging.info("⏰ Timer trigger fired")

    try:
        df_final = run_rss_pipeline()          # <-- returns the enriched DataFrame
        url = write_df_to_blob(
            df_final,
            container=os.environ.get("OUTPUT_CONTAINER", "rss-output")
        )
        logging.info("✅ pipeline finished – file uploaded to %s", url)

    except Exception as exc:
        logging.exception("💥 pipeline failed: %s", exc)
        raise
