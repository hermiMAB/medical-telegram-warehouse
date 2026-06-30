import os
import csv
import json
import glob
import argparse
import logging
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
 
# ---------------------------------------------------------------------------
# Project root resolution
# load_to_postgres.py -> src/ -> medical-telegram-warehouse/
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
 
# Load .env from the project root (not from wherever you run the script)
load_dotenv(PROJECT_ROOT / ".env")
 
# ---------------------------------------------------------------------------
# Logging — console only for this script (scraper handles file logs)
# ---------------------------------------------------------------------------
logger = logging.getLogger("load_to_postgres")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(_handler)
 
# ---------------------------------------------------------------------------
# Known medical channels — only messages from these are accepted.
# Update this set if you add new channels to the scraper.
# ---------------------------------------------------------------------------
MEDICAL_CHANNELS = {

    "CheMed123",
    "Thequorachannel",
    "lobelia4cosmetics",
    "tikvahpharma",
}
 
# ---------------------------------------------------------------------------
# Database connection — values come from .env (set by docker-compose.yml)
# ---------------------------------------------------------------------------
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "telegram_warehouse")
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
 
DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
 
 
# =============================================================================
# DATABASE ENGINE
# =============================================================================
 
def get_engine():
    """
    Create a SQLAlchemy engine using the DATABASE_URL built from .env values.
    The engine is a connection manager — it knows how to reach the database
    but does not open a connection until .connect() is called.
    Exits immediately with a clear message if Docker/Postgres is not running.
    """
    try:
        engine = create_engine(DATABASE_URL)
        # Quick connectivity check — fails fast with a clear message
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"Connected to database '{DB_NAME}' on {DB_HOST}:{DB_PORT}")
        return engine
    except Exception as exc:
        logger.error(
            f"Could not connect to PostgreSQL.\n"
            f"  URL : {DATABASE_URL}\n"
            f"  Make sure Docker is running: docker-compose up -d\n"
            f"  Error: {exc}"
        )
        sys.exit(1)
 
 
# =============================================================================
# TABLE SETUP
# =============================================================================
 
def create_raw_tables(engine) -> None:
    """
    Create the raw schema and both tables if they do not already exist,
    then truncate them so every load is a clean full refresh.
 
    Tables:
      raw.telegram_messages  — scraped text messages from all four channels
      raw.yolo_detections    — object detection results from medical images
    """
    with engine.connect() as conn:
 
        # Schema — groups our raw tables away from any future marts/staging
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
 
        # ------------------------------------------------------------------
        # raw.telegram_messages
        # One row per Telegram message across all four channels.
        # loaded_at is filled automatically so we know when each batch
        # was inserted — useful for DBT incremental models later.
        # ------------------------------------------------------------------
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                message_id      INTEGER,
                channel_name    TEXT,
                channel_title   TEXT,
                message_date    TEXT,
                message_text    TEXT,
                has_media       BOOLEAN,
                image_path      TEXT,
                views           INTEGER,
                forwards        INTEGER,
                loaded_at       TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.execute(text("TRUNCATE TABLE raw.telegram_messages"))
 
        # ------------------------------------------------------------------
        # raw.yolo_detections
        # One row per detected object in a message image.
        # Only populated when data/yolo_results.csv exists (image pipeline).
        # ------------------------------------------------------------------
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.yolo_detections (
                image_path      TEXT,
                message_id      INTEGER,
                channel_name    TEXT,
                detected_class  TEXT,
                confidence      REAL,
                image_category  TEXT,
                loaded_at       TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.execute(text("TRUNCATE TABLE raw.yolo_detections"))
 
        conn.commit()
 
    logger.info("Tables ready: raw.telegram_messages, raw.yolo_detections")
 
 
# =============================================================================
# LOAD TELEGRAM MESSAGES
# =============================================================================
 
def load_json_files(engine, data_path: str) -> int:
    """
    Find all per-channel JSON files under data/raw/telegram_messages/
    and insert their messages into raw.telegram_messages.
 
    Only messages whose channel_name is in MEDICAL_CHANNELS are inserted —
    anything unexpected is skipped and logged as a warning.
 
    Returns the total number of rows inserted.
    """
    pattern = os.path.join(
        data_path, "raw", "telegram_messages", "*", "*.json"
    )
    # Exclude _manifest.json — that is metadata, not message data
    json_files = sorted(
        f for f in glob.glob(pattern)
        if not os.path.basename(f).startswith("_")
    )
 
    if not json_files:
        logger.warning(
            f"No JSON files found at: {pattern}\n"
            f"  Run the scraper first: python src/scraper.py --demo --path data"
        )
        return 0
 
    logger.info(f"Found {len(json_files)} JSON file(s) to load")
 
    total_inserted = 0
    total_skipped  = 0
 
    with engine.connect() as conn:
        for filepath in json_files:
            with open(filepath, "r", encoding="utf-8") as f:
                messages = json.load(f)
 
            # Filter to known medical channels only
            valid   = [m for m in messages if m.get("channel_name") in MEDICAL_CHANNELS]
            skipped = len(messages) - len(valid)
 
            if skipped:
                logger.warning(
                    f"Skipped {skipped} message(s) from unknown channel(s) "
                    f"in {os.path.basename(filepath)}"
                )
                total_skipped += skipped
 
            for msg in valid:
                conn.execute(
                    text("""
                        INSERT INTO raw.telegram_messages (
                            message_id, channel_name, channel_title, message_date,
                            message_text, has_media, image_path, views, forwards
                        ) VALUES (
                            :message_id, :channel_name, :channel_title, :message_date,
                            :message_text, :has_media, :image_path, :views, :forwards
                        )
                    """),
                    {
                        "message_id":    msg["message_id"],
                        "channel_name":  msg["channel_name"],
                        "channel_title": msg.get("channel_title", ""),
                        "message_date":  msg["message_date"],
                        "message_text":  msg.get("message_text", ""),
                        "has_media":     msg.get("has_media", False),
                        "image_path":    msg.get("image_path"),
                        "views":         msg.get("views", 0),
                        "forwards":      msg.get("forwards", 0),
                    },
                )
 
            total_inserted += len(valid)
            logger.info(
                f"  {os.path.basename(filepath):<35} "
                f"-> {len(valid)} rows inserted"
            )
 
        conn.commit()
 
    logger.info(f"Messages loaded : {total_inserted}")
    if total_skipped:
        logger.info(f"Messages skipped (unknown channel): {total_skipped}")
 
    return total_inserted
 
 
# =============================================================================
# LOAD YOLO DETECTIONS  (optional — only if image pipeline has run)
# =============================================================================
 
def load_yolo_results(engine, csv_path: str = None) -> None:
    """
    Load YOLO object detection results into raw.yolo_detections.
 
    This is optional — the function exits silently if the CSV does not exist.
    The CSV is produced by the image detection pipeline (not the scraper).
 
    Expected CSV columns:
        image_path, message_id, channel_name, detected_class,
        confidence, image_category
    """
    # Default path relative to project root
    if csv_path is None:
        csv_path = str(PROJECT_ROOT / "data" / "yolo_results.csv")
 
    if not os.path.exists(csv_path):
        logger.info(
            "No YOLO results found — skipping image detection load.\n"
            f"  Expected at: {csv_path}\n"
            "  (Run the image detection pipeline to generate this file.)"
        )
        return
 
    rows_inserted = 0
 
    with engine.connect() as conn:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
 
            for row in reader:
                # Only load detections for our known medical channels
                if row.get("channel_name") not in MEDICAL_CHANNELS:
                    logger.warning(
                        f"Skipping YOLO row — unknown channel: {row.get('channel_name')}"
                    )
                    continue
 
                conn.execute(
                    text("""
                        INSERT INTO raw.yolo_detections (
                            image_path, message_id, channel_name,
                            detected_class, confidence, image_category
                        ) VALUES (
                            :image_path, :message_id, :channel_name,
                            :detected_class, :confidence, :image_category
                        )
                    """),
                    {
                        "image_path":     row["image_path"],
                        "message_id":     int(row["message_id"]),
                        "channel_name":   row["channel_name"],
                        "detected_class": row["detected_class"],
                        "confidence":     float(row["confidence"]),
                        "image_category": row["image_category"],
                    },
                )
                rows_inserted += 1
 
        conn.commit()
 
    logger.info(f"YOLO detections loaded: {rows_inserted} rows from {csv_path}")
 
 
# =============================================================================
# SUMMARY REPORT
# =============================================================================
 
def print_summary(engine) -> None:
    """
    Print a quick row-count summary per channel after loading,
    so you can immediately verify everything landed correctly.
    """
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                channel_name,
                COUNT(*)            AS message_count,
                SUM(views)          AS total_views,
                SUM(CASE WHEN has_media THEN 1 ELSE 0 END) AS messages_with_media
            FROM raw.telegram_messages
            GROUP BY channel_name
            ORDER BY channel_name
        """))
        rows = result.fetchall()
 
    if not rows:
        logger.warning("raw.telegram_messages is empty after load.")
        return
 
    logger.info("\n-- Load Summary ---------------------------------------------")
    logger.info(f"  {'Channel':<25} {'Messages':>10} {'Total Views':>13} {'With Media':>12}")
    logger.info(f"  {'-'*25} {'-'*10} {'-'*13} {'-'*12}")
    for row in rows:
        logger.info(
            f"  {row[0]:<25} {row[1]:>10} {row[2]:>13} {row[3]:>12}"
        )
    logger.info("-------------------------------------------------------------\n")
 

 
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load medical Telegram data lake into PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""

        """,
    )
    parser.add_argument(
        "--path", type=str,
        default=str(PROJECT_ROOT / "data"),
        help="Root data directory (default: <project_root>/data)",
    )
    parser.add_argument(
        "--no-yolo", action="store_true",
        help="Skip loading YOLO detection results even if the CSV exists.",
    )
    args = parser.parse_args()
 
    logger.info("=== Medical Telegram Warehouse — Postgres Loader ===")
    logger.info(f"Data path : {args.path}")
    logger.info(f"Database  : {DB_NAME} on {DB_HOST}:{DB_PORT}")
    logger.info(f"Channels  : {', '.join(sorted(MEDICAL_CHANNELS))}")
 
    # 1. Connect
    engine = get_engine()
 
    # 2. Create / reset tables
    create_raw_tables(engine)
 
    # 3. Load scraped messages
    load_json_files(engine, args.path)
 
    # 4. Load YOLO detections (optional)
    if not args.no_yolo:
        load_yolo_results(engine)
 
    # 5. Print per-channel summary
    print_summary(engine)
 
    logger.info("Done. Your data is ready for DBT.")