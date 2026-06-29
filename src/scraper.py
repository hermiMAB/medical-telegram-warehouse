import os
import csv
import json
import asyncio
import argparse
import logging
import random
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Project root resolution
# scraper.py  →  src/  →  medical-telegram-warehouse/
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]   # medical-telegram-warehouse/
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.datalake import write_channel_messages_json, write_manifest  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Telegram credentials  (from .env — never hard-code these)
# ---------------------------------------------------------------------------
api_id_str: Optional[str] = os.getenv("Tg_API_ID")
api_hash:   Optional[str] = os.getenv("Tg_API_HASH")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TODAY = datetime.today().strftime("%Y-%m-%d")

DEFAULT_CHANNEL_DELAY = 3.0   # seconds between channels (avoids flood bans)
DEFAULT_MESSAGE_DELAY = 0.5   # seconds between individual messages

# Target channels — update handles here if the actual usernames differ
TARGET_CHANNELS = [
    "@Thequorachannel",     # Doctors Online 🇪🇹
    "@CheMed123",           # CheMed — Medical products
    "@lobelia4cosmetics",   # Lobelia Cosmetics
    "@tikvahpharma",        # Tikvah Pharma
]

# ---------------------------------------------------------------------------
# Logging — file + console, both via the same logger
# ---------------------------------------------------------------------------
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("medical_scraper")
logger.setLevel(logging.INFO)

_file_handler = logging.FileHandler(
    LOG_DIR / f"scrape_{TODAY}.log", encoding="utf-8"
)
_file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)

_console_handler = logging.StreamHandler()
_console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

logger.addHandler(_file_handler)
logger.addHandler(_console_handler)


# =============================================================================
# LIVE SCRAPING  (requires Telegram authentication)
# =============================================================================

async def scrape_channel(
    client,
    channel: str,
    csv_writer,
    base_path: str,
    date_str: str,
    limit: int = 100,
    message_delay: float = DEFAULT_MESSAGE_DELAY,
    channel_delay: float = DEFAULT_CHANNEL_DELAY,
    max_retries: int = 3,
) -> int:
    """
    Scrape a single Telegram channel.

    Returns the number of messages saved, or 0 on unrecoverable error.
    Retries automatically on FloodWaitError up to `max_retries` times.
    """
    from telethon.tl.types import MessageMediaPhoto
    from telethon.errors import FloodWaitError

    channel_name = channel.strip("@")
    retries = 0

    while True:
        try:
            entity = await client.get_entity(channel)
            channel_title = getattr(entity, "title", channel_name)
            messages = []

            # Directory for images belonging to this channel
            channel_image_dir = Path(base_path) / "raw" / "images" / channel_name
            channel_image_dir.mkdir(parents=True, exist_ok=True)

            logger.info(f"Scraping @{channel_name} — '{channel_title}' (limit={limit})")

            async for message in client.iter_messages(entity, limit=limit):
                image_path: Optional[str] = None
                has_media = message.media is not None

                # Download photo attachments only (skip documents, stickers, etc.)
                if has_media and isinstance(message.media, MessageMediaPhoto):
                    img_file = channel_image_dir / f"{message.id}.jpg"
                    try:
                        await client.download_media(message.media, str(img_file))
                        image_path = str(img_file)
                    except Exception as exc:
                        logger.warning(
                            f"Image download failed — msg {message.id} in @{channel_name}: {exc}"
                        )

                row = _build_row(
                    message_id=message.id,
                    channel_name=channel_name,
                    channel_title=channel_title,
                    message_date=message.date.isoformat(),
                    message_text=message.message or "",
                    has_media=has_media,
                    image_path=image_path,
                    views=message.views or 0,
                    forwards=message.forwards or 0,
                )
                csv_writer.writerow(list(row.values()))
                messages.append(row)

                if message_delay > 0:
                    await asyncio.sleep(message_delay)

            # Persist channel messages as a date-partitioned JSON file
            write_channel_messages_json(
                base_path=base_path,
                date_str=date_str,
                channel_name=channel_name,
                messages=messages,
            )

            logger.info(f"Done @{channel_name}: {len(messages)} messages saved")

            if channel_delay > 0:
                await asyncio.sleep(channel_delay)

            return len(messages)

        except FloodWaitError as exc:
            wait = max(int(getattr(exc, "seconds", 0) or 0), 1)
            logger.warning(f"FloodWaitError on @{channel_name}: sleeping {wait}s")
            await asyncio.sleep(wait)
            retries += 1
            if retries > max_retries:
                logger.error(f"Max retries exceeded for @{channel_name}. Skipping.")
                return 0

        except Exception as exc:
            logger.error(f"Unhandled error scraping @{channel_name}: {exc}")
            return 0


async def scrape_all_channels(
    client,
    channels: list,
    base_path: str,
    limit: int = 100,
    message_delay: float = DEFAULT_MESSAGE_DELAY,
    channel_delay: float = DEFAULT_CHANNEL_DELAY,
) -> dict:
    """
    Authenticate the Telegram client and scrape all target channels.
    All channels share a single CSV file per day.
    """
    await client.start()
    logger.info(f"Client authenticated. Starting scrape of {len(channels)} channel(s)...")

    # Ensure output directories exist
    csv_dir = Path(base_path) / "raw" / "csv" / TODAY
    csv_dir.mkdir(parents=True, exist_ok=True)
    (Path(base_path) / "raw" / "telegram_messages" / TODAY).mkdir(parents=True, exist_ok=True)
    (Path(base_path) / "raw" / "images").mkdir(parents=True, exist_ok=True)

    csv_path = csv_dir / "telegram_data.csv"
    stats: dict = {}

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(_CSV_HEADERS)

        channel_counts: dict = {}
        for channel in channels:
            count = await scrape_channel(
                client, channel, writer, base_path, TODAY, limit,
                message_delay, channel_delay,
            )
            stats[channel] = count
            channel_counts[channel.strip("@")] = count

    write_manifest(
        base_path=base_path,
        date_str=TODAY,
        channel_message_counts=channel_counts,
    )

    total = sum(stats.values())
    logger.info(f"Scraping complete. Total messages: {total}")
    for ch, n in stats.items():
        logger.info(f"  {ch}: {n} messages")

    return stats


# =============================================================================
# DEMO MODE  — realistic medical / health / pharma sample data
# =============================================================================

# Realistic sample posts per channel.
# Each entry is (message_text, has_media).
SAMPLE_MESSAGES: dict = {
    "DoctorsOnlineET": {
        "title": "Doctors Online 🇪🇹",
        "posts": [
            ("🚨 Health Alert: Typhoid fever cases rising in Addis Ababa. Symptoms: sustained fever, headache, abdominal pain. Seek care early. #PublicHealth", True),
            ("Q: My child has had a fever for 3 days and a rash. Is this measles?\nA: It could be. Please visit the nearest health center immediately for diagnosis and treatment.", False),
            ("Reminder: Malaria is still endemic in lowland areas of Ethiopia. Use insecticide-treated bed nets and seek testing if you have fever and chills.", True),
            ("New WHO guideline on hypertension management is now available. Target BP < 130/80 mmHg for most adult patients. #Cardiology #Ethiopia", False),
            ("Q: What is the recommended iron supplementation dose for pregnant women in Ethiopia?\nA: 60mg elemental iron + 400mcg folic acid daily throughout pregnancy per FMOH protocol.", True),
            ("COVID-19 boosters now available at all health centers in Addis. No appointment needed. Protect yourself and your community. 💉", False),
            ("Case discussion: 35yo female with persistent cough > 3 weeks, night sweats, weight loss. Differential: TB, lymphoma, fungal. First step: GeneXpert sputum test.", True),
            ("Ethiopia's maternal mortality rate has dropped 67% since 2000. Community health extension workers are a key driver of this success. 🙌", False),
            ("Drug interaction alert ⚠️: Rifampicin significantly reduces levels of oral contraceptives. Counsel TB patients on alternative contraception.", True),
            ("Free online CME webinar this Saturday: 'Management of Diabetic Complications in Low-Resource Settings'. Register via the link in bio.", False),
            ("Q: Is Metformin safe in patients with mild CKD (eGFR 45–60)?\nA: Yes, it can be used with caution; avoid if eGFR < 30. Monitor renal function every 3–6 months.", True),
            ("Dengue fever confirmed in Dire Dawa. Symptoms: high fever, severe headache, muscle pain, rash. No specific antiviral; supportive care is key.", False),
            ("Ethiopia is scaling up mental health integration into primary care. If you're experiencing anxiety or depression, your local health post can help.", True),
            ("Pediatric dosing reminder: Amoxicillin for strep pharyngitis in children — 50 mg/kg/day divided every 8–12 hours for 10 days.", False),
            ("Research spotlight: Ethiopian study shows teff-based complementary foods reduce stunting rates in children under 2. Exciting local evidence! 📊", True),
            ("Q: My BP is 160/95 despite Amlodipine 10mg. What next?\nA: Consider adding an ACE inhibitor (e.g., Enalapril 5mg) and reassess in 4 weeks.", False),
            ("Antibiotic resistance is growing in Ethiopia. Only use antibiotics prescribed by a licensed professional. #AntimicrobialStewardship", True),
            ("Burns first-aid tip: Cool the burn with running water for 10–20 minutes. Do NOT apply butter, toothpaste, or oil. Cover with clean cloth. Seek care.", False),
            ("World Diabetes Day: 1 in 10 Ethiopians aged 20–79 is at risk for diabetes. Know your blood sugar. Early detection saves lives. 🩸", True),
            ("FMOH has updated the Essential Medicines List 2024. Key additions include newer insulin analogues and second-line TB drugs. Full list on the FMOH portal.", False),
        ],
    },
    "CheMedET": {
        "title": "CheMed — Medical Products",
        "posts": [
            ("✅ Now in stock: Glucometer test strips (Accu-Chek Active) — 50 strips for 850 ETB. Free delivery within Addis Ababa on orders above 1,500 ETB.", True),
            ("New arrival: Digital infrared thermometers — 420 ETB each. Accurate, fast (1 sec), FDA-cleared. Ideal for clinics and home use.", True),
            ("⚠️ Product recall notice: Lot #2024-TH-09 of XYZ branded disposable syringes. Do not use. Return to supplier for replacement.", False),
            ("Surgical gloves (Latex-free, powder-free) — Box of 100, Sizes S/M/L available — 380 ETB/box. Bulk pricing available for hospitals.", True),
            ("N95 respirator masks now available — KN95 standard certified. 10-pack for 650 ETB. Essential for healthcare workers.", False),
            ("Aneroid sphygmomanometers restocked — 1,200 ETB each. Dual-head stethoscope bundled option: 1,800 ETB. Calibration certificate included.", True),
            ("IV infusion sets (sterile, 15 drops/mL) — Carton of 100 for 1,100 ETB. Fast delivery to facilities in Addis, Hawassa, Mekelle, and Bahir Dar.", False),
            ("Pulse oximeters — fingertip, CMS50D model — 750 ETB each. Reads SpO2 and pulse rate in seconds. Perfect for home monitoring.", True),
            ("Sharps disposal containers (5L) — 55 ETB each. Safe disposal of needles and lancets — required by all licensed health facilities.", False),
            ("Cold chain alert: Our medical refrigerators maintain 2–8°C range. Ideal for vaccine and insulin storage. Units from 18,500 ETB. DM for specs.", True),
            ("Wound care bundle: Povidone-iodine 500mL + sterile gauze 10-pack + 3M Micropore tape — 290 ETB. Great for clinics and pharmacies.", False),
            ("Autoclave sterilization pouches (self-sealing, 200-pack) — 350 ETB. Class 4 chemical indicator. Compatible with all standard autoclaves.", True),
            ("Blood glucose lancets (28G, twist-off safety) — 100-pack for 180 ETB. Compatible with most major glucometer brands.", False),
            ("Diagnostic kit: Rapid malaria RDT (SD Bioline, Pf/Pan) — Box of 25 tests for 1,400 ETB. WHO-prequalified. Fast 15-minute results.", True),
            ("Oxygen concentrator (5L/min, portable) — 28,000 ETB. Home and clinic use. Quiet operation. 1-year warranty. Delivery available nationwide.", False),
            ("Urine dipstick reagent strips (10-parameter) — Pack of 100 for 520 ETB. Tests glucose, protein, blood, pH, ketones, and more.", True),
            ("Elastic adhesive bandages (7.5cm x 4.5m) — Pack of 12 for 480 ETB. High adhesion, latex-free, water-resistant.", False),
            ("ECG electrode pads (disposable, adult, 50-pack) — 320 ETB. Pre-gelled, compatible with most ECG machines.", True),
            ("Nebulizer kit (compressor type, PARI LC) — 3,200 ETB. Effective particle size for lower airway delivery. For home asthma management.", False),
            ("Monthly promo 🎉: 15% off all wound dressing products this week. Minimum order 500 ETB. Use code WOUND15 at checkout via our website.", True),
        ],
    },
    "LobeliaCosmetics": {
        "title": "Lobelia Cosmetics — Cosmetics & Health Products",
        "posts": [
            ("🌿 New arrival: Lobelia Natural Shea Butter Body Cream — 200mL — 320 ETB. Deeply moisturizing, no parabens, no artificial fragrance.", True),
            ("Skin lightening myth ❌: Products with mercury or hydroquinone above 2% are banned in Ethiopia. Always check ingredient labels before buying.", False),
            ("SPF 50 sunscreen — Suitable for all skin tones including melanin-rich complexions — 480 ETB / 100mL. Daily protection from UVA/UVB rays.", True),
            ("Our Ethiopian-made castor oil is back in stock! Cold-pressed, unrefined — 180 ETB / 100mL. For hair, eyebrows, and skin nourishment.", False),
            ("Hair care tip 💡: Washing your hair too frequently strips natural oils. 2–3 times per week is ideal for most hair types.", True),
            ("Kojic acid soap (local brand) — 95 ETB/bar. Gentle brightening, reduces hyperpigmentation from acne. Use sunscreen during the day.", False),
            ("New: Vitamin C serum 20% — 650 ETB / 30mL. Fades dark spots, boosts collagen, antioxidant protection. Morning use recommended.", True),
            ("Attention: A counterfeit 'Lobelia' brand is circulating in Merkato. Only buy from our verified stores or this channel. Check for hologram sticker.", False),
            ("Aloe vera gel (99% pure) — Harvest & processed in Awash Valley, Ethiopia — 220 ETB / 250mL. Soothes sunburn, rashes, and dry skin.", True),
            ("Hair relaxer safety tip ⚠️: Always do a strand and scalp sensitivity test 48 hours before applying chemical relaxers.", False),
            ("Rosehip seed oil — 100% pure, cold-pressed — 580 ETB / 30mL. Rich in Vitamin A and fatty acids. Reduces scars and fine lines.", True),
            ("Intimate hygiene wash — pH-balanced 4.5 formula — 280 ETB / 200mL. Gynecologist-tested. Fragrance-free. Gentle daily use.", False),
            ("Ethiopian kohl (tizita ቲዝታ kohl) — traditional eye cosmetic, now with modern safety testing. 3 shades — 120 ETB each.", True),
            ("Nail care kit: cuticle oil + nail hardener + ridge filler — 450 ETB bundle. Keep nails healthy without breaking the bank.", False),
            ("New: Dermatologist-recommended acne kit — salicylic acid cleanser + niacinamide serum + oil-free moisturizer — 990 ETB bundle.", True),
            ("Cosmetics import update: EFDA now requires all imported cosmetics to carry Amharic labeling. Non-compliant products will be seized.", False),
            ("Customer favourite: Black seed (nigella sativa) oil — locally sourced — 240 ETB / 100mL. Supports skin health and immunity.", True),
            ("Sun protection reminder ☀️: Ethiopian altitude means stronger UV radiation. Wear SPF 30+ even on cloudy days in Addis Ababa.", False),
            ("New partnership with Haramaya University Agriculture Dept for ethically sourced botanical ingredients. 🌱 Supporting local farmers.", True),
            ("End-of-month sale: 20% off all hair care products. Offer valid until Sunday. Order via DM or our Telegram shop link.", False),
        ],
    },
    "TikvahPharma": {
        "title": "Tikvah Pharma — Pharmaceuticals",
        "posts": [
            ("✅ Stock update: Metformin 500mg (1000-tab pack) — 1,850 ETB. Amlodipine 5mg (500-tab pack) — 1,200 ETB. Both EFDA registered.", True),
            ("Dispensing reminder for pharmacists: Oral rehydration salts (ORS) should always be counselled alongside antibiotics for diarrheal illness.", False),
            ("EFDA alert 🚨: Falsified Amoxicillin capsules (250mg) detected in Addis distribution chain. Verify batch numbers with your supplier. Lot: AM-ETH-2024-114.", True),
            ("Cold chain stock: Insulin Glargine (Lantus) — 5 vials for 2,100 ETB. Insulin Regular (Actrapid) — 10 vials for 1,800 ETB. Stored at 2–8°C.", False),
            ("Antiretroviral availability: TLD (Tenofovir/Lamivudine/Dolutegravir) still available through PFSA. Contact your regional health bureau for allocation.", True),
            ("Pharmacovigilance reminder: Report adverse drug reactions to EFDA via the online portal or call 8482. ADR reporting saves lives.", False),
            ("Now available: Azithromycin 500mg (3-pack blister) — 85 ETB. Ciprofloxacin 500mg (10-pack) — 110 ETB. Prescription required.", True),
            ("Hypertension medication access: Enalapril 5mg, Atenolol 50mg, and Hydrochlorothiazide 25mg all in stock. Competitive wholesale prices.", False),
            ("Storage tip for pharmacies 🌡️: Suppositories and certain vaccines must be stored at 2–8°C. Do not freeze unless specified on the label.", True),
            ("Antifungal update: Fluconazole 150mg capsule — 45 ETB per capsule. Clotrimazole 1% cream 20g — 75 ETB. Both in stock.", False),
            ("New: Fixed-dose combination antihypertensive — Amlodipine 5mg + Valsartan 80mg — improves adherence. 580 ETB / 30 tabs.", True),
            ("Pain management: Tramadol 50mg capsules now require special prescription as per updated EFDA narcotic regulation 2024.", False),
            ("Antiparasitic bundle: Albendazole 400mg + Praziquantel 600mg — available for mass drug administration programs. Contact us for bulk pricing.", True),
            ("Counselling tip: Patients on Warfarin must avoid significant changes in Vitamin K intake (leafy greens). Monitor INR closely after diet changes.", False),
            ("Pediatric liquid formulations restocked: Amoxicillin suspension 125mg/5mL, Paracetamol syrup 120mg/5mL, Ibuprofen 100mg/5mL — all EFDA-cleared.", True),
            ("Tikvah's new ordering system is live: place wholesale orders 24/7 via our website. Minimum order 5,000 ETB for free Addis delivery.", False),
            ("Diabetes care pack: Metformin 500mg + Glibenclamide 5mg + Atorvastatin 20mg — 30-day supply for 950 ETB. Prescription required.", True),
            ("EFDA has approved 3 new generic oncology medicines for local distribution. Access to cancer care improving in Ethiopia. 🎗️", False),
            ("Antibiotic stewardship corner: Azithromycin is not effective against most urinary tract infections. Empirical choice should be Nitrofurantoin or Co-trimoxazole.", True),
            ("Monthly donation: 500 ORS sachets and 200 zinc tablets donated to flood-affected communities in Afar region. 🤝 #CommunityHealth", False),
        ],
    },
}

# Channel-specific brand colors used in placeholder demo images
_CHANNEL_COLORS: dict = {
    "DoctorsOnlineET":  (15, 90, 160),   # medical blue
    "CheMedET":         (10, 130, 80),   # clinical green
    "LobeliaCosmetics": (170, 60, 130),  # cosmetics purple-pink
    "TikvahPharma":     (190, 80, 20),   # pharma orange
}


def _create_placeholder_image(
    path: str,
    channel_name: str = "",
    msg_id: int = 0,
    text_snippet: str = "",
) -> None:
    """
    Generate a realistic-looking placeholder JPEG image for demo mode.
    Uses PIL to render the channel name, message ID, and a text snippet
    onto a solid background coloured per channel.
    """
    from PIL import Image, ImageDraw, ImageFont

    bg_color = _CHANNEL_COLORS.get(channel_name, (70, 70, 70))
    img = Image.new("RGB", (480, 320), bg_color)
    draw = ImageDraw.Draw(img)

    # Try system fonts; fall back to PIL default
    try:
        font_lg = ImageFont.truetype(
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 22
        )
        font_sm = ImageFont.truetype(
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 14
        )
    except OSError:
        font_lg = ImageFont.load_default()
        font_sm = font_lg

    draw.text((20, 18), f"@{channel_name}", fill="white", font=font_lg)
    draw.text((20, 50), f"Message #{msg_id}", fill=(210, 210, 210), font=font_sm)

    # Simple manual word-wrap onto the image
    words = text_snippet[:150].split()
    lines, line = [], ""
    for word in words:
        candidate = (line + " " + word).strip()
        if len(candidate) > 48:
            lines.append(line)
            line = word
        else:
            line = candidate
    if line:
        lines.append(line)

    y = 90
    for ln in lines[:6]:
        draw.text((20, y), ln, fill=(230, 230, 230), font=font_sm)
        y += 22

    draw.text((20, 300), "DEMO IMAGE — NOT REAL", fill=(255, 255, 255), font=font_sm)
    img.save(path, "JPEG", quality=85)


def run_demo(base_path: str, limit: int) -> None:
    """
    Generate a full demo data lake run using SAMPLE_MESSAGES.
    Produces the exact same file layout as the live scraper.
    """
    logger.info("[DEMO] Generating sample medical/health/pharma data")

    date_str = TODAY
    base = Path(base_path)

    csv_dir = base / "raw" / "csv" / date_str
    csv_dir.mkdir(parents=True, exist_ok=True)
    (base / "raw" / "telegram_messages" / date_str).mkdir(parents=True, exist_ok=True)
    (base / "raw" / "images").mkdir(parents=True, exist_ok=True)

    csv_path = csv_dir / "telegram_data.csv"
    now = datetime.now(timezone.utc)
    channel_counts: dict = {}

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(_CSV_HEADERS)

        for channel_name, channel_data in SAMPLE_MESSAGES.items():
            channel_title = channel_data["title"]
            posts = channel_data["posts"][:limit]
            messages = []

            channel_image_dir = base / "raw" / "images" / channel_name
            channel_image_dir.mkdir(parents=True, exist_ok=True)

            logger.info(f"[DEMO] @{channel_name} — '{channel_title}' ({len(posts)} posts)")

            for i, (text, has_media) in enumerate(posts):
                msg_id = 2000 + i
                msg_date = (
                    now - timedelta(hours=i * 3 + random.randint(0, 2))
                ).isoformat()
                image_path = None

                if has_media:
                    img_file = channel_image_dir / f"{msg_id}.jpg"
                    _create_placeholder_image(str(img_file), channel_name, msg_id, text)
                    image_path = str(img_file)

                row = _build_row(
                    message_id=msg_id,
                    channel_name=channel_name,
                    channel_title=channel_title,
                    message_date=msg_date,
                    message_text=text,
                    has_media=has_media,
                    image_path=image_path,
                    views=random.randint(100, 12_000),
                    forwards=random.randint(0, 500),
                )
                messages.append(row)
                writer.writerow(list(row.values()))

            write_channel_messages_json(
                base_path=base_path,
                date_str=date_str,
                channel_name=channel_name,
                messages=messages,
            )
            channel_counts[channel_name] = len(messages)
            logger.info(f"[DEMO] Finished @{channel_name}: {len(messages)} messages")

    write_manifest(
        base_path=base_path,
        date_str=date_str,
        channel_message_counts=channel_counts,
    )

    total = sum(channel_counts.values())
    logger.info(f"[DEMO] Complete. Total messages: {total}")
    for ch, n in channel_counts.items():
        logger.info(f"  @{ch}: {n} messages")
    logger.info(f"[DEMO] Data lake root : {base_path}/raw/")
    logger.info(f"[DEMO] Log file       : logs/scrape_{date_str}.log")


# =============================================================================
# SHARED HELPERS
# =============================================================================

_CSV_HEADERS = [
    "message_id", "channel_name", "channel_title", "message_date",
    "message_text", "has_media", "image_path", "views", "forwards",
]


def _build_row(
    *,
    message_id: int,
    channel_name: str,
    channel_title: str,
    message_date: str,
    message_text: str,
    has_media: bool,
    image_path: Optional[str],
    views: int,
    forwards: int,
) -> dict:
    """
    Build a standardised message dict that matches _CSV_HEADERS exactly.
    Having a single helper ensures live and demo modes produce identical schemas.
    """
    return {
        "message_id":    message_id,
        "channel_name":  channel_name,
        "channel_title": channel_title,
        "message_date":  message_date,
        "message_text":  message_text,
        "has_media":     has_media,
        "image_path":    image_path,
        "views":         views,
        "forwards":      forwards,
    }


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Telegram scraper for Ethiopian medical & health channels",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
  # Quick demo — no Telegram credentials required
  python src/scraper.py --demo --path data --limit 20

  # Full demo with all 20 posts per channel
  python src/scraper.py --demo --path data

  # Live scrape (requires Tg_API_ID and Tg_API_HASH in .env)
  python src/scraper.py --path data --limit 200

  # Live scrape with custom delays (gentler on rate limits)
  python src/scraper.py --path data --limit 500 --message-delay 1.5 --channel-delay 5
        """,
    )
    parser.add_argument(
        "--path", type=str, default="data",
        help="Root data directory (default: data/). Relative to project root.",
    )
    parser.add_argument(
        "--limit", type=int, default=100,
        help="Max messages to fetch per channel (default: 100).",
    )
    parser.add_argument(
        "--message-delay", type=float, default=DEFAULT_MESSAGE_DELAY,
        help=f"Seconds between messages in live mode (default: {DEFAULT_MESSAGE_DELAY}).",
    )
    parser.add_argument(
        "--channel-delay", type=float, default=DEFAULT_CHANNEL_DELAY,
        help=f"Seconds between channels in live mode (default: {DEFAULT_CHANNEL_DELAY}).",
    )
    parser.add_argument(
        "--demo", action="store_true",
        help="Run in demo mode — generates synthetic data, no Telegram auth needed.",
    )
    args = parser.parse_args()

    # Resolve --path relative to project root so it works regardless of
    # which directory the user calls the script from.
    data_path = str(PROJECT_ROOT / args.path)

    if args.demo:
        run_demo(data_path, args.limit)

    else:
        if not api_id_str or not api_hash:
            print(
                "ERROR: Tg_API_ID or Tg_API_HASH not found.\n"
                "Add them to your .env file at the project root."
            )
            sys.exit(1)

        try:
            from telethon import TelegramClient
        except ImportError:
            print("ERROR: telethon is not installed. Run: pip install telethon")
            sys.exit(1)

        api_id = int(api_id_str)
        session_path = str(PROJECT_ROOT / "telegram_scraper_session")
        client = TelegramClient(session_path, api_id, api_hash)
        logger.info("Telegram client initialised")

        async def _main() -> None:
            async with client:
                await scrape_all_channels(
                    client,
                    TARGET_CHANNELS,
                    data_path,
                    limit=args.limit,
                    message_delay=args.message_delay,
                    channel_delay=args.channel_delay,
                )

        asyncio.run(_main())