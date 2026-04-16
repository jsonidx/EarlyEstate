# EarlyEstate

Early-stage real estate distress screener for Germany. Surfaces distressed-asset signals (insolvency publications, bank ZV listings) before they hit the mass market.

**Cost: €0/month** — runs fully on GitHub Actions + Supabase free tier.

---

## How it works

```
Insolvency portal (every 30 min)  ──┐
Immowelt ZV listings (daily 07:00) ─┤─► Parse ► Entity Resolution ► Geocode ► Match ► Telegram alert
ZVG portal (disabled*)             ──┘
```

Scoring signals (0–100):

| Signal | Points | Fires when |
|---|---|---|
| Name similarity | 0–40 | Party name fuzzy-matches listing title |
| Geo distance | 0–30 | PostGIS distance (or 15 pts city match, 20 pts PLZ match) |
| Auction signal terms | 0–20 | "zwangsversteigerung", "verkehrswert" etc. in listing |
| Register ID match | 0–10 | HRB/HRA number exact match |
| Court jurisdiction | 0–15 | Insolvency court city = ZV listing city |

Score ≥ 80 → **HIGH** → instant Telegram alert  
Score 50–79 → **MEDIUM** → instant Telegram alert  
Score 20–49 → **LOW** → queued for daily digest  

---

## Setup (one-time, ~10 minutes)

### 1. Create a free Supabase project

1. Go to [supabase.com](https://supabase.com) → New project
2. Choose a region close to Germany (e.g. `eu-central-1 Frankfurt`)
3. Set a strong database password — save it
4. Wait for the project to provision (~2 min)
5. Go to **Settings → Database → Connection string**
   - Copy the **URI** (starts with `postgresql://postgres:...`)
   - Also copy the **URI** section under "Connection pooling" for async use

You need two connection strings:
- `DATABASE_URL` — async driver (replace `postgresql://` with `postgresql+asyncpg://`)
- `DATABASE_URL_SYNC` — sync driver, keep as `postgresql://` (for Alembic)

Example:
```
DATABASE_URL=postgresql+asyncpg://postgres:[PASSWORD]@db.[REF].supabase.co:5432/postgres
DATABASE_URL_SYNC=postgresql://postgres:[PASSWORD]@db.[REF].supabase.co:5432/postgres
```

### 2. Create a Telegram bot (for alerts)

1. Open Telegram → search `@BotFather` → `/newbot`
2. Follow prompts, copy the **bot token**
3. Start a chat with your new bot, then visit:
   `https://api.telegram.org/bot<TOKEN>/getUpdates`
4. Send any message to the bot, refresh the URL — copy your `chat.id`

### 3. Add GitHub Secrets

Go to your repo → **Settings → Secrets and variables → Actions → New repository secret**

| Secret name | Value |
|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://postgres:[PASSWORD]@db.[REF].supabase.co:5432/postgres` |
| `DATABASE_URL_SYNC` | `postgresql://postgres:[PASSWORD]@db.[REF].supabase.co:5432/postgres` |
| `TELEGRAM_BOT_TOKEN` | Your bot token from BotFather |
| `TELEGRAM_CHAT_ID` | Your chat ID from getUpdates |

Optional (for email alerts):

| Secret name | Value |
|---|---|
| `SMTP_HOST` | `smtp.gmail.com` |
| `SMTP_PORT` | `587` |
| `SMTP_USER` | your Gmail address |
| `SMTP_PASSWORD` | [Gmail App Password](https://myaccount.google.com/apppasswords) |

Optional (North Data enrichment — improves PLZ matching and register ID lookup):

| Secret name | Value |
|---|---|
| `NORTH_DATA_API_KEY` | API key from northdata.de |

### 4. Run the DB migration

Go to **Actions → DB Migrate → Run workflow** → click "Run workflow".

This runs `alembic upgrade head` + seeds the source records. Takes ~1 minute.

### 5. You're live

The workflows will now run automatically:

| Workflow | Schedule | What it does |
|---|---|---|
| `insolvency.yml` | Every 30 min (weekdays) | Scrapes insolvency portal for all 16 states |
| `bank_portals.yml` | Daily 07:00 UTC | Scrapes Immowelt ZV listings + runs matcher |
| `digest.yml` | Daily 08:00 UTC | Sends daily Telegram digest of top LOW matches |
| `purge.yml` | Weekly Sunday 03:00 UTC | InsBekV § 3 retention purge + expire stale matches |

You can also trigger any workflow manually via **Actions → [workflow] → Run workflow**.

---

## API (optional)

Deploy to Railway for a read-only review UI and status endpoint:

1. Create a new project at [railway.app](https://railway.app)
2. Connect your GitHub repo
3. Set environment variables: `DATABASE_URL`, and optionally `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
4. Railway auto-deploys on push

Key endpoints:

| Endpoint | Description |
|---|---|
| `GET /matches?status=OPEN&min_score=50` | List match candidates |
| `GET /matches/{id}` | Full enriched match detail (party + event + lead) |
| `PATCH /matches/{id}` | Update status (CONFIRMED / REJECTED) |
| `GET /leads?city=München` | Browse ZV asset leads |
| `GET /events?event_type=INSOLVENCY_PUBLICATION` | Browse insolvency events |
| `GET /health` | Health check |
| `GET /docs` | Interactive API docs (Swagger UI) |

---

## Local development

```bash
# Copy env file
cp .env.example .env
# Edit .env with your Supabase connection strings

# Install deps
pip install -e ".[dev]"

# Start local Postgres (alternative to Supabase)
docker compose up -d db

# Migrate + seed
alembic upgrade head
python -m app.jobs.run_once --seed

# Run API
uvicorn app.api.main:app --reload

# Run tests
pytest
```

---

## Architecture

```
app/
├── adapters/       # Scraping adapters (insolvency portal, immowelt ZV, zvg*)
├── pipeline/       # ER, geocoder, enrichment, matcher, alerter, auditor
├── alerts/         # Telegram, email, webhook channels
├── api/            # FastAPI routes (sources, events, leads, matches, admin)
├── models/         # SQLAlchemy models (PostGIS)
└── jobs/           # run_once.py (GHA entrypoint), runner.py, seed.py

.github/workflows/
├── insolvency.yml    # Every 30 min weekdays / 2h weekends
├── bank_portals.yml  # Daily 07:00 UTC (Immowelt ZV scrape + match)
├── digest.yml        # Daily 08:00 UTC (Telegram digest)
├── purge.yml         # Weekly (retention purge + stale match expiry)
├── migrate.yml       # Manual only
└── stats.yml         # Manual — prints DB row counts
```

## Compliance notes

- **ZVG portal**: disabled by default (`ZVG_ADAPTER_ENABLED=false`). `robots.txt` disallows detail endpoints. Enable only after legal review.
- **InsBekV § 3**: automated weekly purge deletes personal data from insolvency records older than 6 months.
- **GDPR**: purpose-limited to risk screening / professional investor workflow. Data minimization enforced — raw HTML not stored.
- **Grundbuch**: ownership matching requires permissioned access (GBO § 12). Not implemented in MVP.
