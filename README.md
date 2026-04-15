# EarlyEstate

Early-stage real estate distress screener for Germany. Surfaces distressed-asset signals (insolvency publications, bank ZV listings) before they hit the mass market.

**Cost: €0/month** — runs fully on GitHub Actions + Supabase free tier.

---

## How it works

```
Insolvency portal (every 2h)  ──┐
Sparkasse / LBS (daily)        ├─► Parse ► Entity Resolution ► Geocode ► Match ► Telegram alert
ZVG portal (disabled*)         ──┘
```

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

### 4. Run the DB migration

Go to **Actions → DB Migrate → Run workflow** → click "Run workflow".

This runs `alembic upgrade head` + seeds the source records. Takes ~1 minute.

### 5. You're live

The workflows will now run automatically:

| Workflow | Schedule | What it does |
|---|---|---|
| `insolvency.yml` | Every 2 hours | Scrapes insolvency portal for all 16 states |
| `bank_portals.yml` | Daily 06:00 UTC | Scrapes Sparkasse + LBS for ZV listings |
| `purge.yml` | Weekly Sunday 03:00 UTC | InsBekV § 3 retention purge |

You can also trigger any workflow manually via **Actions → [workflow] → Run workflow**.

---

## Upgrade to 30-minute insolvency cadence (free)

The default is every 2 hours to stay within the 2,000 free minutes/month limit for **private repos**.

To get 30-minute cadence for free:
1. Make the repo public (Settings → Danger Zone → Change visibility)
2. Edit `.github/workflows/insolvency.yml`, change the cron to `'*/30 * * * *'`
3. Commit and push

GitHub Actions is **unlimited for public repos**. Your secrets remain private.

---

## Local development

```bash
# Copy env file
cp .env.example .env
# Edit .env with your Supabase connection strings

# Install deps
make install

# Start local Postgres (alternative to Supabase)
make up          # docker compose up -d db

# Migrate + seed
make migrate
make seed

# Run API
make dev

# Run worker
make worker

# Run tests
make test
```

---

## Architecture

```
app/
├── adapters/       # Scraping adapters (insolvency, sparkasse, lbs, zvg*)
├── pipeline/       # ER, geocoder, enrichment, matcher, alerter, auditor
├── alerts/         # Telegram, email, webhook channels
├── api/            # FastAPI routes (sources, events, leads, matches, admin)
├── models/         # SQLAlchemy models (PostGIS)
└── jobs/           # run_once.py (GHA entrypoint), runner.py, seed.py

.github/workflows/
├── insolvency.yml  # Every 2h
├── bank_portals.yml # Daily
├── purge.yml       # Weekly
└── migrate.yml     # Manual only
```

## Compliance notes

- **ZVG portal**: disabled by default (`ZVG_ADAPTER_ENABLED=false`). `robots.txt` disallows detail endpoints. Enable only after legal review.
- **InsBekV § 3**: automated weekly purge deletes personal data from insolvency records older than 6 months.
- **GDPR**: purpose-limited to risk screening / professional investor workflow. Data minimization enforced — raw HTML not stored.
- **Grundbuch**: ownership matching requires permissioned access (GBO § 12). Not implemented in MVP.
