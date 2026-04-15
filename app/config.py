from typing import Literal, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # ── Database ──────────────────────────────────────────────────────────────
    database_url: str = "postgresql+asyncpg://earlyestate:earlyestate@localhost:5432/earlyestate"
    database_url_sync: str = "postgresql://earlyestate:earlyestate@localhost:5432/earlyestate"

    # ── Geocoding ─────────────────────────────────────────────────────────────
    geocoding_provider: Literal["nominatim", "google"] = "nominatim"
    nominatim_user_agent: str = "EarlyEstate/0.1"
    nominatim_base_url: str = "https://nominatim.openstreetmap.org"
    google_maps_api_key: Optional[str] = None

    # ── Enrichment APIs ───────────────────────────────────────────────────────
    north_data_api_key: Optional[str] = None
    sprengnetter_api_key: Optional[str] = None

    # ── Telegram ──────────────────────────────────────────────────────────────
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None

    # ── Email ─────────────────────────────────────────────────────────────────
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None
    alert_from_email: str = "alerts@earlyestate.local"

    # ── Webhook ───────────────────────────────────────────────────────────────
    webhook_url: Optional[str] = None
    webhook_secret: Optional[str] = None

    # ── onOffice CRM (optional) ───────────────────────────────────────────────
    onoffice_api_key: Optional[str] = None
    onoffice_api_secret: Optional[str] = None

    # ── Scheduler cadence ─────────────────────────────────────────────────────
    insolvency_poll_minutes: int = 30
    bank_portal_poll_hours: int = 24

    # ── Feature flags ─────────────────────────────────────────────────────────
    # ZVG portal: robots.txt disallows detail endpoints — disabled until legal review
    zvg_adapter_enabled: bool = False

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)


settings = Settings()
