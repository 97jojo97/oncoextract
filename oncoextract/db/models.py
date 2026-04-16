"""Database connection and helpers for OncoExtract."""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL, make_url

load_dotenv()


def _postgres_host_for_ssl() -> str:
    """Host used to decide sslmode (Streamlit Cloud often sets only DATABASE_URL)."""
    host = (os.getenv("POSTGRES_HOST") or "").strip().lower()
    if host:
        return host
    for key in ("DATABASE_URL", "POSTGRES_URL"):
        raw = os.getenv(key)
        if not raw:
            continue
        try:
            h = (make_url(raw).host or "").strip().lower()
        except Exception:
            continue
        if h:
            return h
    return "localhost"


def postgres_sslmode() -> str:
    """psycopg2 sslmode. Neon/RDS require TLS; local Docker Postgres usually has no SSL."""
    override = os.getenv("POSTGRES_SSLMODE", "").strip()
    if override:
        return override
    host = _postgres_host_for_ssl()
    if host in ("localhost", "127.0.0.1", "::1"):
        return "disable"
    return "require"


def get_connection_url() -> URL:
    """Build a SQLAlchemy URL so special characters in the password stay valid."""
    for key in ("DATABASE_URL", "POSTGRES_URL"):
        raw = os.getenv(key)
        if raw:
            return make_url(raw)

    return URL.create(
        "postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER", "oncoextract"),
        password=os.getenv("POSTGRES_PASSWORD", "oncoextract_dev"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "oncoextract"),
    )


def get_connection_string() -> str:
    return get_connection_url().render_as_string(hide_password=False)


def get_jdbc_url() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "oncoextract")
    base = f"jdbc:postgresql://{host}:{port}/{db}"
    if postgres_sslmode() != "disable":
        return f"{base}?sslmode={postgres_sslmode()}"
    return base


def get_engine() -> Engine:
    url = get_connection_url()
    # sslmode for Neon / managed Postgres (also fixes "connection is insecure").
    return create_engine(url, connect_args={"sslmode": postgres_sslmode()})


