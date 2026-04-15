"""Database connection and helpers for OncoExtract."""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL, make_url

load_dotenv()


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
    return f"jdbc:postgresql://{host}:{port}/{db}"


def get_engine() -> Engine:
    return create_engine(get_connection_url())
