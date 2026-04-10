"""Database connection and helpers for OncoExtract."""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()


def get_connection_string() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "oncoextract")
    user = os.getenv("POSTGRES_USER", "oncoextract")
    password = os.getenv("POSTGRES_PASSWORD", "oncoextract_dev")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def get_jdbc_url() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "oncoextract")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def get_engine() -> Engine:
    return create_engine(get_connection_string())
