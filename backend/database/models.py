from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy import String, Integer, DateTime, ForeignKey, JSON, Text, Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    user_id: Mapped[str] = mapped_column(String, primary_key=True)
    email: Mapped[str] = mapped_column(String, unique=True, index=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String, nullable=False)

    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Dataset(Base):
    __tablename__ = "datasets"

    dataset_id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str] = mapped_column(String, ForeignKey("users.user_id"), nullable=False)  # ✅

    original_dataset_id: Mapped[str] = mapped_column(String, nullable=False)
    file_name: Mapped[str] = mapped_column(String, nullable=False)
    sheet_name: Mapped[str] = mapped_column(String, nullable=False)

    n_rows: Mapped[int] = mapped_column(Integer, nullable=False)
    n_cols: Mapped[int] = mapped_column(Integer, nullable=False)
    dtypes: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)

    bucket: Mapped[str] = mapped_column(String, nullable=False)
    raw_key: Mapped[str] = mapped_column(String, nullable=False)

    raw_parquet_key: Mapped[str] = mapped_column(String, nullable=False)      # ✅ NEW
    current_parquet_key: Mapped[str] = mapped_column(String, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class Profile(Base):
    __tablename__ = "profiles"

    profile_id: Mapped[str] = mapped_column(String, primary_key=True)
    dataset_id: Mapped[str] = mapped_column(
        String, ForeignKey("datasets.dataset_id"), nullable=False
    )

    bucket: Mapped[str] = mapped_column(String, nullable=False, default="local")
    report_key: Mapped[str] = mapped_column(String, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class CleaningRun(Base):
    __tablename__ = "cleaning_runs"

    run_id: Mapped[str] = mapped_column(String, primary_key=True)

    user_id: Mapped[str] = mapped_column(
        String, ForeignKey("users.user_id"), nullable=False, index=True
    )

    dataset_id: Mapped[str] = mapped_column(
        String, ForeignKey("datasets.dataset_id"), nullable=False, index=True
    )

    status: Mapped[str] = mapped_column(String, nullable=False, default="queued")
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    bucket: Mapped[str] = mapped_column(String, nullable=False)
    report_key: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    cleaned_parquet_key: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    cleaned_xlsx_key: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)