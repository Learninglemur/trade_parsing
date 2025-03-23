from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional, List, Union
from sqlmodel import Field, SQLModel, Relationship


# Enums
class TradeSide(str, PyEnum):
    BUY = "BUY"
    SELL = "SELL"


class TradeStatus(str, PyEnum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"


class OptionType(str, PyEnum):
    CALL = "CALL"
    PUT = "PUT"


# Base models
class UserBase(SQLModel):
    email: str
    name: Optional[str] = None
    password: str


# Models with relationships
class User(UserBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    # Relationships
    trades: List["Trade"] = Relationship(back_populates="user")


class TradeBase(SQLModel):
    timestamp: datetime
    date: str  # Date portion (YYYY-MM-DD)
    time: Optional[str] = None  # Time portion (HH:MM:SS)
    symbol: str
    price: float
    quantity: float
    side: TradeSide
    status: TradeStatus = TradeStatus.COMPLETED
    commission: Optional[float] = 0.0
    net_proceeds: Optional[float] = 0.0
    is_option: bool = False
    option_type: Optional[str] = None  # Changed from Union[OptionType, None] to String to avoid validation errors
    strike_price: Optional[float] = None
    expiry_date: Optional[datetime] = None
    description: Optional[str] = None
    broker_type: str
    user_id: int


class Trade(TradeBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    dte: Optional[int] = None

    # Relationships
    user_id: Optional[int] = Field(default=None, foreign_key="user.id")
    user: Optional[User] = Relationship(back_populates="trades")


# OHLCVData model
class OHLCVDataBase(SQLModel):
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: datetime


class OHLCVData(OHLCVDataBase, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    class Config:
        table_name = "ohlcvdata"
        indexes = [
            ("symbol", "timestamp", "unique"),
            ("timestamp",),
            ("symbol",)
        ]


# READ models for API responses
class UserRead(UserBase):
    id: int
    created_at: datetime
    updated_at: datetime


class TradeRead(TradeBase):
    id: int
    dte: Optional[int] = None


# CREATE models for API requests
class UserCreate(UserBase):
    pass


class TradeCreate(TradeBase):
    pass 