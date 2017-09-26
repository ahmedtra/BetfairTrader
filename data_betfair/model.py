from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

from sqlalchemy import Column, Date, Boolean, Enum, Float, ForeignKey, Index, Integer, String, text, DateTime, Time, \
    SmallInteger

class EventTypes(Base):
    __tablename__ = "betfair_data"
    Id = Column(Integer, primary_key=True)
    SPORTS_ID = Column(Float)
    EVENT_ID = Column(Float)
    SETTLED_DATE = Column(Integer)
    FULL_DESCRIPTION = Column(String)
    SCHEDULED_OFF = Column(DateTime)
    EVENT = Column(String)
    SELECTION_ID = Column(Float)
    SELECTION = Column(String)
    ODDS = Column(Float)
    NUMBER_BETS = Column(Float)
    VOLUME_MATCHED = Column(Float)
    LATEST_TAKEN = Column(DateTime)
    FIRST_TAKEN = Column(DateTime)
    WIN_FLAG = Column(Float)
    IN_PLAY = Column(String)
    COMPETITION_TYPE = Column(String)
    COMPETITION = Column(String)
    FIXTURES = Column(String)
    EVENT_NAME = Column(String)
    MARKET_TYPE = Column(String)
