from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

from sqlalchemy import Column, Date, Boolean, Enum, Float, ForeignKey, Index, Integer, String, text, DateTime, Time, \
    SmallInteger

class EventTypes(Base):
    __tablename__ = "event_types"
    event_type_id = Column(String, primary_key=True)
    name = Column(String)
    market_count = Column(Integer)

class Competitions(Base):
    __tablename__ = "competitions"
    competition_id = Column(String, primary_key=True)
    name = Column(String)
    competition_region = Column(String)
    market_count  = Column(Integer)
    event_type_id = Column(String)

class Events(Base):
    __tablename__ = "events"
    event_id = Column(String, primary_key=True)
    name = Column(String)
    country_code = Column(String)
    timezone = Column(String)
    venue = Column(String)
    open_date = Column(DateTime)
    market_count = Column(Integer)
    competition_id = Column(String)

class Markets(Base):
    __tablename__ = "markets"
    market_id = Column(String, primary_key=True)
    market_name = Column(String)
    market_start_time = Column(DateTime)
    description = Column(String)
    event_id = Column(String)


class Runners(Base):
    __tablename__ = "runners"
    selection_id = Column(Integer, primary_key=True)
    runner_name = Column(String)
    handicap = Column(Float)
    sort_priority = Column(Integer)
    meta_data_id = Column(Integer)

class RunnersMap(Base):
    __tablename__ = "runners_map"
    id = Column(Integer, primary_key=True)
    market_id = Column(String)
    selection_id = Column(Integer)

class Strategies(Base):
    __tablename__ = "strategies"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    event = Column(String)
    event_name = Column(String)
    status = Column(String)


class Orders(Base):
    __tablename__  = "orders"
    id = Column(Integer, primary_key=True)
    strategy_id = Column(Integer)
    bet_id = Column(Integer)
    size = Column(Float)
    selection_id = Column(Integer)
    market_id = Column(Integer)
    price = Column(Float)
    side = Column(String)
    executed = Column(Float)
    average_price = Column(Float)
    ref = Column(String)
    state = Column(String)

class States(Base):
    __tablename__ = "states"
    id = Column(Integer, primary_key=True)
    strategy_id = Column(Integer)
    time = Column(DateTime)
    key = Column(String)
    value = Column(String)
