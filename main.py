import random
from datetime import datetime, timedelta
from contextlib import asynccontextmanager 
from fastapi import FastAPI, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Union
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime, Float, UniqueConstraint, func, and_
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from sqlalchemy.orm import Session

SQLALCHEMY_DATABASE_URL = "sqlite:///./sql_app.db" 

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False} 
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Grid(Base):
    __tablename__ = "grids" 
    id = Column(Integer, primary_key=True, index=True) 
    name = Column(String, unique=True, index=True) 
    regions = relationship("GridRegion", back_populates="grid")

class GridRegion(Base):
    __tablename__ = "grid_regions"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    grid_id = Column(Integer, ForeignKey("grids.id")) 
    grid = relationship("Grid", back_populates="regions")
    nodes = relationship("GridNode", back_populates="region")

    __table_args__ = (UniqueConstraint('name', 'grid_id', name='_name_grid_uc'),)

class GridNode(Base):
    __tablename__ = "grid_nodes"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    grid_region_id = Column(Integer, ForeignKey("grid_regions.id")) 
    region = relationship("GridRegion", back_populates="nodes")
    measures = relationship("Measure", back_populates="node")

    __table_args__ = (UniqueConstraint('name', 'grid_region_id', name='_name_region_uc'),)

class Measure(Base):
   
    __tablename__ = "measures"
    id = Column(Integer, primary_key=True, index=True)
    grid_node_id = Column(Integer, ForeignKey("grid_nodes.id")) 
    timestamp = Column(DateTime, index=True) 
    collected_datetime = Column(DateTime, index=True) 
    value = Column(Float) 
    node = relationship("GridNode", back_populates="measures")

    __table_args__ = (UniqueConstraint('grid_node_id', 'timestamp', 'collected_datetime', name='_node_ts_collected_uc'),)


def create_db_and_tables():
    Base.metadata.create_all(bind=engine)
    print("Database tables created.")

_global_unique_microsecond_counter = 0

def insert_initial_data(db: Session):
    print("Inserting initial Grid, Region, Node data...")
    grid1 = Grid(name="Grid1")
    grid2 = Grid(name="Grid2")
    grid3 = Grid(name="Grid3")
    db.add_all([grid1, grid2, grid3])
    db.commit() 
    db.refresh(grid1) 
    db.refresh(grid2)
    db.refresh(grid3)

    region1_g1 = GridRegion(name="Region1", grid_id=grid1.id)
    region2_g1 = GridRegion(name="Region2", grid_id=grid1.id)
    region3_g1 = GridRegion(name="Region3", grid_id=grid1.id)

    region1_g2 = GridRegion(name="Region1", grid_id=grid2.id)
    region2_g2 = GridRegion(name="Region2", grid_id=grid2.id)
    region3_g2 = GridRegion(name="Region3", grid_id=grid2.id)

    region1_g3 = GridRegion(name="Region1", grid_id=grid3.id)
    region2_g3 = GridRegion(name="Region2", grid_id=grid3.id)
    region3_g3 = GridRegion(name="Region3", grid_id=grid3.id)
    db.add_all([
        region1_g1, region2_g1, region3_g1,
        region1_g2, region2_g2, region3_g2,
        region1_g3, region2_g3, region3_g3
    ])
    db.commit()
    db.refresh(region1_g1)
    db.refresh(region2_g1)
    db.refresh(region3_g1)
    db.refresh(region1_g2)
    db.refresh(region2_g2)
    db.refresh(region3_g2)
    db.refresh(region1_g3)
    db.refresh(region2_g3)
    db.refresh(region3_g3)

    nodes = []
    for region in [
        region1_g1, region2_g1, region3_g1,
        region1_g2, region2_g2, region3_g2,
        region1_g3, region2_g3, region3_g3
    ]:
        for i in range(1, 4):
            nodes.append(GridNode(name=f"Node{i}", grid_region_id=region.id))
    db.add_all(nodes)
    db.commit()
    print("Successfully inserted initial Grid, Region, and Node data.")

def insert_measures_data(db: Session, num_weeks: int = 1):
    global _global_unique_microsecond_counter
    print(f"Inserting {num_weeks} week(s) of measure data...")
    all_nodes = db.query(GridNode).all()
    if not all_nodes:
        print("No grid nodes found in the database. Please ensure initial data is inserted first.")
        return

    start_timestamp_for_measures = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(days=num_weeks * 7)
    end_timestamp_for_measures = start_timestamp_for_measures + timedelta(days=num_weeks * 7)

    measures_to_insert = []

    current_timestamp = start_timestamp_for_measures
    while current_timestamp < end_timestamp_for_measures:
        for node in all_nodes:
            _global_unique_microsecond_counter += 1
            random_ms_offset_1 = random.randint(0, 999)
            collected_time_1 = current_timestamp + timedelta(microseconds=_global_unique_microsecond_counter, milliseconds=random_ms_offset_1)
            measures_to_insert.append(Measure(
                grid_node_id=node.id,
                timestamp=current_timestamp,
                collected_datetime=collected_time_1,
                value=round(random.uniform(90.0, 110.0), 2)
            ))

            _global_unique_microsecond_counter += 1
            random_ms_offset_2 = random.randint(0, 999)
            collected_time_2 = current_timestamp - timedelta(hours=2) + timedelta(microseconds=_global_unique_microsecond_counter, milliseconds=random_ms_offset_2)
            measures_to_insert.append(Measure(
                grid_node_id=node.id,
                timestamp=current_timestamp,
                collected_datetime=collected_time_2,
                value=round(random.uniform(90.0, 110.0), 2)
            ))

            _global_unique_microsecond_counter += 1
            random_ms_offset_3 = random.randint(0, 999)
            collected_time_3 = current_timestamp - timedelta(hours=1) + timedelta(microseconds=_global_unique_microsecond_counter, milliseconds=random_ms_offset_3)
            measures_to_insert.append(Measure(
                grid_node_id=node.id,
                timestamp=current_timestamp,
                collected_datetime=collected_time_3,
                value=round(random.uniform(90.0, 110.0), 2)
            ))

            for _ in range(random.randint(0, 2)):
                _global_unique_microsecond_counter += 1
                random_ms_offset_future = random.randint(0, 999)
                future_collected_time = current_timestamp + timedelta(minutes=random.randint(1, 120)) + timedelta(microseconds=_global_unique_microsecond_counter, milliseconds=random_ms_offset_future)
                measures_to_insert.append(Measure(
                    grid_node_id=node.id,
                    timestamp=current_timestamp,
                    collected_datetime=future_collected_time,
                    value=round(random.uniform(85.0, 115.0), 2)
                ))

        current_timestamp += timedelta(hours=1) 

    db.bulk_save_objects(measures_to_insert)
    db.commit()
    print(f"Inserted {len(measures_to_insert)} measure records for {num_weeks} week(s).")

# FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables() 
    db = SessionLocal() 
    try:
        if db.query(Grid).first() is None:
            print("Database is empty. Inserting initial data and 1 week of measures data")
            insert_initial_data(db)
            insert_measures_data(db, num_weeks=1) 
        else:
            print("Existing data found. Skipping initial data insertion.")
    except Exception as e:
        print(f"Startup data insertion failed: {e}")
    finally:
        db.close() 
    yield 
    print("FastAPI application is shutting down.")

app = FastAPI(
    title="Grid Node Measures API",
    description="API to retrieve hourly timeseries values of Grid Nodes with evolution support.",
    lifespan=lifespan 
)

def get_db():
    db = SessionLocal()
    try:
        yield db 
    finally:
        db.close() 

class MeasureResponse(BaseModel):
    grid_node_name: str
    timestamp: datetime
    value: float
    collected_datetime: datetime 

@app.get("/latest_values", response_model=List[MeasureResponse],
         summary="Get the latest value for each timestamp in a date range.")
async def get_latest_values(
    start_datetime: datetime = Query(..., description="Start datetime (e.g., 2023-01-01T00:00:00)"),
    end_datetime: datetime = Query(..., description="End datetime (e.2023-01-08T00:00:00)"),
    db: Session = Depends(get_db)
):
    print(f"Fetching measures from {start_datetime} to {end_datetime}")
    all_measures = db.query(
        GridNode.name,
        Measure.timestamp,
        Measure.value,
        Measure.collected_datetime,
        Measure.id 
    ).join(Measure, GridNode.id == Measure.grid_node_id)\
     .filter(
        Measure.timestamp >= start_datetime,
        Measure.timestamp < end_datetime
     ).all()

    print(f"Fetched {len(all_measures)} raw measures.")

    all_measures.sort(
        key=lambda r: (r.name, r.timestamp, r.collected_datetime, r.id),
        reverse=True 
    )

    print("- - - Sorted Measures (Top 50 for debugging) - -  -")
    for i, r in enumerate(all_measures):
        if i >= 50: 
            break
        print(f"  Sorted: Node={r.name}, TS={r.timestamp}, Value={r.value}, Collected={r.collected_datetime}, ID={r.id}")
    print("- - - - - - - - - -")


    latest_values_map = {} 

    for r in all_measures:
        key = (r.name, r.timestamp)
        if key not in latest_values_map:
            latest_values_map[key] = MeasureResponse(
                grid_node_name=r.name,
                timestamp=r.timestamp,
                value=r.value,
                collected_datetime=r.collected_datetime
            )

    print(f"Final map contains {len(latest_values_map)} unique (node, timestamp) entries.")
    
    response_list = sorted(
        list(latest_values_map.values()),
        key=lambda x: (x.grid_node_name, x.timestamp)
    )

    if not response_list:
        raise HTTPException(status_code=404, detail="No data found for the specified date range.")

    return response_list

@app.get("/collected_values", response_model=List[MeasureResponse],
         summary="Get values for each timestamp corresponding to a specific collected datetime.")
async def get_collected_values(
    start_datetime: datetime = Query(..., description="Start datetime (e.g., 2025-07-12T16:00:00)"),
    end_datetime: datetime = Query(..., description="End datetime (e.g., 2025-07-19T16:00:00)"),
    collected_datetime: datetime = Query(..., description="The specific collected datetime (e.g., 2025-07-12T16:00:00.000000). Must match exactly.")
    ,db: Session = Depends(get_db)
):
    print(f"DEBUG: get_collected_values received:")
    print(f"  start_datetime: {start_datetime}")
    print(f"  end_datetime: {end_datetime}")
    print(f"  collected_datetime: {collected_datetime}")

    results = db.query(
        GridNode.name,
        Measure.timestamp,
        Measure.value,
        Measure.collected_datetime
    ).join(Measure, GridNode.id == Measure.grid_node_id)\
     .filter(
        Measure.timestamp >= start_datetime,
        Measure.timestamp < end_datetime,
        Measure.collected_datetime == collected_datetime 
     ).order_by(GridNode.name, Measure.timestamp).all() 

    if not results:
        print(f"DEBUG: No results found for collected_datetime={collected_datetime}")
        raise HTTPException(status_code=404, detail="No data found for the specified criteria.")

    return [
        MeasureResponse(
            grid_node_name=r.name,
            timestamp=r.timestamp,
            value=r.value,
            collected_datetime=r.collected_datetime
        ) for r in results
    ]
