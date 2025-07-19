
# Python Coding Challenge – Grid Time-Series Data Management

Questions:

- Design a database schema to store:
  - Grids (TSO/ISOs): `Grid1`, `Grid2`, `Grid3`
  - Regions under each grid: `Region1`, `Region2`, `Region3`
  - Nodes under each region: `Node1`, `Node2`, `Node3`
- Store **hourly time-series values** for each node
- Support **time-series evolution** (e.g., multiple values for the same timestamp collected at different times)
- Insert **1 week of sample data**
- Build **2 REST APIs**:
  1. Return the **latest value** for each timestamp in a given time range
  2. Return the **value as of a specific collected time**

---
Database Schema

The schema is built using PostgreSQL and includes the following tables:

- `grid` – Represents ISO/TSOs (e.g., Grid1, Grid2)
- `grid_region` – Regions under each grid
- `grid_node` – Nodes under each region
- `measures` – Time-series data for each node, with support for evolution

Measures Table

This is the core of the challenge:

```sql
CREATE TABLE measures (
    id SERIAL PRIMARY KEY,
    grid_node_id INTEGER NOT NULL REFERENCES grid_node(id),
    timestamp TIMESTAMP NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    value NUMERIC NOT NULL,
    UNIQUE (grid_node_id, timestamp, collected_at)
);
