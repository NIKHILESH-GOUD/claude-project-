## Real-Time E-Commerce Intelligence Platform
Real-time streaming analytics platform processing e-commerce events end-to-end from Kafka ingestion through Spark stream processing to a serving API and live dashboard.
An end-to-end data platform that simulates a live online store and processes
orders, clickstream, and inventory events in real time, built to demonstrate
Data Engineering (Kafka, Spark Structured Streaming, Airflow), Data Analytics
(dashboards, KPIs), and applied ML (demand forecasting) in one project.

## Architecture

```
 ┌─────────────┐     ┌───────┐     ┌─────────────────────────┐     ┌──────────────┐
 │  Producers   │ --> │ Kafka │ --> │ Spark Structured Stream  │ --> │ Postgres /   │
 │ orders /     │     │topics │     │ (joins, rolling metrics) │     │ Delta tables │
 │ clicks /     │     └───────┘     └─────────────────────────┘     └──────┬───────┘
 │ inventory    │                                                          │
 └─────────────┘                                                           v
                                                                  ┌──────────────────┐
      ┌────────────┐        ┌───────────────────┐                │ Power BI/Tableau │
      │  Airflow    │ -----> │ Demand forecasting│ --------------> │  dashboard       │
      │ (batch DAGs)│        │ + reorder model   │                └──────────────────┘
      └────────────┘        └───────────────────┘
                                     │
                                     v
                          ┌────────────────────┐
                          │ FastAPI ops endpoint│
                          │ (live alerts feed)  │
                          └────────────────────┘
```

## Status: Week 1 & 2 ✅

- [ ] Kafka + Zookeeper + Kafka UI + Postgres via Docker Compose
- [ ] Shared product catalog (`producers/catalog.py`)
- [ ] Order producer with realistic hourly traffic curve
- [ ] Clickstream producer (page views, product views, cart events)
- [ ] Inventory producer with low-stock / restock simulation
- [ ] Spark Structured Streaming job (`spark_jobs/streaming_job.py`) with 4 live outputs:
  - `revenue_per_minute` — tumbling 1-min window revenue/order metrics
  - `clickstream_funnel` — event counts per category/event_type per minute
  - `low_stock_alerts` — real-time filter on inventory events
  - `cart_abandonment` — **stream-stream join**: add-to-cart events with no matching order within 10 minutes
- [ ] Transformation logic unit-tested with static data (`spark_jobs/test_transformations.py`)
- [ ] Week 3 — Airflow DAGs: ETL, demand forecasting retrain, reorder point calc
- [ ] Week 4 — Power BI/Tableau dashboard + FastAPI ops endpoint + demo

## Week 2 quickstart

1. **Create the Postgres tables** the job writes to:
   ```bash
   docker exec -i postgres psql -U ecom -d ecommerce < spark_jobs/init_postgres_tables.sql
   ```

2. **Validate the transformation logic locally** (no Kafka needed — uses static test data):
   ```bash
   cd spark_jobs
   python test_transformations.py
   ```

3. **Run the real streaming job** (needs producers running from Week 1, plus the Kafka + Postgres JDBC connector jars):
   ```bash
   spark-submit \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
     spark_jobs/streaming_job.py
   ```
   Spark will download the two connector packages on first run (needs internet access).
   Query the Postgres tables (e.g. `SELECT * FROM revenue_per_minute ORDER BY window_start DESC;`)
   to see live results land every 15-30 seconds.

## Quickstart (Week 1)

1. **Start infrastructure**
   ```bash
   docker compose up -d
   ```
   This brings up Kafka, Zookeeper, Kafka UI (http://localhost:8080), and Postgres.

2. **Install Python dependencies**
   ```bash
   python -m venv venv
   source venv/bin/activate   # or venv\Scripts\activate on Windows
   pip install -r requirements.txt
   ```

3. **Create Kafka topics**
   ```bash
   python producers/create_topics.py
   ```

4. **Start streaming events**
   ```bash
   cd producers
   python run_all.py
   ```
   You should see interleaved `[orders]`, `[clickstream]`, and `[inventory]` log
   lines. Open http://localhost:8080 (Kafka UI) to watch messages land in each
   topic in real time.

## Why this project

Most portfolio projects stop at "trained a model on a static CSV." This one
demonstrates the full lifecycle a real data team owns:

- **Data Engineering**: Kafka topic design, Spark Structured Streaming,
  Airflow orchestration, Delta/Postgres storage
- **Data Analysis**: live KPI dashboards (revenue, conversion, inventory health)
- **Data Science**: demand forecasting + product recommendations
- **Applied engineering**: FastAPI service exposing live alerts

## Repo structure

```
ecommerce-intelligence-platform/
├── docker-compose.yml       # Kafka, Zookeeper, Kafka UI, Postgres
├── requirements.txt
├── producers/                # Week 1
│   ├── catalog.py
│   ├── create_topics.py
│   ├── order_producer.py
│   ├── clickstream_producer.py
│   ├── inventory_producer.py
│   └── run_all.py
├── spark_jobs/                # Week 2
├── airflow/dags/              # Week 3
├── dashboard/                 # Week 4
├── api/                        # Week 4 (FastAPI)
├── data/
└── docs/
```
