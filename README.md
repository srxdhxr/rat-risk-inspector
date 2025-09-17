# Dashboard API

A lightweight FastAPI application that connects to MotherDuck and provides data for dashboards.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set environment variables:
```bash
export MOTHERDUCK_TOKEN="your_motherduck_token_here"
export MOTHERDUCK_DATABASE="main"
```

3. Run the application:
```bash
python dashboard_api.py
```

The API will be available at `http://localhost:8000`

## API Endpoints

- `GET /` - Health check
- `GET /health` - Detailed health check with database connectivity
- `GET /tables` - List all available tables
- `GET /data/{table_name}` - Get data from a specific table with pagination
- `POST /query` - Execute custom SQL queries (SELECT only)
- `GET /stats` - Get database statistics

## Usage Examples

### Get all tables
```bash
curl http://localhost:8000/tables
```

### Get data from a table
```bash
curl "http://localhost:8000/data/my_table?limit=50&offset=0"
```

### Execute custom query
```bash
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM my_table WHERE id > 100 LIMIT 10"}'
```

## Features

- Lightweight and fast
- CORS enabled for frontend integration
- Pagination support
- SQL injection protection
- Error handling and logging
- Health checks
