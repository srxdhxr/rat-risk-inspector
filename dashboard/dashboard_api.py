from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import os
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Dashboard API", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MotherDuck connection configuration
MOTHERDUCK_TOKEN = os.getenv("MD_TOKEN")
MOTHERDUCK_DATABASE = os.getenv("MOTHERDUCK_DATABASE", "MY_WH")

def get_connection():
    """Create and return a DuckDB connection to MotherDuck"""
    try:
        conn = duckdb.connect(f"md:{MOTHERDUCK_DATABASE}")
        if MOTHERDUCK_TOKEN:
            conn.execute(f"SET motherduck_token='{MOTHERDUCK_TOKEN}'")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to MotherDuck: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Dashboard API is running", "status": "healthy"}

@app.get("/health")
async def health_check():
    """Detailed health check with database connectivity"""
    try:
        conn = get_connection()
        conn.execute("SELECT 1")
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}

@app.get("/tables")
async def get_tables():
    """Get list of available tables"""
    try:
        conn = get_connection()
        # For MotherDuck, try to get tables from information_schema
        try:
            result = conn.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name
            """).fetchall()
            tables = [f"{row[0]}.{row[1]}" for row in result]
        except:
            # Fallback: try common table names
            common_tables = [
                "main_mart.fact_restaurant_rat",
                "main.fact_restaurant_rat", 
                "mart.fact_restaurant_rat"
            ]
            tables = []
            for table in common_tables:
                try:
                    conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
                    tables.append(table)
                except:
                    continue
        
        conn.close()
        return {"tables": tables}
    except Exception as e:
        logger.error(f"Error fetching tables: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch tables: {str(e)}")

@app.get("/data/{table_name}")
async def get_table_data(table_name: str, limit: int = 100, offset: int = 0):
    """Get data from a specific table with pagination"""
    try:
        conn = get_connection()
        
        # Handle schema.table format or just table name
        if '.' in table_name:
            full_table_name = table_name
        else:
            # For MotherDuck, try common schema patterns
            common_schemas = ['main', 'main_mart', 'mart', 'public']
            table_found = False
            
            for schema in common_schemas:
                try:
                    # Try to query the table directly
                    conn.execute(f"SELECT 1 FROM {schema}.{table_name} LIMIT 1")
                    full_table_name = f"{schema}.{table_name}"
                    table_found = True
                    break
                except:
                    continue
            
            if not table_found:
                raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in common schemas")
        
        # Validate table exists by trying to query it
        try:
            conn.execute(f"SELECT 1 FROM {full_table_name} LIMIT 1")
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"Table '{full_table_name}' not found: {str(e)}")
        
        # Get table schema
        schema_result = conn.execute(f"DESCRIBE {full_table_name}").fetchall()
        schema = [{"column": row[0], "type": row[1], "null": row[2]} for row in schema_result]
        
        # Get data with pagination
        data_result = conn.execute(
            f"SELECT * FROM {full_table_name} LIMIT {limit} OFFSET {offset}"
        ).fetchall()
        
        # Get column names
        columns = [desc[0] for desc in conn.description]
        
        # Convert to list of dictionaries
        data = [dict(zip(columns, row)) for row in data_result]
        
        # Get total count
        count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        total_count = count_result[0] if count_result else 0
        
        conn.close()
        
        return {
            "table": table_name,
            "schema": schema,
            "data": data,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "total": total_count,
                "has_more": offset + limit < total_count
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching data from {table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {str(e)}")

@app.post("/query")
async def execute_query(query: Dict[str, str]):
    """Execute a custom SQL query"""
    try:
        sql_query = query.get("query")
        if not sql_query:
            raise HTTPException(status_code=400, detail="Query parameter is required")
        
        # Basic SQL injection protection - only allow SELECT statements
        sql_query = sql_query.strip().upper()
        if not sql_query.startswith("SELECT"):
            raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")
        
        conn = get_connection()
        result = conn.execute(query["query"]).fetchall()
        columns = [desc[0] for desc in conn.description] if conn.description else []
        
        # Convert to list of dictionaries
        data = [dict(zip(columns, row)) for row in result]
        
        conn.close()
        
        return {
            "query": query["query"],
            "columns": columns,
            "data": data,
            "row_count": len(data)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")

@app.get("/restaurant/{camis}/inspection")
async def get_restaurant_inspection(camis: str):
    """Get latest restaurant inspection data for a specific CAMIS"""
    try:
        conn = get_connection()
        
        # Get latest inspection data
        result = conn.execute(f"""
            SELECT * FROM main_mart.fact_restaurant_inspection 
            WHERE camis = '{camis}' 
            ORDER BY inspection_date DESC 
            LIMIT 1
        """).fetchall()
        
        if not result:
            raise HTTPException(status_code=404, detail="No inspection data found for this restaurant")
        
        # Get column names
        columns = [desc[0] for desc in conn.description]
        
        # Convert to dictionary
        inspection_data = dict(zip(columns, result[0]))
        
        conn.close()
        
        return {
            "camis": camis,
            "inspection": inspection_data
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching inspection data for {camis}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch inspection data: {str(e)}")

@app.get("/restaurant/{camis}/rat")
async def get_restaurant_rat_data(camis: str):
    """Get rat activity data for a specific restaurant"""
    try:
        conn = get_connection()
        
        # Get rat data
        result = conn.execute(f"""
            SELECT * FROM main_mart.fact_restaurant_rat 
            WHERE camis = '{camis}' 
            LIMIT 1
        """).fetchall()
        
        if not result:
            raise HTTPException(status_code=404, detail="No rat data found for this restaurant")
        
        # Get column names
        columns = [desc[0] for desc in conn.description]
        
        # Convert to dictionary
        rat_data = dict(zip(columns, result[0]))
        
        conn.close()
        
        return {
            "camis": camis,
            "rat_data": rat_data
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching rat data for {camis}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch rat data: {str(e)}")

@app.get("/zipcode/{zipcode}/stats")
async def get_zipcode_stats(zipcode: str):
    """Get aggregated statistics for a specific zipcode"""
    try:
        conn = get_connection()
        
        # Get zipcode statistics
        result = conn.execute(f"""
            SELECT 
                zipcode,
                COUNT(*) as restaurant_count,
                AVG(composite_rat_risk_score) as avg_risk_score,
                AVG(zip_rat_activity_rate_6m) as avg_rat_activity,
                AVG(avg_score_6m) as avg_inspection_score
            FROM main_mart.fact_restaurant_rat 
            WHERE zipcode = '{zipcode}'
            GROUP BY zipcode
        """).fetchall()
        
        if not result:
            raise HTTPException(status_code=404, detail="No data found for this zipcode")
        
        # Get column names
        columns = [desc[0] for desc in conn.description]
        
        # Convert to dictionary
        zipcode_stats = dict(zip(columns, result[0]))
        
        conn.close()
        
        return {
            "zipcode": zipcode,
            "stats": zipcode_stats
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching zipcode stats for {zipcode}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch zipcode stats: {str(e)}")

@app.get("/stats")
async def get_database_stats():
    """Get basic database statistics"""
    try:
        conn = get_connection()
        
        # Get table count
        tables_result = conn.execute("SHOW TABLES").fetchall()
        table_count = len(tables_result)
        
        # Get database size (if available)
        try:
            size_result = conn.execute("SELECT pg_database_size(current_database())").fetchone()
            db_size = size_result[0] if size_result else 0
        except:
            db_size = 0
        
        conn.close()
        
        return {
            "table_count": table_count,
            "database_size_bytes": db_size,
            "tables": [table[0] for table in tables_result]
        }
    except Exception as e:
        logger.error(f"Error fetching database stats: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch stats: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
