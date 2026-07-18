# Ingestion Tracker Data Workflows Project
### How to run locally
From the root directory run:
```powershell
docker compose up --build
```

## API Endpoints

### liveliness check
```
http://localhost:8000/
```
### Swagger docs
```
http://localhost:8000/docs
```

### Testing a route
Bash
```bash
curl -X POST "http://localhost:8080/api/v1/route-name-here"
```
Powershell
```powershell
Invoke-WebRequest -Uri "http://localhost:8000/api/v1/route-name-here" -Method POST
```

## Querying the Ingestion DB
```docker
docker exec -it ingestion_db psql -U your-username -d your-dbname 
```
To see your list of tables
```docker
\dt
```
To quit
```
\q
```
When you see dbname=# you can write your sql.
***Be sure to end your sql with ';'***

# Pipeline Flow
```
                 insert_job()
                     |
                     v
              bronze_status=pending
                     |
                     v
              start_bronze()
                     |
                     v
          download/process raw file
                     |
                     v
             complete_bronze()
                     |
                     v
             silver_status=pending
                     |
                     v
             start_silver()
                     |
                     v
          transform parquet/json
                     |
                     v
             complete_silver()
```