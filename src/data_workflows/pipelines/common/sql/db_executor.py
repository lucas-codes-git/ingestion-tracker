from src.services.database import pool

class DatabaseExecutor:
    def execute(self, query):
        with pool.connection as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                
            conn.commit(query)