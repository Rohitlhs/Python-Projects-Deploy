# import psycopg2
# from psycopg2.extras import RealDictCursor
# from dotenv import load_dotenv

# load_dotenv()

# class PostgresDBConnection:
#     def __init__(self, db_url: str):
#         self.db_url = db_url
#         self.connection = None

#     def connect(self):
#         try:
#             self.connection = psycopg2.connect(self.db_url, cursor_factory=RealDictCursor)
#             print("✅ Connection to PostgreSQL database established successfully.")
#         except Exception as e:
#             print(f"❌ Error connecting to PostgreSQL database: {e}")
#             raise

#     def disconnect(self):
#         if self.connection:
#             self.connection.close()
#             print("🔌 PostgreSQL database connection closed.")

#     def commit(self):
#         if self.connection:
#             self.connection.commit()

#     def rollback(self):
#         if self.connection:
#             self.connection.rollback()

#     def execute_query(self, query, params=None):
#         if not self.connection:
#             raise RuntimeError("No active database connection.")

#         with self.connection.cursor() as cursor:
#             cursor.execute(query, params)
#             try:
#                 result = cursor.fetchall()
#                 self.connection.commit()
#                 if len(result) == 1:
#                     return result[0]
#                 if len(result) == 0:
#                     return None
#                 return result
#             except psycopg2.ProgrammingError:
#                 self.connection.commit()
#                 return None
# # if __name__ == "__main__":
# #     import os
# #     db_url = os.getenv("DATABASE_URL")

# #     if not db_url:
# #         raise EnvironmentError("DATABASE_URL not found in environment variables")

# #     db_conn = PostgresDBConnection(db_url)
# #     db_conn.connect()

# #     result = db_conn.execute_query("SELECT version();")
# #     print(result)

# #     db_conn.disconnect()
