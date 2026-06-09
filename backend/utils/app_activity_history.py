# import json
# import os
# from datetime import datetime
# from hashlib import sha256
# from typing import Any, Dict, List, Optional

# from database.postgres_connection import PostgresDBConnection
# from utils.configure import main_logger


# def _truncate(value: Optional[str], max_len: int) -> Optional[str]:
#     if value is None:
#         return None
#     if not isinstance(value, str):
#         value = str(value)
#     return value[:max_len]


# def _payload_data_summary(payload_data: Dict[str, Any]) -> Dict[str, Any]:
#     table_data = payload_data.get("table_data")
#     table_context = payload_data.get("table_context")
#     user_query = payload_data.get("user_query") or payload_data.get("userQuery")

#     table_data_len: Optional[int] = None
#     table_data_sha256: Optional[str] = None
#     if isinstance(table_data, str):
#         table_data_len = len(table_data)
#         table_data_sha256 = sha256(table_data.encode("utf-8", errors="ignore")).hexdigest()

#     return {
#         "user_query": user_query,
#         "table_context": table_context,
#         "table_data_len": table_data_len,
#         "table_data_sha256": table_data_sha256,
#     }


# def save_app_activity_history(
#     *,
#     endpoint: str,
#     payload_identifier: Dict[str, Any],
#     payload_data: Dict[str, Any],
#     ai_response: Dict[str, Any],
#     user_timestamp: Optional[datetime] = None,
# ) -> Optional[str]:
#     """
#     Best-effort insert into `app_activity_history`.

#     Returns inserted UUID string when available (INSERT ... RETURNING id), else None.
#     """
#     db_url = os.getenv("DATABASE_URL")
#     if not db_url:
#         main_logger.warning("DATABASE_URL not set; skipping app_activity_history insert")
#         return None

#     appkey = (
#         payload_identifier.get("appkey")
#         or payload_data.get("appkey")
#         or os.getenv("DEFAULT_APPKEY", "WEB_MODE")
#     )

#     wma_object_code = (
#         payload_identifier.get("wma_object_code")
#         or payload_identifier.get("wmaObjectCode")
#         or payload_data.get("wma_object_code")
#         or payload_data.get("wmaObjectCode")
#     )

#     app_page_frame_seqid = (
#         payload_identifier.get("app_page_frame_seqid")
#         or payload_identifier.get("appPageFrameSeqid")
#         or payload_data.get("app_page_frame_seqid")
#         or payload_data.get("appPageFrameSeqid")
#         or payload_data.get("frame_id")
#     )

#     iud_seqid = (
#         payload_identifier.get("iud_seqid")
#         or payload_identifier.get("iudSeqid")
#         or payload_data.get("iud_seqid")
#         or payload_data.get("iudSeqid")
#     )

#     user_code = payload_identifier.get("user_code") or payload_data.get("user_code")

#     ai_log_text = payload_data.get("user_query") or payload_data.get("userQuery")

#     ts = user_timestamp or datetime.utcnow()

#     query = """
#         INSERT INTO app_activity_history (
#             appkey,
#             wma_object_code,
#             app_page_frame_seqid,
#             ai_log_text,
#             iud_seqid,
#             wma_user_code,
#             user_timestamp
#         )
#         VALUES (%s, %s, %s, %s, %s, %s, %s)
#         RETURNING id
#     """
#     params = (
#         _truncate(appkey, 100),
#         _truncate(wma_object_code, 100),
#         _truncate(app_page_frame_seqid, 150),
#         ai_log_text,
#         _truncate(iud_seqid, 100),
#         _truncate(user_code, 50),
#         ts,
#     )

#     db_conn = PostgresDBConnection(db_url)
#     try:
#         db_conn.connect()
#         inserted = db_conn.execute_query(query, params)
#         if isinstance(inserted, dict):
#             return inserted.get("id")
#         return None
#     except Exception as e:
#         main_logger.error(f"Failed to insert app_activity_history row: {e}")
#         return None
#     finally:
#         try:
#             db_conn.disconnect()
#         except Exception:
#             pass


# def fetch_app_activity_history_questions(
#     *,
#     appkey: str,
#     user_code: str,
#     wma_object_code: str,
#     app_page_frame_seqid: str,
#     limit: int = 50,
# ) -> List[str]:
#     """
#     Fetch recent history rows and return a list of {id, user_query, user_timestamp}.
#     For TABLE_GPT_PLUS, query by appkey and wma_object_code only.
#     """
#     db_url = os.getenv("DATABASE_URL")
#     if not db_url:
#         raise RuntimeError("DATABASE_URL not set")

#     safe_limit = max(1, min(int(limit or 50), 500))

#     # For TABLE_GPT_PLUS, use simplified query with only appkey and wma_object_code
#     is_table_gpt_plus = appkey == "TABLE_GPT_PLUS" and user_code == "TABLE_GPT_PLUS_USER"

#     if is_table_gpt_plus:
#         query = """
#             SELECT id, ai_log_text, user_timestamp
#             FROM app_activity_history
#             WHERE appkey = %s
#               AND wma_object_code = %s
#             ORDER BY user_timestamp DESC NULLS LAST, id DESC
#             LIMIT %s
#         """
#         params = (
#             _truncate(appkey, 100),
#             _truncate(wma_object_code, 100),
#             safe_limit,
#         )
#     else:
#         query = """
#             SELECT id, ai_log_text, user_timestamp
#             FROM app_activity_history
#             WHERE appkey = %s
#               AND wma_user_code = %s
#               AND wma_object_code = %s
#               AND app_page_frame_seqid = %s
#             ORDER BY user_timestamp DESC NULLS LAST, id DESC
#             LIMIT %s
#         """
#         params = (
#             _truncate(appkey, 100),
#             _truncate(user_code, 50),
#             _truncate(wma_object_code, 100),
#             _truncate(app_page_frame_seqid, 150),
#             safe_limit,
#         )

#     db_conn = PostgresDBConnection(db_url)
#     try:
#         db_conn.connect()
#         rows = db_conn.execute_query(query, params) or []
#         if isinstance(rows, dict):
#             rows = [rows]
#     finally:
#         try:
#             db_conn.disconnect()
#         except Exception:
#             pass

#     items: List[str] = []
#     for row in rows:
#         if not isinstance(row, dict):
#             continue
#         ai_log_text = row.get("ai_log_text")
#         user_query: Optional[str] = None

#         if isinstance(ai_log_text, str) and ai_log_text.strip():
#             try:
#                 parsed = json.loads(ai_log_text)
#                 if isinstance(parsed, dict):
#                     user_query = parsed.get("user_query")
#                     if not user_query and isinstance(parsed.get("payload_data"), dict):
#                         user_query = parsed["payload_data"].get("user_query")
#             except Exception:
#                 user_query = None

#         if user_query is None and isinstance(ai_log_text, str):
#             # fallback: store first 200 chars if the text isn't JSON
#             user_query = ai_log_text.strip()[:200] if ai_log_text.strip() else None

#         if not user_query:
#             continue

#         items.append(user_query)

#     return items


# def fetch_app_activity_history_dashboard(
#     *,
#     filter_type: str = "all",
#     appkey: Optional[str] = None,
#     user_code: Optional[str] = None,
#     wma_object_code: Optional[str] = None,
#     app_page_frame_seqid: Optional[str] = None,
#     iud_seqid: Optional[str] = None,
#     limit: int = 10,
#     offset: int = 0,
# ) -> Dict[str, Any]:
#     """
#     Fetch aggregated dashboard data from app_activity_history.
#     filter_type: "all" - all records
#                  "webmode" - only WEB_MODE
#                  "unique" - appkey NOT WEB_MODE
#     """
#     db_url = os.getenv("DATABASE_URL")
#     if not db_url:
#         raise RuntimeError("DATABASE_URL not set")

#     safe_limit = max(1, min(int(limit or 10), 100))
#     safe_offset = max(0, int(offset or 0))

#     conditions = []
#     params = []

#     if filter_type == "webmode":
#         conditions.append("appkey = %s")
#         params.append("WEB_MODE")
#     elif filter_type == "unique":
#         conditions.append("appkey != %s")
#         params.append("WEB_MODE")
    
#     if appkey and filter_type == "unique":
#         conditions.append("appkey = %s")
#         params.append(_truncate(appkey, 100))
#     if user_code:
#         conditions.append("wma_user_code LIKE %s")
#         params.append(f"%{_truncate(user_code, 50)}%")
#     if wma_object_code:
#         conditions.append("wma_object_code LIKE %s")
#         params.append(f"%{_truncate(wma_object_code, 100)}%")
#     if app_page_frame_seqid:
#         conditions.append("app_page_frame_seqid LIKE %s")
#         params.append(f"%{_truncate(app_page_frame_seqid, 150)}%")
#     if iud_seqid:
#         conditions.append("iud_seqid = %s")
#         params.append(_truncate(iud_seqid, 100))

#     where_clause = " AND ".join(conditions) if conditions else "1=1"

#     count_query = f"""
#         SELECT COUNT(*) as total
#         FROM app_activity_history
#         WHERE {where_clause}
#     """
    
#     data_query = f"""
#         SELECT id, appkey, wma_user_code, wma_object_code, app_page_frame_seqid, iud_seqid, ai_log_text, user_timestamp
#         FROM app_activity_history
#         WHERE {where_clause}
#         ORDER BY user_timestamp DESC NULLS LAST, id DESC
#         LIMIT %s OFFSET %s
#     """

#     unique_appkeys_query = """
#         SELECT DISTINCT appkey FROM app_activity_history WHERE appkey != 'WEB_MODE' ORDER BY appkey
#     """

#     db_conn = PostgresDBConnection(db_url)
#     rows = []
#     total_count = 0
#     unique_appkeys: List[str] = []
#     try:
#         db_conn.connect()
#         count_result = db_conn.execute_query(count_query, params)
#         if isinstance(count_result, dict):
#             total_count = count_result.get("total", 0) or 0
#         elif isinstance(count_result, list) and len(count_result) > 0:
#             total_count = count_result[0].get("total", 0) or 0
        
#         data_params = params + [safe_limit, safe_offset]
#         rows = db_conn.execute_query(data_query, data_params) or []
#         if isinstance(rows, dict):
#             rows = [rows]

#         unique_appkeys_result = db_conn.execute_query(unique_appkeys_query) or []
#         if isinstance(unique_appkeys_result, dict):
#             unique_appkeys = [unique_appkeys_result.get("appkey")] if unique_appkeys_result.get("appkey") else []
#         elif isinstance(unique_appkeys_result, list):
#             unique_appkeys = [r.get("appkey") for r in unique_appkeys_result if r.get("appkey")]
#     finally:
#         try:
#             db_conn.disconnect()
#         except Exception:
#             pass

#     recent_queries: List[Dict[str, Any]] = []
#     daily_counts: Dict[str, int] = {}

#     for row in rows:
#         if not isinstance(row, dict):
#             continue
#         ai_log_text = row.get("ai_log_text")
#         user_timestamp = row.get("user_timestamp")
#         user_query: Optional[str] = None

#         if isinstance(ai_log_text, str) and ai_log_text.strip():
#             try:
#                 parsed = json.loads(ai_log_text)
#                 if isinstance(parsed, dict):
#                     user_query = parsed.get("user_query")
#                     if not user_query and isinstance(parsed.get("payload_data"), dict):
#                         user_query = parsed["payload_data"].get("user_query")
#             except Exception:
#                 user_query = None

#         if user_query is None and isinstance(ai_log_text, str):
#             user_query = ai_log_text.strip()[:200] if ai_log_text.strip() else None

#         query_item = {
#             "id": row.get("id"),
#             "appkey": row.get("appkey"),
#             "wma_user_code": row.get("wma_user_code"),
#             "wma_object_code": row.get("wma_object_code"),
#             "app_page_frame_seqid": row.get("app_page_frame_seqid"),
#             "iud_seqid": row.get("iud_seqid"),
#             "ai_log_text": user_query,
#             "user_timestamp": user_timestamp.isoformat() if user_timestamp and hasattr(user_timestamp, 'isoformat') else str(user_timestamp) if user_timestamp else None,
#         }
        
#         if user_timestamp:
#             date_key = str(user_timestamp)[:10] if user_timestamp else "unknown"
#             daily_counts[date_key] = daily_counts.get(date_key, 0) + 1
        
#         recent_queries.append(query_item)

#     return {
#         "total_queries": total_count,
#         "recent_queries": recent_queries,
#         "daily_counts": daily_counts,
#         "unique_appkeys": unique_appkeys,
#         "filter_type": filter_type,
#         "page": {
#             "limit": safe_limit,
#             "offset": safe_offset,
#             "total": total_count,
#         },
#     }

