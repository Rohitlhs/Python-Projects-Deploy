from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from utils.check_health import get_status_data, get_version_data, smart_response
import asyncio
import contextlib
from fastapi.responses import StreamingResponse
from starlette.middleware.sessions import SessionMiddleware
from pydantic import BaseModel, field_validator,validator, root_validator
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
import json
import uuid
import time
from urllib.parse import quote
import openai
import os
import logging
import dotenv
import shutil
from datetime import datetime
from fastapi.responses import JSONResponse, HTMLResponse, Response

from app.suggestion import SuggestionHandler 

from utils.crypto_utils import decrypt_url,decrypt_dict_fields
from utils.configure import main_logger, debug_logger, load_config
from utils.check_health import get_status_data, get_version_data, smart_response
from utils.app_version import get_app_version
from utils.sse_event import sse_event
from utils.redis_delete_utils import _safe_session_id, _delete_redis_session_keys, _runtime_session_base_dir
from typing import Optional, Dict, Any
import pandas as pd
from app.langgraph_config.nodes.session_dataset_loader import session_dataset_loader_node
# from utils.app_activity_history import save_app_activity_history, fetch_app_activity_history_questions, fetch_app_activity_history_dashboard

## FOR GLOBAL SERVICE
# from global_service.global_service_pkg_config import *
# from global_service.global_service_pkg_config import log_debug_request
# from global_service_fastapi_pkg.debug_log_flag_manager import enable_debug_logs, disable_debug_logs

from app.langgraph_config.graph.graph_builder import Table_GPT
from app.langgraph_config.node_utils.stage_labels import STAGE_LABELS
from app.components.error_handler import is_error_message, get_friendly_error_message

from app.components.session_utils import (
    bind_stored_csv_to_session,
    list_stored_csv_bots,
    load_stored_csv_prompt,
    _store_embed_metadata,
    _load_embed_metadata,
    _load_session_csv_preview,
    cleanup_expired_sessions,
    _get_session_cleanup_interval_seconds,
    _get_session_ttl_seconds,
)

##################################################### FASTAPI APP #######################################################
app = FastAPI()
session_cleanup_task: asyncio.Task | None = None

app.add_middleware(ProxyHeadersMiddleware, trusted_hosts="*")

# app.mount("/api/agentai/table_gpt_plus/static", StaticFiles(directory="static"), name="static")
# templates = Jinja2Templates(directory="templates")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Then add SessionMiddleware
app.add_middleware(
    SessionMiddleware,
    secret_key="your-very-secret-key"
)

# Load environment 
dotenv.load_dotenv()
openai.api_key = os.getenv('OPENAI_API_KEY')
load_config()

# Custom log collector for GLOBAL SERVICE
# class RequestLogCollector(logging.Handler):
#     def __init__(self):
#         super().__init__()
#         self.logs = []
#         self.counter = 1
#     def emit(self, record):
#         msg = self.format(record)
#         numbered_msg = f"{self.counter} {msg}"
#         self.logs.append(numbered_msg)
#         self.counter += 1
        
# log_collector = RequestLogCollector()
# formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
# log_collector.setFormatter(formatter)
# step_logger = StepLogger()
# app_version = get_app_version()

######################################### DEBUG LOGS GLOBAL SERVICE ENDPOINTS ##########################################
# class DebugLogFlagRequest(BaseModel):
#     save_debug_logs: bool
#     debug_active_by: Optional[str] = None
#     debug_end_by: Optional[str] = None
#     start_time: Optional[str] = None
#     end_time: Optional[str] = None
#     app_code: str = "L-PYA100"  # Default app code

class SessionDeleteRequest(BaseModel):
    user_ref_no: str

class HistoryFetchRequest(BaseModel):
    payload_identifier: Dict[str, Any] = {}
    payload_data: Dict[str, Any] = {}
    limit: int = 50

# @app.post("/api/agentai/table_gpt_plus/set_debug_log_flag")
# async def set_debug_log_flag(data: DebugLogFlagRequest):
#     if data.save_debug_logs:
#         result = enable_debug_logs(data.debug_active_by)
#     else:
#         result = disable_debug_logs(
#             debug_active_by=data.debug_active_by,
#             debug_end_by=data.debug_end_by,
#             start_time=data.start_time,
#             end_time=data.end_time,
#             app_code=data.app_code  # Use the configured app code
#         )
#     return result

# @app.get("/api/agentai/table_gpt_plus/debug_log_status")
# async def get_debug_log_status():
#     return {
#         "debug_flag": getattr(config, "SAVE_DEBUG_LOGS", False),
#         "start_by": getattr(config, "debug_active_by", None),
#         "start_time": getattr(config, "start_time", None),
#         "end_time": getattr(config, "end_time", None),
#         "end_by": getattr(config, "debug_end_by", None),
#         "app_code": getattr(config, "APP_CODE", None)
#     }
#################################### END DEBUG LOGS GLOBAL SERVICE ENDPOINTS ##################################

# Initialize handlers  
suggestion_handler = SuggestionHandler()

# Configure logging
logging.basicConfig(level=logging.DEBUG)

##### Session cleanup loop to automatically delete expired session data from disk and Redis, preventing buildup of old data.
# Runs in the background and can be configured via environment variables. #####
async def _session_cleanup_loop():
    ttl_seconds = _get_session_ttl_seconds()
    interval_seconds = _get_session_cleanup_interval_seconds()
    main_logger.info(
        f"Session cleanup loop started. ttl_seconds={ttl_seconds}, interval_seconds={interval_seconds}"
    )

    while True:
        try:
            deleted_count = cleanup_expired_sessions(ttl_seconds=ttl_seconds)
            if deleted_count:
                main_logger.info(f"Auto-deleted {deleted_count} expired session(s).")
        except Exception as e:
            main_logger.error(f"Session cleanup loop failed: {e}")

        await asyncio.sleep(interval_seconds)


@app.on_event("startup")
async def startup_session_cleanup():
    global session_cleanup_task
    try:
        cleanup_expired_sessions(ttl_seconds=_get_session_ttl_seconds())
    except Exception as e:
        main_logger.error(f"Initial session cleanup failed: {e}")

    session_cleanup_task = asyncio.create_task(_session_cleanup_loop())


@app.on_event("shutdown")
async def shutdown_session_cleanup():
    global session_cleanup_task
    if session_cleanup_task:
        session_cleanup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await session_cleanup_task


################################################################## TABLE GPT ENDPOINTS #######################################################
### Suggestions Endpoint ###   
@app.post("/api/agentai/table_gpt_plus/suggestions")
async def suggestions(request: Request):
    # Attach log collector GLOBAL SERVICE
    # main_logger.addHandler(log_collector)
    # if debug_logger:
    #     debug_logger.addHandler(log_collector)
    # log_collector.logs.clear()
    # collected_logs = None
    # step_logger.log("Suggestions endpoint called")
    payload_request_status_code = "Success (200 OK)"
    payload_response_status_msg = None
    
    request_time = datetime.now().isoformat()
    request_data = await request.json()
    payload_data = request_data.get("payload_data", {})
    payload_identifier = request_data.get("payload_identifier", {})
    # Handle both encrypted and plain JSON
    if isinstance(request_data, str):
        decrypted = decrypt_url(request_data)
        if decrypted:
            try:
                request_data = json.loads(decrypted)
                payload_data = request_data.get("payload_data", {})
                payload_identifier = request_data.get("payload_identifier", {})
            except Exception:
                pass
    main_logger.info(f"----------------->Suggestions request received at {request_time} ")
    # step_logger.log(f"Suggestions request received at {request_time}  with data: {payload_identifier}")
    try:
        context = payload_data.get('table_data')
        table_context = payload_data.get('table_context')
        raw_session_id = payload_identifier.get("user_ref_no")
        bot_id = (payload_data.get("table_gpt_bot_id") or
                  payload_data.get("table_gpt_bot", {}).get("bot_id", "") or
                  payload_identifier.get("table_gpt_bot_id", ""))

        if (context is None or str(context).strip() == "") and raw_session_id and not bot_id:
            session_id = _safe_session_id(raw_session_id)
            context = _load_session_csv_preview(session_id)
            main_logger.info(
                f"Suggestions request reused stored session dataset preview for session_id={session_id}"
            )

        suggestions = await suggestion_handler.handle_suggestions(context, table_context, request, payload_data, payload_identifier)
        
        # Extract actual response body for logging
        if isinstance(suggestions, JSONResponse):
            response_body_str = suggestions.body.decode()  # Get the actual JSON string
        else:
            response_body_str = json.dumps(suggestions)
        # step_logger.log(f"Suggestions response: {response_body_str}")
        
        # GLOBAL SERVICE LOGS
        # step_logger.log(f"Chat response generated")
        # payload_request_status_code = "Success (200 OK)"
        # payload_response_status_msg = "Success (200 OK)"
        # log_request(
        #     request, step_logger, request_body=json.dumps(request_data), response_body=response_body_str, log_type="INFO", app_version=app_version,
        #     payload_request_status_code=payload_request_status_code,
        #     payload_response_status_msg=payload_response_status_msg
        # )
        # collected_logs = '\n'.join(log_collector.logs)
        # log_debug_request(request, request_body=json.dumps(request_data), response_body=response_body_str, debug_logs=collected_logs)
        return suggestions
    except Exception as e:
        main_logger.error(f"Error: {str(e)}")
        if debug_logger:
            debug_logger.error(f"Error: {str(e)}")
            
        # GLOBAL SERVICE LOGS 
        # step_logger.log("chat response generated (error)")
        # payload_request_status_code = "Failure (400 Bad Request)"
        # payload_response_status_msg = "Failure (400 Bad Request)"
        # log_request(
        #     request, step_logger, request_body=json.dumps(request_data), log_type="INFO", app_version=app_version,
        #     payload_request_status_code=payload_request_status_code,
        #     payload_response_status_msg=payload_response_status_msg
        # )
        # collected_logs = '\n'.join(log_collector.logs)
        # log_debug_request(request, request_body=json.dumps(request_data), debug_logs=collected_logs)
        return JSONResponse(content={'answer': "Sorry, there was an error processing your request.", 'error': str(e), 'status': 'error'}, status_code=500)  

##################################################### Chat Endpoint #####################################
@app.post("/api/agentai/table_gpt_plus/chat")
async def chat(request: Request):
    # Attach log collector GLOBAL SERVICE
    # main_logger.addHandler(log_collector)
    # if debug_logger:
    #     debug_logger.addHandler(log_collector)
    # log_collector.logs.clear()
    # collected_logs = None
    # step_logger.log("Chat endpoint called")
    payload_request_status_code = "Success (200 OK)"
    payload_response_status_msg = None
    
    ### Request Incoming
    request_time = datetime.now().isoformat()
    request_data = await request.json()
    payload_identifier = request_data.get("payload_identifier", {})
    payload_data = request_data.get("payload_data", {})

    session_id = payload_identifier.get("user_ref_no")
    
    # Handle both encrypted and plain JSON (if needed)
    if isinstance(request_data, str):
        decrypted = decrypt_url(request_data)
        if decrypted:
            try:
                request_data = json.loads(decrypted)
                payload_identifier = request_data.get("payload_identifier", {})
                payload_data = request_data.get("payload_data", {})
            except Exception:
                pass
            
    main_logger.info(f"----------------->Chat request received at {request_time} ")
    # step_logger.log(f"Chat request received at {request_time}  with data: {payload_identifier} and user_query: {payload_data.get('user_query', '')}")

    try:
        result = Table_GPT.invoke(
            {
                "session_id": session_id,
                "user_query": payload_data.get("user_query", ""),
                "table_data": payload_data.get("table_data"),
                "table_info": payload_data.get("table_context"),
                "custom_prompt": payload_data.get("custom_prompt"),
                "ai_prompt": payload_data.get("aiInputData"),
                "retry_count": 0,
            },
            config={"configurable": {"thread_id": session_id}},
        )

        final_answer = result.get("final_answer", "")
        
        # Check if the answer contains error messages and replace with friendly message
        if is_error_message(final_answer):
            main_logger.warning(f"Error message detected in response, replacing with friendly message")
            final_answer = get_friendly_error_message()

        response_payload = {
            "answer": final_answer,
            "status": "success",
            "intent": result.get("intent"),
            "plan": result.get("plan"),
            "execution_output": result.get("execution_output"),
            "execution_error": result.get("execution_error"),
        }
    except Exception as e:
        main_logger.error(f"Error in chat endpoint: {str(e)}")
        response_payload = {
            "answer": get_friendly_error_message(),
            "status": "error",
            "intent": None,
            "plan": None,
            "execution_output": None,
            "execution_error": None,
        }
    
    main_logger.info(f"Chat response generated: {response_payload}")

    # try:
    #     save_app_activity_history(
    #         endpoint="/api/agentai/table_gpt_plus/chat",
    #         payload_identifier=payload_identifier,
    #         payload_data=payload_data,
    #         ai_response=response_payload,
    #     )
    # except Exception:
    #     pass
    
    # step_logger.log("Chat response generated")
    # payload_request_status_code = "Success (200 OK)"
    # payload_response_status_msg = "Success (200 OK)"
    # log_request(
    #     request, step_logger, request_body=json.dumps(request_data), response_body=json.dumps(response_payload), log_type="INFO", app_version=app_version,
    #     payload_request_status_code=payload_request_status_code,
    #     payload_response_status_msg=payload_response_status_msg
    # )
    # collected_logs = '\n'.join(log_collector.logs)
    # log_debug_request(
    #     request,
    #     request_body=json.dumps(request_data),
    #     response_body=json.dumps(response_payload),
    #     debug_logs=collected_logs,
    # )

    return JSONResponse(content=response_payload, status_code=200)
    
######### Function to create Server-Sent Events (SSE) for streaming responses in the chat endpoint. ############
@app.post("/api/agentai/table_gpt_plus/chat/stream")
async def chat_stream(request: Request):
    execution_started_at = time.perf_counter()

    # Attach log collector GLOBAL SERVICE
    # main_logger.addHandler(log_collector)
    # if debug_logger:
    #     debug_logger.addHandler(log_collector)
    # log_collector.logs.clear()
    # collected_logs = None
    # step_logger.log("Chat stream endpoint called")
    payload_request_status_code = "Success (200 OK)"
    payload_response_status_msg = None
    
    ### Request Incoming
    request_time = datetime.now().isoformat()
    request_data = await request.json()
    payload_identifier = request_data.get("payload_identifier", {})
    payload_data = request_data.get("payload_data", {})
    session_id = payload_identifier.get("user_ref_no")
    
    # Handle both encrypted and plain JSON (if needed)
    if isinstance(request_data, str):
        decrypted = decrypt_url(request_data)
        if decrypted:
            try:
                request_data = json.loads(decrypted)
                payload_identifier = request_data.get("payload_identifier", {})
                payload_data = request_data.get("payload_data", {})
            except Exception:
                pass
            
    main_logger.info(f"----------------->Chat stream request received at {request_time} ")
    # step_logger.log(f"Chat stream request received at {request_time}  with data: {payload_identifier} and user_query: {payload_data.get('user_query', '')}")

    input_state = {
        "session_id": session_id,
        "user_query": payload_data.get("user_query", ""),
        "table_data": payload_data.get("table_data"),
        "table_info": payload_data.get("table_context"),
        "custom_prompt": payload_data.get("custom_prompt"),
        "ai_prompt": payload_data.get("aiInputData"),
        "retry_count": 0,
    }
    main_logger.info(f"Chat stream input User Query: {input_state['user_query']}")
    main_logger.info(f"Chat stream input state: {input_state['user_query']} with table_data: {str(input_state['table_data'])[:100]} and table_info: {str(input_state['table_info'])[:100]} and custom_prompt: {str(input_state['custom_prompt'])[:100]} and ai_prompt: {str(input_state['ai_prompt'])[:100]}")

    config = {"configurable": {"thread_id": session_id}}

    async def event_generator():
        latest_final_answer = ""
        latest_stage = ""
        stage_history = []
        latest_meta = {
            "intent": None,
            "plan": None,
            "execution_output": None,
            "execution_error": None,
            "has_visualization": False,
            "visualization_spec": None,
        }
        final_status = "success"
        client_disconnected = False

        yield sse_event("started", {
            "session_id": session_id,
            "message": "Chat started"
        })

        try:
            for chunk in Table_GPT.stream(input_state, config=config, stream_mode="updates"):
                # Check if client is still connected
                if not client_disconnected and await request.is_disconnected():
                    main_logger.info(f"Client disconnected during stream for session {session_id}, setting cancellation flag")
                    client_disconnected = True
                    # Set cancellation flag and continue to propagate it through remaining nodes
                    input_state["should_cancel"] = True
                
                for node_name, node_update in chunk.items():
                    if not isinstance(node_update, dict):
                        continue

                    for k in ("intent", "plan", "execution_output", "execution_error", "has_visualization", "visualization_spec"):
                        if k in node_update and node_update.get(k) is not None:
                            latest_meta[k] = node_update.get(k)

                    current_stage = node_update.get("current_stage")
                    current_stage_label = node_update.get("current_stage_label")

                    if current_stage and current_stage != latest_stage:
                        latest_stage = current_stage
                        stage_history.append({
                            "node": current_stage,
                            "label": current_stage_label or STAGE_LABELS.get(current_stage, current_stage),
                        })
                        yield sse_event("stage", stage_history[-1])

                    if node_update.get("final_answer"):
                        latest_final_answer = node_update["final_answer"]
                
                # If client disconnected and we got the final answer, break out
                if client_disconnected and latest_final_answer:
                    main_logger.info(f"Client disconnected, breaking stream after final answer for session {session_id}")
                    break
        except GeneratorExit:
            response_payload = {
                "answer": latest_final_answer,
                "status": "client_disconnected",
                "intent": latest_meta.get("intent"),
                "plan": latest_meta.get("plan"),
                "execution_output": latest_meta.get("execution_output"),
                "execution_error": latest_meta.get("execution_error"),
                "stages": stage_history,
            }
            # try:
            #     save_app_activity_history(
            #         endpoint="/api/agentai/table_gpt_plus/chat/stream",
            #         payload_identifier=payload_identifier,
            #         payload_data=payload_data,
            #         ai_response=response_payload,
            #     )
            # except Exception:
            #     pass
            # step_logger.log("chat response generated (client disconnected)")
            # log_request(
            #     request, step_logger, request_body=json.dumps(request_data), response_body=json.dumps(response_payload), log_type="INFO", app_version=app_version,
            #     payload_request_status_code="Failure (499 Client Closed Request)",
            #     payload_response_status_msg="Failure (499 Client Closed Request)"
            # )
            # collected_logs = '\n'.join(log_collector.logs)
            # log_debug_request(
            #     request,
            #     request_body=json.dumps(request_data),
            #     response_body=json.dumps(response_payload),
            #     debug_logs=collected_logs,
            # )
            raise
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            # Handle connection errors gracefully - these occur when client abruptly disconnects
            main_logger.debug(f"Connection error during stream for session {session_id}: {type(e).__name__}")
            response_payload = {
                "answer": latest_final_answer,
                "status": "client_disconnected",
                "intent": latest_meta.get("intent"),
                "plan": latest_meta.get("plan"),
                "execution_output": latest_meta.get("execution_output"),
                "execution_error": latest_meta.get("execution_error"),
                "stages": stage_history,
            }
            # try:
            #     save_app_activity_history(
            #         endpoint="/api/agentai/table_gpt_plus/chat/stream",
            #         payload_identifier=payload_identifier,
            #         payload_data=payload_data,
            #         ai_response=response_payload,
            #     )
            # except Exception:
            #     pass
            # Don't re-raise connection errors - client is already gone
            return
        except Exception as e:
            final_status = "error"
            main_logger.error(f"Error in chat stream: {e}")
            latest_final_answer = get_friendly_error_message()
        
        # Check if the answer contains error messages and replace with friendly message
        if is_error_message(latest_final_answer):
            main_logger.warning(f"Error message detected in stream response, replacing with friendly message")
            latest_final_answer = get_friendly_error_message()
            if final_status == "success":
                final_status = "error"

        execution_duration = time.perf_counter() - execution_started_at
        execution_time_seconds = round(execution_duration, 2)
        execution_time_ms = round(execution_duration * 1000, 2)

        yield sse_event("final", {
            "answer": latest_final_answer,
            "status": final_status,
            "has_visualization": latest_meta.get("has_visualization", False),
            "visualization_spec": latest_meta.get("visualization_spec"),
            "execution_time_seconds": execution_time_seconds,
            "execution_time_ms": execution_time_ms,
        })

        response_payload = {
            "answer": latest_final_answer,
            "status": final_status,
            "execution_time_seconds": execution_time_seconds,
            "execution_time_ms": execution_time_ms,
            "intent": latest_meta.get("intent"),
            "plan": latest_meta.get("plan"),
            "execution_output": latest_meta.get("execution_output"),
            "execution_error": latest_meta.get("execution_error"),
            "stages": stage_history,
        }
        main_logger.info(f"Chat stream final response: {response_payload}")
        main_logger.info(f"Chat stream final Answer: {latest_final_answer} ")

        # try:
        #     save_app_activity_history(
        #         endpoint="/api/agentai/table_gpt_plus/chat/stream",
        #         payload_identifier=payload_identifier,
        #         payload_data=payload_data,
        #         ai_response=response_payload,
        #     )
        # except Exception:
        #     pass
        # step_logger.log("Chat stream response generated with Final Answer: \n" + str(latest_final_answer[:1000]))
        # payload_request_status_code = "Success (200 OK)" if final_status == "success" else "Failure (500 Internal Server Error)"
        # payload_response_status_msg = payload_request_status_code
        # log_request(
        #     request, step_logger, request_body=json.dumps(request_data), response_body=json.dumps(response_payload), log_type="INFO", app_version=app_version,
        #     payload_request_status_code=payload_request_status_code,
        #     payload_response_status_msg=payload_response_status_msg
        # )
        # collected_logs = '\n'.join(log_collector.logs)
        # log_debug_request(
        #     request,
        #     request_body=json.dumps(request_data),
        #     response_body=json.dumps(response_payload),
        #     debug_logs=collected_logs,
        # )

    return StreamingResponse(event_generator(), media_type="text/event-stream")

############# CHAT HISTORY ENDPOINT TO FETCH PAST USER QUESTIONS FOR THE SAME APP/USER/OBJECT #############
# @app.post("/api/agentai/table_gpt_plus/history")
# async def get_app_activity_history(data: HistoryFetchRequest):
    # payload_identifier = data.payload_identifier or {}
    # payload_data = data.payload_data or {}

    # appkey = payload_identifier.get("appkey") or payload_data.get("appkey")
    # user_code = payload_identifier.get("user_code") or payload_data.get("user_code")
    # wma_object_code = (
    #     payload_identifier.get("wma_object_code")
    #     or payload_identifier.get("wmaObjectCode")
    #     or payload_data.get("wma_object_code")
    #     or payload_data.get("wmaObjectCode")
    # )
    # app_page_frame_seqid = (
    #     payload_identifier.get("app_page_frame_seqid")
    #     or payload_identifier.get("appPageFrameSeqid")
    #     or payload_data.get("app_page_frame_seqid")
    #     or payload_data.get("appPageFrameSeqid")
    #     or payload_data.get("frame_id")
    # )

    # # For TABLE_GPT_PLUS, set defaults and relax requirements
    # if appkey == "TABLE_GPT_PLUS" or payload_identifier.get("appkey") == "TABLE_GPT_PLUS":
    #     appkey = "TABLE_GPT_PLUS"
    #     user_code = user_code or "TABLE_GPT_PLUS_USER"
    #     app_page_frame_seqid = app_page_frame_seqid or wma_object_code or "TABLE_GPT_PLUS_FRAME"

    # missing = []
    # if not appkey:
    #     missing.append("appkey")
    # if not wma_object_code:
    #     missing.append("wma_object_code")
    # if not user_code:
    #     missing.append("user_code")
    # if not app_page_frame_seqid:
    #     missing.append("app_page_frame_seqid")

    # if missing:
    #     raise HTTPException(status_code=400, detail=f"Missing required fields: {', '.join(missing)}")

    # try:
    #     questions = fetch_app_activity_history_questions(
    #         appkey=str(appkey),
    #         user_code=str(user_code),
    #         wma_object_code=str(wma_object_code),
    #         app_page_frame_seqid=str(app_page_frame_seqid),
    #         limit=data.limit,
    #     )
    #     return JSONResponse(content={"success": True, "questions": questions}, status_code=200)
    # except Exception as e:
    #     main_logger.error(f"Failed to fetch app activity history: {e}")
    #     raise HTTPException(status_code=500, detail="Failed to fetch history")


class DashboardFetchRequest(BaseModel):
    payload_identifier: Dict[str, Any] = {}
    payload_data: Dict[str, Any] = {}
    filter_type: str = "all"
    appkey: Optional[str] = None
    user_code: Optional[str] = None
    wma_object_code: Optional[str] = None
    app_page_frame_seqid: Optional[str] = None
    limit: int = 10
    offset: int = 0


# @app.post("/api/agentai/table_gpt_plus/dashboard")
# async def get_dashboard_data(data: DashboardFetchRequest):
    # try:
    #     dashboard_data = fetch_app_activity_history_dashboard(
    #         filter_type=data.filter_type,
    #         appkey=data.appkey,
    #         user_code=data.user_code,
    #         wma_object_code=data.wma_object_code,
    #         app_page_frame_seqid=data.app_page_frame_seqid,
    #         limit=data.limit,
    #         offset=data.offset,
    #     )
    #     return JSONResponse(content={"success": True, "data": dashboard_data}, status_code=200)
    # except Exception as e:
    #     main_logger.error(f"Failed to fetch dashboard data: {e}")
    #     raise HTTPException(status_code=500, detail="Failed to fetch dashboard data")


class DashboardEmbedRequest(BaseModel):
    payload_identifier: Dict[str, Any] = {}


class StoredCsvEmbedRequest(BaseModel):
    bot_id: str
    payload_identifier: Dict[str, Any] = {}
    payload_data: Dict[str, Any] = {}


# @app.post("/api/agentai/table_gpt_plus/dashboard/embed")
# async def get_dashboard_embed_data(data: DashboardEmbedRequest):
    # payload_identifier = data.payload_identifier or {}

    # appkey = payload_identifier.get("appkey")
    # wma_object_code = payload_identifier.get("wma_object_code")
    # app_page_frame_seqid = payload_identifier.get("app_page_frame_seqid")
    # iud_seqid = payload_identifier.get("iud_seqid")
    # user_code = payload_identifier.get("user_code")

    # try:
    #     dashboard_data = fetch_app_activity_history_dashboard(
    #         filter_type="unique",
    #         appkey=appkey,
    #         wma_object_code=wma_object_code,
    #         app_page_frame_seqid=app_page_frame_seqid,
    #         iud_seqid=iud_seqid,
    #         user_code=user_code,
    #         limit=10,
    #         offset=0,
    #     )
    #     return JSONResponse(content={"success": True, "data": dashboard_data}, status_code=200)
    # except Exception as e:
    #     main_logger.error(f"Failed to fetch embed dashboard data: {e}")
    #     raise HTTPException(status_code=500, detail="Failed to fetch embed dashboard data")


@app.post("/api/agentai/table_gpt_plus/dashboard/embed_url")
async def create_dashboard_embed_url(request: Request):
    """Create an embed URL for the dashboard page."""
    request_data = await request.json()
    payload_identifier = request_data.get("payload_identifier", {})

    frontend_base = (
        os.getenv("TABLE_GPT_FRONTEND_BASE_URL")
        or os.getenv("FRONTEND_BASE_URL")
        or request.headers.get("origin")
        or request.headers.get("referer")
    ).rstrip("/")

    query_params = []
    for key in ["appkey", "wma_object_code", "app_page_frame_seqid", "iud_seqid", "user_code"]:
        value = payload_identifier.get(key)
        if value:
            query_params.append(f"{key}={quote(str(value))}")

    embed_path = os.getenv("TABLE_GPT_DASHBOARD_EMBED_PATH", "/web/agentai/table_gpt_plus/dashboard/embed")
    embed_url = f"{frontend_base}{embed_path}?{'&'.join(query_params)}" if query_params else f"{frontend_base}{embed_path}"

    main_logger.info(f"Created dashboard embed URL: {embed_url}")

    return JSONResponse(content={
        "success": True,
        "embed_url": embed_url,
    }, status_code=200)


@app.get("/api/agentai/table_gpt_plus/dashboard/embed/session")
async def get_dashboard_embed_session(
    appkey: str = "",
    wma_object_code: str = "",
    app_page_frame_seqid: str = "",
    iud_seqid: str = "",
    user_code: str = "",
):
    """Get dashboard embed session data."""
    if not appkey:
        return JSONResponse(content={
            "success": False,
            "message": "appkey is required",
        }, status_code=400)

    return JSONResponse(content={
        "success": True,
        "payload_identifier": {
            "appkey": appkey,
            "wma_object_code": wma_object_code,
            "app_page_frame_seqid": app_page_frame_seqid,
            "iud_seqid": iud_seqid,
            "user_code": user_code,
        },
    }, status_code=200)

################################## Session Delete Endpoint #############################
@app.post("/api/agentai/table_gpt_plus/session/delete")
async def delete_session_data(data: SessionDeleteRequest):
    try:
        session_id = _safe_session_id(data.user_ref_no)
        session_dir = _runtime_session_base_dir() / session_id
        redis_deleted_keys = 0

        if session_dir.exists():
            shutil.rmtree(session_dir)
            main_logger.info(f"Deleted session runtime folder for user_ref_no: {data.user_ref_no}")
        redis_deleted_keys = _delete_redis_session_keys(session_id)
        main_logger.info(
            f"Session delete summary for user_ref_no={data.user_ref_no}: "
            f"folder_deleted={not session_dir.exists()}, redis_deleted_keys={redis_deleted_keys}"
        )

        if session_dir.exists():
            folder_deleted = False
            message = "No runtime session folder found for this user_ref_no."
        else:
            folder_deleted = True
            message = "Session runtime data deleted successfully."

        return JSONResponse(
            content={
                "status": "success",
                "message": message,
                "user_ref_no": data.user_ref_no,
                "session_dir": str(session_dir),
                "folder_deleted": folder_deleted,
                "redis_deleted_keys": redis_deleted_keys,
            },
            status_code=200,
        )
    except ValueError as e:
        return JSONResponse(
            content={
                "status": "error",
                "message": str(e),
            },
            status_code=400,
        )
    except Exception as e:
        main_logger.error(f"Error deleting session runtime data: {str(e)}")
        if debug_logger:
            debug_logger.error(f"Error deleting session runtime data: {str(e)}")
        return JSONResponse(
            content={
                "status": "error",
                "message": f"Failed to delete session runtime data: {str(e)}",
            },
            status_code=500,
        )

#################### Embed URL Creation Endpoint #############################
@app.post("/api/agentai/table_gpt_plus/chatbot/embed")
async def create_embed_url(request: Request):
    """Create an embed URL after persisting the session dataset and lightweight request metadata."""
    request_data = await request.json()

    payload_identifier = request_data.get("payload_identifier", {})
    payload_data = request_data.get("payload_data", {})
    raw_session_id = payload_identifier.get("user_ref_no")
    if not raw_session_id:
        return JSONResponse(
            content={
                "success": False,
                "message": "payload_identifier.user_ref_no is required",
            },
            status_code=400,
        )

    session_id = _safe_session_id(raw_session_id)
    table_data = payload_data.get("table_data")

    if table_data is not None and str(table_data).strip() != "":
        load_result = session_dataset_loader_node(
            {
                "session_id": session_id,
                "table_data": table_data,
            }
        )
        if not load_result.get("dataset_available"):
            return JSONResponse(
                content={
                    "success": False,
                    "message": load_result.get("error_message") or "Failed to store session dataset.",
                    "session_id": session_id,
                },
                status_code=400,
            )
        main_logger.info(f"Stored embed dataset via session_dataset_loader for session_id={session_id}")

    metadata = _store_embed_metadata(session_id, payload_identifier, payload_data)

    # Frontend base URL (prefer explicit env var, then request origin, then sensible default)
    frontend_base = (
        os.getenv("TABLE_GPT_FRONTEND_BASE_URL")
        or os.getenv("FRONTEND_BASE_URL")
        or request.headers.get("origin")
        or request.headers.get("referer")
    ).rstrip("/")

    embed_path = os.getenv("TABLE_GPT_EMBED_PATH", "/web/agentai/table_gpt_plus/chatbot/embed")
    embed_url = f"{frontend_base}{embed_path}?session_id={quote(session_id)}"

    main_logger.info(
        f"Created embed URL for user_ref_no={raw_session_id}, resolved_session_id={session_id}"
    )

    return JSONResponse(content={
        "success": True,
        "user_ref_no": raw_session_id,
        "session_id": session_id,
        "embed_url": embed_url,
        "embed_metadata": metadata,
    }, status_code=200)

########## Endpoints to manage permanent CSV-backed chatbot options and sessions, 
# allowing users to create embed sessions based on pre-stored CSV bots without needing to include the full dataset in the request. 
# This is useful for scenarios where the same CSV bot is reused across multiple sessions or users. ##########
@app.get("/api/agentai/table_gpt_plus/chatbot/embed/bots")
async def get_stored_csv_bots():
    """List permanent CSV-backed chatbot options from backend/TableGpt_Plus."""
    bots = list_stored_csv_bots()
    # print(f"Retrieved {bots} stored CSV bots for embed sessions")
    return JSONResponse(
        content={
            "success": True,
            "data": bots,
        },
        status_code=200,      
    )
    
#### Endpoint to create an embed session based on a selected permanent CSV bot, identified by bot_id.
@app.post("/api/agentai/table_gpt_plus/chatbot/embed/bots/session")
async def create_stored_csv_embed_session(data: StoredCsvEmbedRequest, request: Request):
    """Create an embed session from a permanent CSV bot without requiring table_data in the request."""
    payload_identifier = data.payload_identifier or {}
    payload_data = data.payload_data or {}

    raw_session_id = (f"{uuid.uuid4()}")
    session_id = _safe_session_id(str(raw_session_id))

    load_result = bind_stored_csv_to_session(data.bot_id, session_id)
    if not load_result.get("dataset_available"):
        return JSONResponse(
            content={
                "success": False,
                "message": load_result.get("error_message") or "Failed to load selected CSV bot.",
                "session_id": session_id,
            },
            status_code=400,
        )

    source_bot = load_result.get("source_bot", {})
    request_ai_prompt = str(payload_data.get("aiInputData") or "").strip()
    stored_ai_prompt = load_stored_csv_prompt(data.bot_id)
    merged_ai_prompt = "\n\n".join(
        prompt for prompt in [request_ai_prompt, stored_ai_prompt] if prompt
    )

    merged_payload_identifier = {
        **payload_identifier,
        "user_ref_no": session_id,
        "table_gpt_bot_id": data.bot_id,
    }
    # Ensure the stored payload identifier includes the bot name so
    # `save_app_activity_history` can persist it into `wma_object_code`.
    if isinstance(source_bot, dict) and source_bot.get("name"):
        merged_payload_identifier["wma_object_code"] = source_bot.get("name")
    # Set a fixed app key so history rows are attributed to TABLE_GPT_PLUS
    merged_payload_identifier["appkey"] = "TABLE_GPT_PLUS"
    merged_payload_data = {
        **payload_data,
        "table_context": payload_data.get("table_context") or source_bot.get("name") or data.bot_id,
        "aiInputData": merged_ai_prompt,
        "table_gpt_bot": source_bot,
    }

    metadata = _store_embed_metadata(session_id, merged_payload_identifier, merged_payload_data)

    frontend_base = (
        os.getenv("TABLE_GPT_FRONTEND_BASE_URL")
        or os.getenv("FRONTEND_BASE_URL")
        or request.headers.get("origin")
        or request.headers.get("referer")
    ).rstrip("/")

    embed_path = os.getenv("TABLE_GPT_EMBED_PATH", "/web/agentai/table_gpt_plus/chatbot/embed")
    embed_url = f"{frontend_base}{embed_path}?session_id={quote(session_id)}"

    main_logger.info(
        f"Created stored CSV embed session for bot_id={data.bot_id}, session_id={session_id}"
    )

    return JSONResponse(
        content={
            "success": True,
            "user_ref_no": raw_session_id,
            "session_id": session_id,
            "embed_url": embed_url,
            "bot": source_bot,
            "embed_metadata": metadata,
        },
        status_code=200,
    )

#### Endpoint to retrieve embed session metadata, which can be used by the frontend embed page to load necessary context without exposing the full dataset. ####
@app.get("/api/agentai/table_gpt_plus/chatbot/embed/session/{session_id}")
async def get_embed_session(session_id: str):
    safe_session_id = _safe_session_id(session_id)
    metadata = _load_embed_metadata(safe_session_id)

    if not metadata:
        return JSONResponse(
            content={
                "success": False,
                "message": "No embed session metadata found for this session_id.",
                "session_id": safe_session_id,
            },
            status_code=404,
        )

    return JSONResponse(
        content={
            "success": True,
            "session_id": safe_session_id,
            "metadata": metadata,
        },
        status_code=200,
    )


## Health check and version endpoints
@app.get("/api/agentai/table_gpt_plus/status")
async def health_check(request: Request):
    return await smart_response(request, get_status_data())

@app.get("/api/agentai/table_gpt_plus/version")
async def version(request: Request):
    return await smart_response(request, get_version_data())
