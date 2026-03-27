from fastapi import FastAPI, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import time
import json
import hashlib
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import secrets
from dotenv import load_dotenv
load_dotenv()


app = FastAPI(title="Drone Delivery Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_process_time_header(request, call_next):
    print(f">>> {request.method} {request.url}")
    return await call_next(request)


# =========================================================
# CONFIG
# =========================================================

DATABASE_URL = os.getenv("DATABASE_URL")
STALE_AFTER_MS = 4000
OFFLINE_AFTER_MS = 10000


def now_ms():
    return int(time.time() * 1000)

def compute_live_status(base_status: str, last_seen: Optional[int]):
    if not last_seen:
        return "OFFLINE", None

    age_ms = max(0, now_ms() - int(last_seen))

    if age_ms > OFFLINE_AFTER_MS:
        return "OFFLINE", age_ms

    if age_ms > STALE_AFTER_MS:
        return "STALE", age_ms

    return base_status, age_ms

def compute_status_from_last(last: Optional[dict]) -> str:
    if not last:
        return "OFFLINE"

    mode = str(last.get("mode") or "").upper()
    armed = bool(last.get("armed"))

    if mode in ["GUIDED", "AUTO", "MISSION", "LAND", "RTL"]:
        return "BUSY"

    if armed:
        return "BUSY"

    return "IDLE"


def build_source_state(last_json: Optional[str], last_seen: Optional[int], source_name: str):
    last = json.loads(last_json) if last_json else None
    base_status = compute_status_from_last(last)
    live_status, age_ms = compute_live_status(base_status, last_seen)

    return {
        "source": source_name,
        "last": last,
        "last_seen": last_seen,
        "status": live_status,
        "base_status": base_status,
        "age_ms": age_ms
    }


def resolve_effective_source(pi_state: dict, ground_state: dict, primary_source: str = "pi_bridge"):
    primary = pi_state if primary_source == "pi_bridge" else ground_state
    fallback = ground_state if primary_source == "pi_bridge" else pi_state

    primary_fresh = primary["age_ms"] is not None and primary["age_ms"] <= STALE_AFTER_MS
    fallback_fresh = fallback["age_ms"] is not None and fallback["age_ms"] <= STALE_AFTER_MS

    primary_alive = primary["age_ms"] is not None and primary["age_ms"] <= OFFLINE_AFTER_MS
    fallback_alive = fallback["age_ms"] is not None and fallback["age_ms"] <= OFFLINE_AFTER_MS

    if primary_fresh:
        return primary, "primary_fresh"

    if fallback_fresh:
        return fallback, "fallback_fresh"

    if primary_alive and fallback_alive:
        if (primary["age_ms"] or 10**18) <= (fallback["age_ms"] or 10**18):
            return primary, "primary_stale_newer"
        return fallback, "fallback_stale_newer"

    if primary_alive:
        return primary, "primary_stale_keep"

    if fallback_alive:
        return fallback, "fallback_stale_keep"

    return {
        "source": primary_source,
        "last": None,
        "last_seen": None,
        "status": "OFFLINE",
        "base_status": "OFFLINE",
        "age_ms": None
    }, "all_offline"

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        full_name TEXT DEFAULT '',
        email TEXT DEFAULT '',
        phone TEXT DEFAULT '',
        address TEXT DEFAULT '',
        created_ts BIGINT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY,
        username TEXT NOT NULL,
        created_ts BIGINT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS drones (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        last_json TEXT,
        last_seen BIGINT
    )
    """)
    cur.execute("ALTER TABLE drones ADD COLUMN IF NOT EXISTS pi_last_json TEXT")
    cur.execute("ALTER TABLE drones ADD COLUMN IF NOT EXISTS pi_last_seen BIGINT")
    cur.execute("ALTER TABLE drones ADD COLUMN IF NOT EXISTS ground_last_json TEXT")
    cur.execute("ALTER TABLE drones ADD COLUMN IF NOT EXISTS ground_last_seen BIGINT")
    cur.execute("ALTER TABLE drones ADD COLUMN IF NOT EXISTS primary_source TEXT DEFAULT 'pi_bridge'")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id TEXT PRIMARY KEY,
        created_by TEXT NOT NULL,
        dropoff_json TEXT NOT NULL,
        note TEXT,
        status TEXT NOT NULL,
        created_ts BIGINT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS missions (
        id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        drone_id TEXT NOT NULL,
        status TEXT NOT NULL,
        altitude_m DOUBLE PRECISION NOT NULL,
        warehouse_lat DOUBLE PRECISION NOT NULL,
        warehouse_lng DOUBLE PRECISION NOT NULL,
        waypoints_json TEXT NOT NULL,
        created_by TEXT NOT NULL,
        created_ts BIGINT NOT NULL
    )
    """)

    cur.execute("""
    UPDATE drones
    SET pi_last_json = last_json
    WHERE pi_last_json IS NULL AND last_json IS NOT NULL
    """)

    cur.execute("""
    UPDATE drones
    SET pi_last_seen = last_seen
    WHERE pi_last_seen IS NULL AND last_seen IS NOT NULL
    """)

    # Seed admin
    cur.execute("SELECT username FROM users WHERE username = %s", ("admin",))
    if not cur.fetchone():
        cur.execute("""
        INSERT INTO users (
            id, username, password_hash, role,
            full_name, email, phone, address, created_ts
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            "user_admin",
            "admin",
            hash_password("admin123"),
            "admin",
            "Administrator",
            "",
            "",
            "",
            now_ms()
        ))

    cur.execute("SELECT id FROM drones WHERE id = %s", ("drone_1",))
    if not cur.fetchone():
        cur.execute("""
        INSERT INTO drones (
            id, name, status,
            pi_last_json, pi_last_seen,
            ground_last_json, ground_last_seen,
            primary_source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            "drone_1",
            "SITL-1",
            "IDLE",
            None, None,
            None, None,
            "pi_bridge"
        ))

    conn.commit()
    cur.close()
    conn.close()


@app.on_event("startup")
def startup():
    init_db()


# =========================================================
# MODELS
# =========================================================

class LatLng(BaseModel):
    lat: float
    lng: float


class CreateOrderReq(BaseModel):
    dropoff: LatLng
    note: Optional[str] = None


class CreateMissionReq(BaseModel):
    order_id: str
    drone_id: str
    altitude_m: float = 20.0
    warehouse_lat: float = 10.850602
    warehouse_lng: float = 106.771948


class TelemetryReq(BaseModel):
    drone_id: str
    lat: float
    lng: float
    alt_m: float
    groundspeed_mps: float
    battery_percent: Optional[float] = None
    mode: Optional[str] = None
    armed: Optional[bool] = None
    ts_ms: int
    source: Optional[str] = "pi_bridge"


class EventReq(BaseModel):
    drone_id: str
    mission_id: str
    type: str
    detail: Optional[str] = None
    ts_ms: int


class RegisterReq(BaseModel):
    username: str
    password: str


class LoginReq(BaseModel):
    username: str
    password: str


class UpdateProfileReq(BaseModel):
    full_name: Optional[str] = ""
    email: Optional[str] = ""
    phone: Optional[str] = ""
    address: Optional[str] = ""


class ChangePasswordReq(BaseModel):
    old_password: str
    new_password: str
    confirm_new_password: str


# =========================================================
# DB HELPERS
# =========================================================

def row_to_user(row):
    if not row:
        return None
    return {
        "id": row["id"],
        "username": row["username"],
        "password_hash": row["password_hash"],
        "role": row["role"],
        "full_name": row["full_name"] or "",
        "email": row["email"] or "",
        "phone": row["phone"] or "",
        "address": row["address"] or "",
        "created_ts": row["created_ts"]
    }


def row_to_drone(row):
    if not row:
        return None

    primary_source = row.get("primary_source") or "pi_bridge"

    pi_state = build_source_state(
        row.get("pi_last_json"),
        row.get("pi_last_seen"),
        "pi_bridge"
    )

    ground_state = build_source_state(
        row.get("ground_last_json"),
        row.get("ground_last_seen"),
        "ground_relay"
    )

    effective, reason = resolve_effective_source(pi_state, ground_state, primary_source)

    return {
        "id": row["id"],
        "name": row["name"],
        "status": effective["status"],
        "base_status": effective["base_status"],
        "last": effective["last"],
        "last_seen": effective["last_seen"],
        "telemetry_age_ms": effective["age_ms"],
        "telemetry_source": effective["source"],
        "telemetry_reason": reason,
        "primary_source": primary_source,
        "sources": {
            "pi_bridge": {
                "status": pi_state["status"],
                "age_ms": pi_state["age_ms"],
                "last_seen": pi_state["last_seen"]
            },
            "ground_relay": {
                "status": ground_state["status"],
                "age_ms": ground_state["age_ms"],
                "last_seen": ground_state["last_seen"]
            }
        }
    }


def row_to_order(row):
    if not row:
        return None
    return {
        "id": row["id"],
        "created_by": row["created_by"],
        "dropoff": json.loads(row["dropoff_json"]),
        "note": row["note"],
        "status": row["status"],
        "created_ts": row["created_ts"]
    }


def row_to_mission(row):
    if not row:
        return None
    return {
        "id": row["id"],
        "order_id": row["order_id"],
        "drone_id": row["drone_id"],
        "status": row["status"],
        "altitude_m": row["altitude_m"],
        "warehouse_lat": row["warehouse_lat"],
        "warehouse_lng": row["warehouse_lng"],
        "waypoints": json.loads(row["waypoints_json"]),
        "created_by": row["created_by"],
        "created_ts": row["created_ts"]
    }


def get_user_by_username(username: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE username = %s", (username,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row_to_user(row)


def get_order_by_id(order_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM orders WHERE id = %s", (order_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row_to_order(row)


def get_mission_by_id(mission_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM missions WHERE id = %s", (mission_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row_to_mission(row)


def get_mission_by_order_id(order_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT * FROM missions
    WHERE order_id = %s
    ORDER BY created_ts DESC
    LIMIT 1
    """, (order_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row_to_mission(row)


def get_drone_row_by_id(drone_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM drones WHERE id = %s", (drone_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

def get_drone_by_id(drone_id: str):
    row = get_drone_row_by_id(drone_id)
    return row_to_drone(row)


def get_next_order_id():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    SELECT COALESCE(MAX(CAST(SUBSTRING(id FROM 7) AS INTEGER)), 0) AS max_num
    FROM orders
    WHERE id ~ '^order_[0-9]+$'
    """)

    row = cur.fetchone()
    cur.close()
    conn.close()

    max_num = int(row["max_num"] or 0)
    return f"order_{max_num + 1}"


# =========================================================
# AUTH HELPERS
# =========================================================

def extract_token(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    if not authorization.startswith("Bearer "):
        return None
    return authorization.replace("Bearer ", "").strip()


def save_session(token: str, username: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO sessions (token, username, created_ts)
    VALUES (%s, %s, %s)
    ON CONFLICT (token)
    DO UPDATE SET
        username = EXCLUDED.username,
        created_ts = EXCLUDED.created_ts
    """, (token, username, now_ms()))
    conn.commit()
    cur.close()
    conn.close()


def get_session_username(token: str) -> Optional[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT username FROM sessions WHERE token = %s", (token,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row["username"] if row else None


def delete_session(token: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM sessions WHERE token = %s", (token,))
    conn.commit()
    cur.close()
    conn.close()


def get_current_user(authorization: Optional[str]):
    token = extract_token(authorization)
    if not token:
        return None
    username = get_session_username(token)
    if not username:
        return None
    return get_user_by_username(username)


def require_user(authorization: Optional[str]):
    user = get_current_user(authorization)
    if not user:
        return {"error": "unauthorized"}
    return user


def require_admin(authorization: Optional[str]):
    user = get_current_user(authorization)
    if not user:
        return {"error": "unauthorized"}
    if user["role"] != "admin":
        return {"error": "forbidden"}
    return user


def public_user(user: dict) -> dict:
    return {
        "id": user["id"],
        "username": user["username"],
        "role": user["role"],
        "full_name": user.get("full_name", ""),
        "email": user.get("email", ""),
        "phone": user.get("phone", ""),
        "address": user.get("address", ""),
        "created_ts": user.get("created_ts")
    }

def get_active_mission_by_order_id(order_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT * FROM missions
    WHERE order_id = %s
      AND UPPER(status) NOT IN ('DONE', 'FAILED', 'CANCELLED')
    ORDER BY created_ts DESC
    LIMIT 1
    """, (order_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row_to_mission(row)

def get_next_mission_id():
    
    return f"mission_{now_ms()}"


# =========================================================
# SYSTEM
# =========================================================

@app.get("/health")
def health():
    return {"ok": True, "ts_ms": now_ms()}


# =========================================================
# AUTH API
# =========================================================

@app.post("/auth/register")
def register(req: RegisterReq):
    username = req.username.strip()

    if len(username) < 3:
        return {"error": "username_too_short"}

    if len(req.password) < 4:
        return {"error": "password_too_short"}

    if get_user_by_username(username):
        return {"error": "username_exists"}

    uid = f"user_{username}"

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO users (
        id, username, password_hash, role,
        full_name, email, phone, address, created_ts
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        uid,
        username,
        hash_password(req.password),
        "user",
        "",
        "",
        "",
        "",
        now_ms()
    ))
    conn.commit()
    cur.close()
    conn.close()

    user = get_user_by_username(username)
    return {"ok": True, "user": public_user(user)}


@app.post("/auth/login")
def login(req: LoginReq):
    username = req.username.strip()
    user = get_user_by_username(username)

    if not user or user["password_hash"] != hash_password(req.password):
        return {"error": "invalid_credentials"}

    token = secrets.token_hex(32)
    save_session(token, username)

    return {
        "ok": True,
        "token": token,
        "user": public_user(user)
    }


@app.get("/auth/me")
def auth_me(authorization: Optional[str] = Header(default=None)):
    user = require_user(authorization)
    if isinstance(user, dict) and user.get("error"):
        return user
    return {"ok": True, "user": public_user(user)}


@app.post("/auth/logout")
def logout(authorization: Optional[str] = Header(default=None)):
    token = extract_token(authorization)
    if token:
        delete_session(token)
    return {"ok": True}


@app.put("/auth/change-password")
def change_password(req: ChangePasswordReq, authorization: Optional[str] = Header(default=None)):
    user = require_user(authorization)
    if isinstance(user, dict) and user.get("error"):
        return user

    if hash_password(req.old_password) != user["password_hash"]:
        return {"error": "wrong_old_password"}

    if len(req.new_password) < 4:
        return {"error": "password_too_short"}

    if req.new_password != req.confirm_new_password:
        return {"error": "confirm_password_not_match"}

    if req.old_password == req.new_password:
        return {"error": "same_as_old_password"}

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    UPDATE users
    SET password_hash = %s
    WHERE username = %s
    """, (hash_password(req.new_password), user["username"]))
    conn.commit()
    cur.close()
    conn.close()

    return {"ok": True, "message": "password_changed"}


# =========================================================
# USER PROFILE API
# =========================================================

@app.put("/users/me")
def update_me(req: UpdateProfileReq, authorization: Optional[str] = Header(default=None)):
    user = require_user(authorization)
    if isinstance(user, dict) and user.get("error"):
        return user

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    UPDATE users
    SET full_name = %s, email = %s, phone = %s, address = %s
    WHERE username = %s
    """, (
        (req.full_name or "").strip(),
        (req.email or "").strip(),
        (req.phone or "").strip(),
        (req.address or "").strip(),
        user["username"]
    ))
    conn.commit()
    cur.close()
    conn.close()

    updated = get_user_by_username(user["username"])
    return {"ok": True, "user": public_user(updated)}


@app.get("/users")
def list_users(authorization: Optional[str] = Header(default=None)):
    admin = require_admin(authorization)
    if isinstance(admin, dict) and admin.get("error"):
        return admin

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users ORDER BY created_ts DESC")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [public_user(row_to_user(r)) for r in rows]


# =========================================================
# ORDERS API
# =========================================================
@app.post("/orders/{order_id}/dispatch")
def dispatch_order(order_id: str, authorization: Optional[str] = Header(default=None)):
    admin = require_admin(authorization)
    if isinstance(admin, dict) and admin.get("error"):
        return admin

    order = get_order_by_id(order_id)
    if not order:
        return {"error": "order_not_found"}

    status_upper = str(order["status"]).upper()

    if status_upper == "CANCELLED":
        return {"error": "order_cancelled"}

    if status_upper in ["DELIVERED", "COMPLETED", "DONE"]:
        return {"error": "order_completed"}

    existing_mission = get_active_mission_by_order_id(order_id)
    if existing_mission:
        return {"error": "mission_already_exists"}

    drone = get_drone_by_id("drone_1")
    if not drone:
        return {"error": "drone_not_found"}

    mid = get_next_mission_id()
    drop = order["dropoff"]

    mission = {
        "id": mid,
        "order_id": order_id,
        "drone_id": "drone_1",
        "status": "START_REQUESTED",
        "altitude_m": 20.0,
        "warehouse_lat": 10.850602,
        "warehouse_lng": 106.771948,
        "waypoints": [
            {
                "lat": drop["lat"],
                "lng": drop["lng"],
                "alt_m": 20.0
            }
        ],
        "created_by": admin["username"],
        "created_ts": now_ms()
    }

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO missions (
        id, order_id, drone_id, status, altitude_m,
        warehouse_lat, warehouse_lng, waypoints_json, created_by, created_ts
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        mission["id"],
        mission["order_id"],
        mission["drone_id"],
        mission["status"],
        mission["altitude_m"],
        mission["warehouse_lat"],
        mission["warehouse_lng"],
        json.dumps(mission["waypoints"]),
        mission["created_by"],
        mission["created_ts"]
    ))

    cur.execute("""
    UPDATE orders
    SET status = %s
    WHERE id = %s
    """, ("ASSIGNED", order_id))

    conn.commit()
    cur.close()
    conn.close()

    return {"ok": True, "mission": mission}

@app.post("/orders")
def create_order(req: CreateOrderReq, authorization: Optional[str] = Header(default=None)):
    user = require_user(authorization)
    if isinstance(user, dict) and user.get("error"):
        return user

    oid = get_next_order_id()

    order = {
        "id": oid,
        "created_by": user["username"],
        "dropoff": req.dropoff.model_dump(),
        "note": req.note,
        "status": "CREATED",
        "created_ts": now_ms()
    }

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO orders (id, created_by, dropoff_json, note, status, created_ts)
    VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        order["id"],
        order["created_by"],
        json.dumps(order["dropoff"]),
        order["note"],
        order["status"],
        order["created_ts"]
    ))
    conn.commit()
    cur.close()
    conn.close()

    return order


@app.get("/orders")
def list_orders(authorization: Optional[str] = Header(default=None)):
    user = require_user(authorization)
    if isinstance(user, dict) and user.get("error"):
        return user

    conn = get_conn()
    cur = conn.cursor()

    if user["role"] == "admin":
        cur.execute("SELECT * FROM orders ORDER BY created_ts DESC")
    else:
        cur.execute("""
        SELECT * FROM orders
        WHERE created_by = %s
        ORDER BY created_ts DESC
        """, (user["username"],))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [row_to_order(r) for r in rows]


@app.post("/orders/{order_id}/cancel")
def cancel_order(order_id: str, authorization: Optional[str] = Header(default=None)):
    user = require_user(authorization)
    if isinstance(user, dict) and user.get("error"):
        return user

    order = get_order_by_id(order_id)
    if not order:
        return {"error": "order_not_found"}

    is_admin = user["role"] == "admin"
    is_owner = order["created_by"] == user["username"]

    if not is_admin and not is_owner:
        return {"error": "forbidden"}

    blocked_statuses = ["DELIVERED", "COMPLETED", "DONE", "CANCELLED"]
    if str(order["status"]).upper() in blocked_statuses:
        return {"error": "cannot_cancel_order"}

    mission = get_mission_by_order_id(order_id)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    UPDATE orders
    SET status = %s
    WHERE id = %s
    """, ("CANCELLED", order_id))

    if mission and str(mission["status"]).upper() not in ["DONE", "FAILED", "CANCELLED"]:
        cur.execute("""
        UPDATE missions
        SET status = %s
        WHERE id = %s
        """, ("CANCELLED", mission["id"]))

    conn.commit()
    cur.close()
    conn.close()

    return {"ok": True, "order_id": order_id, "status": "CANCELLED"}


# =========================================================
# MISSIONS API
# =========================================================

@app.post("/missions")
def create_mission(req: CreateMissionReq, authorization: Optional[str] = Header(default=None)):
    admin = require_admin(authorization)
    if isinstance(admin, dict) and admin.get("error"):
        return admin

    order = get_order_by_id(req.order_id)
    if not order:
        return {"error": "order_not_found"}

    if str(order["status"]).upper() == "CANCELLED":
        return {"error": "order_cancelled"}

    drone_row = get_drone_row_by_id(req.drone_id)
    if not drone_row:
        return {"error": "drone_not_found"}

    mid = get_next_mission_id()
    drop = order["dropoff"]

    waypoints = [
        {
            "lat": drop["lat"],
            "lng": drop["lng"],
            "alt_m": req.altitude_m
        }
    ]

    mission = {
        "id": mid,
        "order_id": req.order_id,
        "drone_id": req.drone_id,
        "status": "ASSIGNED",
        "altitude_m": req.altitude_m,
        "warehouse_lat": req.warehouse_lat,
        "warehouse_lng": req.warehouse_lng,
        "waypoints": waypoints,
        "created_by": admin["username"],
        "created_ts": now_ms()
    }

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO missions (
        id, order_id, drone_id, status, altitude_m,
        warehouse_lat, warehouse_lng, waypoints_json, created_by, created_ts
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        mission["id"],
        mission["order_id"],
        mission["drone_id"],
        mission["status"],
        mission["altitude_m"],
        mission["warehouse_lat"],
        mission["warehouse_lng"],
        json.dumps(mission["waypoints"]),
        mission["created_by"],
        mission["created_ts"]
    ))

    cur.execute("""
    UPDATE orders
    SET status = %s
    WHERE id = %s
    """, ("QUEUED", req.order_id))

    conn.commit()
    cur.close()
    conn.close()

    return mission


@app.post("/missions/{mission_id}/start")
def start_mission(mission_id: str, authorization: Optional[str] = Header(default=None)):
    admin = require_admin(authorization)
    if isinstance(admin, dict) and admin.get("error"):
        return admin

    mission = get_mission_by_id(mission_id)
    if not mission:
        return {"error": "mission_not_found"}

    if str(mission["status"]).upper() == "CANCELLED":
        return {"error": "mission_cancelled"}

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    UPDATE missions
    SET status = %s
    WHERE id = %s
    """, ("START_REQUESTED", mission_id))

    cur.execute("""
    UPDATE orders
    SET status = %s
    WHERE id = %s
    """, ("ASSIGNED", mission["order_id"]))

    conn.commit()
    cur.close()
    conn.close()

    return {"ok": True}


@app.get("/missions")
def list_missions(authorization: Optional[str] = Header(default=None)):
    user = require_user(authorization)
    if isinstance(user, dict) and user.get("error"):
        return user

    conn = get_conn()
    cur = conn.cursor()

    if user["role"] == "admin":
        cur.execute("SELECT * FROM missions ORDER BY created_ts DESC")
        rows = cur.fetchall()
    else:
        cur.execute("""
        SELECT m.*
        FROM missions m
        JOIN orders o ON m.order_id = o.id
        WHERE o.created_by = %s
        ORDER BY m.created_ts DESC
        """, (user["username"],))
        rows = cur.fetchall()

    cur.close()
    conn.close()
    return [row_to_mission(r) for r in rows]


# =========================================================
# BRIDGE API
# =========================================================

@app.get("/bridge/missions/next")
def bridge_next_mission(drone_id: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    SELECT * FROM missions
    WHERE drone_id = %s AND status = %s
    ORDER BY created_ts ASC
    LIMIT 1
    """, (drone_id, "START_REQUESTED"))

    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return {"mission": None}

    mission = row_to_mission(row)

    cur.execute("""
    UPDATE missions
    SET status = %s
    WHERE id = %s
    """, ("RUNNING", mission["id"]))

    cur.execute("""
    UPDATE orders
    SET status = %s
    WHERE id = %s
    """, ("IN_FLIGHT", mission["order_id"]))

    conn.commit()
    cur.close()
    conn.close()

    updated = get_mission_by_id(mission["id"])
    return {"mission": updated}


@app.post("/bridge/telemetry")
def bridge_telemetry(req: TelemetryReq):
    drone_row = get_drone_row_by_id(req.drone_id)
    if not drone_row:
        return {"error": "drone_not_found"}

    source = (req.source or "pi_bridge").strip().lower()

    last_data = {
        "lat": req.lat,
        "lng": req.lng,
        "alt_m": req.alt_m,
        "groundspeed_mps": req.groundspeed_mps,
        "battery_percent": req.battery_percent,
        "mode": req.mode,
        "armed": req.armed,
        "ts_ms": req.ts_ms,
        "source": source
    }

    mode_upper = str(req.mode or "").upper()
    if mode_upper in ["GUIDED", "AUTO", "MISSION", "LAND", "RTL"]:
        base_status = "BUSY"
    elif req.armed:
        base_status = "BUSY"
    else:
        base_status = "IDLE"

    conn = get_conn()
    cur = conn.cursor()

    if source == "pi_bridge":
        cur.execute("""
        UPDATE drones
        SET status = %s,
            pi_last_json = %s,
            pi_last_seen = %s
        WHERE id = %s
        """, (base_status, json.dumps(last_data), now_ms(), req.drone_id))

    elif source == "ground_relay":
        cur.execute("""
        UPDATE drones
        SET status = %s,
            ground_last_json = %s,
            ground_last_seen = %s
        WHERE id = %s
        """, (base_status, json.dumps(last_data), now_ms(), req.drone_id))

    else:
        cur.close()
        conn.close()
        return {"error": "invalid_source"}

    conn.commit()
    cur.close()
    conn.close()

    return {"ok": True, "source": source}


@app.post("/bridge/event")
def bridge_event(req: EventReq):
    mission = get_mission_by_id(req.mission_id)
    if not mission:
        return {"ok": False, "error": "mission_not_found"}

    if str(mission["status"]).upper() == "CANCELLED":
        return {"ok": False, "error": "mission_cancelled"}

    conn = get_conn()
    cur = conn.cursor()

    if req.type == "ARRIVED":
        cur.execute("UPDATE orders SET status = %s WHERE id = %s", ("ARRIVED", mission["order_id"]))

    elif req.type == "DELIVERED":
        cur.execute("UPDATE orders SET status = %s WHERE id = %s", ("DELIVERED", mission["order_id"]))

    elif req.type == "COMPLETED":
        cur.execute("UPDATE orders SET status = %s WHERE id = %s", ("COMPLETED", mission["order_id"]))
        cur.execute("UPDATE missions SET status = %s WHERE id = %s", ("DONE", req.mission_id))

    elif req.type == "FAILED":
        cur.execute("UPDATE orders SET status = %s WHERE id = %s", ("FAILED", mission["order_id"]))
        cur.execute("UPDATE missions SET status = %s WHERE id = %s", ("FAILED", req.mission_id))

    conn.commit()
    cur.close()
    conn.close()

    return {"ok": True}


@app.post("/bridge/missions/{mission_id}/complete")
def complete_mission(mission_id: str):
    mission = get_mission_by_id(mission_id)
    if not mission:
        return {"error": "mission_not_found"}

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE missions SET status = %s WHERE id = %s", ("DONE", mission_id))
    cur.execute("UPDATE orders SET status = %s WHERE id = %s", ("COMPLETED", mission["order_id"]))
    conn.commit()
    cur.close()
    conn.close()

    return {"ok": True}


# =========================================================
# DRONES API
# =========================================================

@app.get("/drones")
def list_drones(authorization: Optional[str] = Header(default=None)):
    user = require_user(authorization)
    if isinstance(user, dict) and user.get("error"):
        return user

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM drones ORDER BY id ASC")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [row_to_drone(r) for r in rows]


@app.get("/drones/{drone_id}")
def get_drone_api(drone_id: str, authorization: Optional[str] = Header(default=None)):
    user = require_user(authorization)
    if isinstance(user, dict) and user.get("error"):
        return user

    drone = get_drone_by_id(drone_id)
    return drone or {"error": "drone_not_found"}