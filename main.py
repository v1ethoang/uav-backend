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


def now_ms():
    return int(time.time() * 1000)


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

    # Seed drone
    cur.execute("SELECT id FROM drones WHERE id = %s", ("drone_1",))
    if not cur.fetchone():
        cur.execute("""
        INSERT INTO drones (id, name, status, last_json, last_seen)
        VALUES (%s, %s, %s, %s, %s)
        """, ("drone_1", "SITL-1", "IDLE", None, None))

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
    last = json.loads(row["last_json"]) if row["last_json"] else None
    return {
        "id": row["id"],
        "name": row["name"],
        "status": row["status"],
        "last": last,
        "last_seen": row["last_seen"]
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


def get_drone_by_id(drone_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM drones WHERE id = %s", (drone_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row_to_drone(row)


def get_next_order_id():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT id
    FROM orders
    WHERE id LIKE 'order_%'
    ORDER BY created_ts DESC
    LIMIT 500
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    max_num = 0
    for row in rows:
        oid = str(row["id"])
        if not oid.startswith("order_"):
            continue
        suffix = oid.replace("order_", "", 1)
        if suffix.isdigit():
            n = int(suffix)
            if n > max_num:
                max_num = n

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

    token = f"tok_{int(time.time())}_{username}"
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

    drone = get_drone_by_id(req.drone_id)
    if not drone:
        return {"error": "drone_not_found"}

    mid = f"mission_{int(time.time() * 1000)}"
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
    drone = get_drone_by_id(req.drone_id)
    if not drone:
        return {"error": "drone_not_found"}

    last_data = {
        "lat": req.lat,
        "lng": req.lng,
        "alt_m": req.alt_m,
        "groundspeed_mps": req.groundspeed_mps,
        "battery_percent": req.battery_percent,
        "mode": req.mode,
        "armed": req.armed,
        "ts_ms": req.ts_ms
    }

    if req.mode in ["GUIDED", "AUTO", "MISSION", "LAND"]:
        status = "BUSY"
    elif req.armed:
        status = "BUSY"
    else:
        status = "IDLE"

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    UPDATE drones
    SET status = %s, last_json = %s, last_seen = %s
    WHERE id = %s
    """, (status, json.dumps(last_data), now_ms(), req.drone_id))
    conn.commit()
    cur.close()
    conn.close()

    return {"ok": True}


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