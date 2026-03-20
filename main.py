from fastapi import FastAPI, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict
import time
import uuid

app = FastAPI(title="Drone Delivery Backend (MVP)")

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
# IN-MEMORY DATA
# =========================================================

def now_ms():
    return int(time.time() * 1000)


DRONES: Dict[str, dict] = {
    "drone_1": {
        "id": "drone_1",
        "name": "SITL-1",
        "status": "IDLE",
        "last": None,
        "last_seen": None
    }
}

ORDERS: Dict[str, dict] = {}
MISSIONS: Dict[str, dict] = {}

USERS: Dict[str, dict] = {
    "admin": {
        "id": "user_admin",
        "username": "admin",
        "password": "admin123",
        "role": "admin",
        "created_ts": now_ms()
    }
}

TOKENS: Dict[str, str] = {}  # token -> username


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


# =========================================================
# AUTH HELPERS
# =========================================================

def extract_token(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    if not authorization.startswith("Bearer "):
        return None
    return authorization.replace("Bearer ", "").strip()


def get_current_user(authorization: Optional[str]):
    token = extract_token(authorization)
    if not token:
        return None
    username = TOKENS.get(token)
    if not username:
        return None
    return USERS.get(username)


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

    if username in USERS:
        return {"error": "username_exists"}

    uid = f"user_{len(USERS) + 1}"

    USERS[username] = {
        "id": uid,
        "username": username,
        "password": req.password,
        "role": "user",
        "created_ts": now_ms()
    }

    return {"ok": True, "user": {"id": uid, "username": username, "role": "user"}}


@app.post("/auth/login")
def login(req: LoginReq):
    username = req.username.strip()
    user = USERS.get(username)

    if not user or user["password"] != req.password:
        return {"error": "invalid_credentials"}

    token = str(uuid.uuid4())
    TOKENS[token] = username

    return {
        "ok": True,
        "token": token,
        "user": {
            "id": user["id"],
            "username": user["username"],
            "role": user["role"]
        }
    }


@app.get("/auth/me")
def auth_me(authorization: Optional[str] = Header(default=None)):
    user = require_user(authorization)
    if isinstance(user, dict) and user.get("error"):
        return user

    return {
        "ok": True,
        "user": {
            "id": user["id"],
            "username": user["username"],
            "role": user["role"]
        }
    }


@app.post("/auth/logout")
def logout(authorization: Optional[str] = Header(default=None)):
    token = extract_token(authorization)
    if token and token in TOKENS:
        del TOKENS[token]
    return {"ok": True}


# =========================================================
# USER API
# =========================================================

@app.post("/orders")
def create_order(req: CreateOrderReq, authorization: Optional[str] = Header(default=None)):
    user = require_user(authorization)
    if isinstance(user, dict) and user.get("error"):
        return user

    oid = f"order_{len(ORDERS)+1}"

    ORDERS[oid] = {
        "id": oid,
        "created_by": user["username"],
        "dropoff": req.dropoff.model_dump(),
        "note": req.note,
        "status": "CREATED",
        "created_ts": now_ms()
    }

    return ORDERS[oid]


@app.get("/orders")
def list_orders(authorization: Optional[str] = Header(default=None)):
    user = require_user(authorization)
    if isinstance(user, dict) and user.get("error"):
        return user

    if user["role"] == "admin":
        return list(ORDERS.values())

    return [o for o in ORDERS.values() if o.get("created_by") == user["username"]]


# =========================================================
# OPERATOR / ADMIN API
# =========================================================

@app.post("/missions")
def create_mission(req: CreateMissionReq, authorization: Optional[str] = Header(default=None)):
    admin = require_admin(authorization)
    if isinstance(admin, dict) and admin.get("error"):
        return admin

    if req.order_id not in ORDERS:
        return {"error": "order_not_found"}

    if req.drone_id not in DRONES:
        return {"error": "drone_not_found"}

    mid = f"mission_{len(MISSIONS)+1}"

    order = ORDERS[req.order_id]
    drop = order["dropoff"]

    waypoints = [
        {
            "lat": drop["lat"],
            "lng": drop["lng"],
            "alt_m": req.altitude_m
        }
    ]

    MISSIONS[mid] = {
        "id": mid,
        "order_id": req.order_id,
        "drone_id": req.drone_id,
        "status": "START_REQUESTED",
        "altitude_m": req.altitude_m,
        "warehouse_lat": req.warehouse_lat,
        "warehouse_lng": req.warehouse_lng,
        "waypoints": waypoints,
        "created_by": admin["username"],
        "created_ts": now_ms()
    }

    ORDERS[req.order_id]["status"] = "QUEUED"

    return MISSIONS[mid]


@app.post("/missions/{mission_id}/start")
def start_mission(mission_id: str, authorization: Optional[str] = Header(default=None)):
    admin = require_admin(authorization)
    if isinstance(admin, dict) and admin.get("error"):
        return admin

    if mission_id not in MISSIONS:
        return {"error": "mission_not_found"}

    MISSIONS[mission_id]["status"] = "START_REQUESTED"
    order_id = MISSIONS[mission_id]["order_id"]
    ORDERS[order_id]["status"] = "ASSIGNED"

    return {"ok": True}


@app.get("/missions")
def list_missions(authorization: Optional[str] = Header(default=None)):
    user = require_user(authorization)
    if isinstance(user, dict) and user.get("error"):
        return user

    if user["role"] == "admin":
        return list(MISSIONS.values())

    my_order_ids = {o["id"] for o in ORDERS.values() if o.get("created_by") == user["username"]}
    return [m for m in MISSIONS.values() if m["order_id"] in my_order_ids]


@app.get("/users")
def list_users(authorization: Optional[str] = Header(default=None)):
    admin = require_admin(authorization)
    if isinstance(admin, dict) and admin.get("error"):
        return admin

    return [
        {
            "id": u["id"],
            "username": u["username"],
            "role": u["role"],
            "created_ts": u["created_ts"]
        }
        for u in USERS.values()
    ]


# =========================================================
# BRIDGE API
# =========================================================

@app.get("/bridge/missions/next")
def bridge_next_mission(drone_id: str):
    for m in MISSIONS.values():
        if m["drone_id"] == drone_id and m["status"] == "START_REQUESTED":
            m["status"] = "RUNNING"

            order_id = m["order_id"]
            ORDERS[order_id]["status"] = "IN_FLIGHT"

            return {"mission": m}

    return {"mission": None}


@app.post("/bridge/telemetry")
def bridge_telemetry(req: TelemetryReq):
    if req.drone_id in DRONES:
        DRONES[req.drone_id]["last_seen"] = now_ms()
        DRONES[req.drone_id]["last"] = {
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
            DRONES[req.drone_id]["status"] = "BUSY"
        elif req.armed:
            DRONES[req.drone_id]["status"] = "BUSY"
        else:
            DRONES[req.drone_id]["status"] = "IDLE"

    return {"ok": True}


@app.post("/bridge/event")
def bridge_event(req: EventReq):
    m = MISSIONS.get(req.mission_id)

    if not m:
        return {"ok": False, "error": "mission_not_found"}

    oid = m["order_id"]

    if req.type == "ARRIVED":
        ORDERS[oid]["status"] = "ARRIVED"

    elif req.type == "DELIVERED":
        ORDERS[oid]["status"] = "DELIVERED"

    elif req.type == "COMPLETED":
        ORDERS[oid]["status"] = "COMPLETED"
        m["status"] = "DONE"

    elif req.type == "FAILED":
        ORDERS[oid]["status"] = "FAILED"
        m["status"] = "FAILED"

    return {"ok": True}


@app.post("/bridge/missions/{mission_id}/complete")
def complete_mission(mission_id: str):
    if mission_id not in MISSIONS:
        return {"error": "mission_not_found"}

    MISSIONS[mission_id]["status"] = "DONE"
    oid = MISSIONS[mission_id]["order_id"]
    ORDERS[oid]["status"] = "COMPLETED"

    return {"ok": True}


# =========================================================
# DRONES
# =========================================================

@app.get("/drones")
def list_drones(authorization: Optional[str] = Header(default=None)):
    user = require_user(authorization)
    if isinstance(user, dict) and user.get("error"):
        return user

    return list(DRONES.values())


@app.get("/drones/{drone_id}")
def get_drone(drone_id: str, authorization: Optional[str] = Header(default=None)):
    user = require_user(authorization)
    if isinstance(user, dict) and user.get("error"):
        return user

    return DRONES.get(drone_id, {"error": "drone_not_found"})