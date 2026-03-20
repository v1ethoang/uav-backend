import time
import math
import requests

from dronekit import connect, VehicleMode, LocationGlobalRelative
from pymavlink import mavutil


# =========================================================
# CONFIG
# =========================================================

BACKEND = "http://127.0.0.1:8080"   # đổi nếu backend chạy chỗ khác
DRONE_ID = "drone_1"
CONNECTION = "127.0.0.1:14552"

MISSION_POLL_SEC = 2.0
TELEMETRY_TIMEOUT = 1.0
EVENT_TIMEOUT = 2.0
REQUEST_TIMEOUT = 2.0


# ==============================
# FLIGHT TUNING
# ==============================
MIN_CRUISE_SPEED = 3.0
MAX_CRUISE_SPEED = 10.0
APPROACH_MIN_SPEED = 1.8
DESCEND_SPEED = 1.8
RETURN_MAX_SPEED = 8.0

WAYPOINT_RADIUS = 1.0
ALT_TOLERANCE = 0.8
DELIVERY_ALT = 6.0
HOME_LAND_ALT = 4.0
DELIVERY_HOLD_SEC = 0.3
GOTO_TIMEOUT = 180

APPROACH_BRAKE_DISTANCE = 3.0
GOTO_LOG_PERIOD = 1.0


# =========================================================
# HTTP SESSION
# =========================================================

http = requests.Session()


# =========================================================
# UTIL
# =========================================================

def now_ms():
    return int(time.time() * 1000)


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")


def get_distance(a, b):
    """
    Khoảng cách ngang giữa 2 điểm GPS (m).
    """
    R = 6371000.0

    lat1 = math.radians(a.lat)
    lat2 = math.radians(b.lat)
    dlat = lat2 - lat1
    dlon = math.radians(b.lon - a.lon)

    h = (
        math.sin(dlat / 2.0) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
    )
    return 2.0 * R * math.asin(math.sqrt(h))


def clamp_speed(speed_mps, max_speed):
    return max(0.8, min(speed_mps, max_speed))


def compute_cruise_speed_by_distance(distance_m):
    if distance_m >= 200:
        return 10.0
    elif distance_m >= 120:
        return 8.0
    elif distance_m >= 60:
        return 6.0
    elif distance_m >= 25:
        return 4.5
    else:
        return MIN_CRUISE_SPEED


def compute_approach_speed(remaining_dist, cruise_speed):
    if remaining_dist <= APPROACH_BRAKE_DISTANCE:
        return APPROACH_MIN_SPEED
    return cruise_speed


def set_vehicle_speed(vehicle, speed_mps, max_speed=MAX_CRUISE_SPEED):
    speed_mps = clamp_speed(speed_mps, max_speed)
    vehicle.groundspeed = speed_mps
    return speed_mps


def ensure_home(vehicle, wait_sec=20):
    """
    Đảm bảo có home_location.
    Nếu chưa có thì chờ. Nếu vẫn chưa có thì fallback về vị trí hiện tại.
    """
    if vehicle.home_location is not None:
        return vehicle.home_location

    log("Waiting home location init...")
    start = time.time()

    while time.time() - start < wait_sec:
        if vehicle.home_location is not None:
            return vehicle.home_location
        send_telemetry(vehicle)
        time.sleep(1)

    current = vehicle.location.global_frame
    if current and current.lat is not None and current.lon is not None:
        log("Home location not ready, fallback to current global position")
        return current

    return None


def validate_mission(mission):
    if not isinstance(mission, dict):
        return False, "mission_not_dict"

    if "id" not in mission:
        return False, "mission_missing_id"

    if "order_id" not in mission:
        return False, "mission_missing_order_id"

    if "waypoints" not in mission:
        return False, "mission_missing_waypoints"

    wps = mission.get("waypoints")
    if not isinstance(wps, list) or len(wps) == 0:
        return False, "mission_empty_waypoints"

    wp = wps[0]
    if "lat" not in wp or "lng" not in wp or "alt_m" not in wp:
        return False, "waypoint_missing_fields"

    return True, None


# =========================================================
# TELEMETRY / EVENT
# =========================================================

def send_telemetry(vehicle):
    try:
        pos = vehicle.location.global_relative_frame
        if pos is None or pos.lat is None or pos.lon is None:
            return

        payload = {
            "drone_id": DRONE_ID,
            "lat": pos.lat,
            "lng": pos.lon,
            "alt_m": pos.alt if pos.alt is not None else 0.0,
            "groundspeed_mps": float(vehicle.groundspeed or 0.0),
            "battery_percent": None,
            "mode": vehicle.mode.name if vehicle.mode else None,
            "armed": bool(vehicle.armed),
            "ts_ms": now_ms()
        }

        http.post(f"{BACKEND}/bridge/telemetry", json=payload, timeout=TELEMETRY_TIMEOUT)
    except Exception:
        pass


def send_event(mission_id, event_type, detail=None):
    payload = {
        "drone_id": DRONE_ID,
        "mission_id": mission_id,
        "type": event_type,
        "detail": detail,
        "ts_ms": now_ms()
    }

    try:
        http.post(f"{BACKEND}/bridge/event", json=payload, timeout=EVENT_TIMEOUT)
        log(f"EVENT -> {event_type} ({mission_id})")
    except Exception as e:
        log(f"EVENT send failed: {event_type}, err={e}")


# =========================================================
# TAKEOFF
# =========================================================

def wait_alt(vehicle, target_alt, timeout=60):
    start = time.time()

    while True:
        current = vehicle.location.global_relative_frame.alt
        log(f"ALT: {current}")

        if current is not None and current >= target_alt * 0.95:
            return True

        if time.time() - start > timeout:
            log("wait_alt timeout")
            return False

        send_telemetry(vehicle)
        time.sleep(1)


def arm_takeoff(vehicle, alt):
    log("Waiting armable...")
    while not vehicle.is_armable:
        send_telemetry(vehicle)
        time.sleep(1)

    log("Switch GUIDED...")
    vehicle.mode = VehicleMode("GUIDED")
    while vehicle.mode.name != "GUIDED":
        send_telemetry(vehicle)
        time.sleep(0.5)

    log("Arming...")
    vehicle.armed = True
    while not vehicle.armed:
        send_telemetry(vehicle)
        time.sleep(0.5)

    log(f"Takeoff to {alt:.2f} m")
    vehicle.simple_takeoff(alt)

    ok = wait_alt(vehicle, alt)
    if not ok:
        raise Exception("takeoff_alt_timeout")

    log("Altitude reached")


# =========================================================
# GOTO
# =========================================================

def goto(vehicle, lat, lon, alt, base_speed=None, timeout=GOTO_TIMEOUT, max_speed=MAX_CRUISE_SPEED):
    wp = LocationGlobalRelative(lat, lon, alt)

    current = vehicle.location.global_relative_frame
    initial_dist = get_distance(current, wp)

    if base_speed is None:
        cruise_speed = compute_cruise_speed_by_distance(initial_dist)
    else:
        cruise_speed = base_speed

    cruise_speed = clamp_speed(cruise_speed, max_speed)

    log(
        f"GOTO lat={lat:.6f}, lon={lon:.6f}, alt={alt:.2f}, "
        f"dist={initial_dist:.2f}m, cruise={cruise_speed:.2f}m/s"
    )

    current_cmd_speed = set_vehicle_speed(vehicle, cruise_speed, max_speed=max_speed)
    vehicle.simple_goto(wp)

    start = time.time()
    last_print = 0.0

    while True:
        now = time.time()

        current = vehicle.location.global_relative_frame
        dist = get_distance(current, wp)
        alt_now = current.alt if current.alt is not None else 0.0
        alt_err = abs(alt_now - alt)
        actual_speed = float(vehicle.groundspeed or 0.0)

        target_speed = compute_approach_speed(dist, cruise_speed)
        target_speed = clamp_speed(target_speed, max_speed)

        if abs(target_speed - current_cmd_speed) >= 0.2:
            current_cmd_speed = set_vehicle_speed(vehicle, target_speed, max_speed=max_speed)
            log(f"Speed update -> target_speed={current_cmd_speed:.2f} m/s")

        if now - last_print >= GOTO_LOG_PERIOD:
            log(
                f"GOTO dist={dist:.2f}m alt={alt_now:.2f}m alt_err={alt_err:.2f}m "
                f"target_speed={current_cmd_speed:.2f}m/s actual_speed={actual_speed:.2f}m/s"
            )
            last_print = now

        if dist <= WAYPOINT_RADIUS and alt_err <= ALT_TOLERANCE:
            log("Waypoint reached")
            return True

        if now - start > timeout:
            log("Waypoint timeout")
            return False

        send_telemetry(vehicle)
        time.sleep(0.3)


# =========================================================
# SERVO DROP
# =========================================================

def payload_drop(vehicle):
    log("Drop payload")

    open_msg = vehicle.message_factory.command_long_encode(
        0, 0,
        mavutil.mavlink.MAV_CMD_DO_SET_SERVO,
        0,
        9,
        1900,
        0, 0, 0, 0, 0
    )
    vehicle.send_mavlink(open_msg)
    vehicle.flush()
    time.sleep(0.8)

    close_msg = vehicle.message_factory.command_long_encode(
        0, 0,
        mavutil.mavlink.MAV_CMD_DO_SET_SERVO,
        0,
        9,
        1100,
        0, 0, 0, 0, 0
    )
    vehicle.send_mavlink(close_msg)
    vehicle.flush()


# =========================================================
# DELIVERY
# =========================================================

def deliver(vehicle, wp, cruise_alt, mission_id):
    lat = float(wp["lat"])
    lon = float(wp["lng"])

    log("Fly to delivery point")
    ok = goto(vehicle, lat, lon, cruise_alt)
    if not ok:
        raise Exception("goto_delivery_timeout")

    send_event(mission_id, "ARRIVED")

    log("Descend for delivery")
    ok = goto(
        vehicle,
        lat,
        lon,
        DELIVERY_ALT,
        base_speed=DESCEND_SPEED,
        timeout=90,
        max_speed=DESCEND_SPEED
    )
    if not ok:
        raise Exception("descend_delivery_timeout")

    log("Brief stabilize")
    time.sleep(DELIVERY_HOLD_SEC)

    log("Drop payload now")
    payload_drop(vehicle)
    send_event(mission_id, "DELIVERED")

    log("Climb back to cruise altitude")
    ok = goto(
        vehicle,
        lat,
        lon,
        cruise_alt,
        base_speed=2.5,
        timeout=90,
        max_speed=3.0
    )
    if not ok:
        raise Exception("climb_after_drop_timeout")


# =========================================================
# RETURN HOME
# =========================================================

def return_home(vehicle, cruise_alt, mission_id):
    home = ensure_home(vehicle, wait_sec=20)
    if home is None:
        raise Exception("home_location_unavailable")

    log("Return home at cruise altitude")
    ok = goto(
        vehicle,
        home.lat,
        home.lon,
        cruise_alt,
        base_speed=RETURN_MAX_SPEED,
        timeout=GOTO_TIMEOUT,
        max_speed=RETURN_MAX_SPEED
    )
    if not ok:
        raise Exception("return_home_timeout")

    log("Descend near home by GPS")
    ok = goto(
        vehicle,
        home.lat,
        home.lon,
        HOME_LAND_ALT,
        base_speed=1.8,
        timeout=120,
        max_speed=2.2
    )
    if not ok:
        raise Exception("home_descend_timeout")

    log("Switch to LAND")
    vehicle.mode = VehicleMode("LAND")
    while vehicle.mode.name != "LAND":
        send_telemetry(vehicle)
        time.sleep(0.5)

    log("Waiting disarm...")
    while vehicle.armed:
        send_telemetry(vehicle)
        time.sleep(1)

    log("Landed and disarmed")
    send_event(mission_id, "COMPLETED")


# =========================================================
# FAILSAFE / RECOVERY
# =========================================================

def safe_land(vehicle):
    try:
        log("SAFE LAND triggered")
        vehicle.mode = VehicleMode("LAND")
        while vehicle.mode.name != "LAND":
            send_telemetry(vehicle)
            time.sleep(0.5)
    except Exception as e:
        log(f"safe_land error: {e}")


# =========================================================
# MISSION EXECUTION
# =========================================================

def execute_mission(vehicle, mission):
    ok, reason = validate_mission(mission)
    if not ok:
        raise Exception(reason)

    wps = mission["waypoints"]
    cruise_alt = max(10.0, float(wps[0]["alt_m"]))
    log(f"Mission {mission['id']} | order={mission['order_id']} | cruise_alt={cruise_alt:.2f}m")

    arm_takeoff(vehicle, cruise_alt)

    for i, wp in enumerate(wps, start=1):
        log(f"Delivery point {i}/{len(wps)}")
        deliver(vehicle, wp, cruise_alt, mission["id"])

    return_home(vehicle, cruise_alt, mission["id"])


# =========================================================
# MISSION POLL
# =========================================================

def fetch_next_mission():
    try:
        r = http.get(
            f"{BACKEND}/bridge/missions/next",
            params={"drone_id": DRONE_ID},
            timeout=REQUEST_TIMEOUT
        )

        if r.status_code != 200:
            log(f"Mission fetch bad status: {r.status_code}")
            return None

        data = r.json()
        if not isinstance(data, dict):
            return None

        mission = data.get("mission")
        return mission
    except Exception as e:
        log(f"Mission fetch error: {e}")
        return None


def mission_loop(vehicle):
    while True:
        mission = fetch_next_mission()

        if mission:
            mid = mission.get("id")
            log(f"NEW MISSION: {mid}")

            try:
                execute_mission(vehicle, mission)
                log(f"MISSION DONE: {mid}")
            except Exception as e:
                log(f"Mission execution error: {e}")
                send_event(mid, "FAILED", detail=str(e))
                safe_land(vehicle)

        send_telemetry(vehicle)
        time.sleep(MISSION_POLL_SEC)


# =========================================================
# MAIN
# =========================================================

def setup_vehicle_params(vehicle):
    log("Setting flight parameters...")

    params = {
        "WP_SPD": 1000,
        "WP_ACC": 700,
        "WP_ACC_Z": 250,
        "WP_SPD_UP": 350,
        "WP_SPD_DN": 250,
        "WP_RADIUS_M": 100
    }

    for k, v in params.items():
        try:
            vehicle.parameters[k] = v
            time.sleep(0.5)
            log(f"Set {k} = {vehicle.parameters[k]}")
        except Exception as e:
            log(f"Failed to set {k}: {e}")


def main():
    log("Connecting vehicle...")
    vehicle = connect(CONNECTION, wait_ready=True)
    log("Connected")

    time.sleep(2)
    vehicle.wait_ready("parameters")

    setup_vehicle_params(vehicle)

    try:
        ensure_home(vehicle, wait_sec=20)
    except Exception as e:
        log(f"Home init warning: {e}")

    if vehicle.mode.name == "RTL":
        log("Failsafe triggered: vehicle currently in RTL")

    log("Start mission polling...")
    mission_loop(vehicle)


if __name__ == "__main__":
    main()