import time
import math
import requests

from dronekit import connect, VehicleMode, LocationGlobalRelative
from pymavlink import mavutil


BACKEND = "http://127.0.0.1:8080"
DRONE_ID = "drone_1"
CONNECTION = "127.0.0.1:14552"

# ==============================
# FLIGHT TUNING
# ==============================
MIN_CRUISE_SPEED = 3.0       # m/s
MAX_CRUISE_SPEED = 10.0      # m/s
APPROACH_MIN_SPEED = 1.8     # m/s, chỉ dùng khi còn <= 3m
DESCEND_SPEED = 1.8          # m/s
RETURN_MAX_SPEED = 8.0       # m/s

WAYPOINT_RADIUS = 1.0        # m
ALT_TOLERANCE = 0.8          # m
DELIVERY_ALT = 6.0           # m
HOME_LAND_ALT = 4.0          # m
DELIVERY_HOLD_SEC = 0.3      # s
GOTO_TIMEOUT = 180           # s

APPROACH_BRAKE_DISTANCE = 3.0   # m, chỉ lúc này mới giảm tốc
GOTO_LOG_PERIOD = 1.0           # s


# =========================================================
# UTIL
# =========================================================

def now_ms():
    return int(time.time() * 1000)


def get_distance(a, b):
    """
    Tính khoảng cách ngang giữa 2 điểm GPS, đơn vị mét.
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


def wait_alt(vehicle, alt):
    while True:
        current = vehicle.location.global_relative_frame.alt
        print(f"ALT: {current}")

        if current is not None and current >= alt * 0.95:
            break

        send_telemetry(vehicle)
        time.sleep(1)


def compute_cruise_speed_by_distance(distance_m):
    """
    Xa thì bay nhanh hơn.
    Chú ý: quãng đường ngắn thì dù set cao, drone vẫn khó đạt max thật.
    """
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
    """
    Chỉ giảm tốc khi còn rất gần mục tiêu.
    """
    if remaining_dist <= APPROACH_BRAKE_DISTANCE:
        return APPROACH_MIN_SPEED
    return cruise_speed


def clamp_speed(speed_mps, max_speed):
    return max(0.8, min(speed_mps, max_speed))


def set_vehicle_speed(vehicle, speed_mps, max_speed=MAX_CRUISE_SPEED):
    """
    Chỉ set groundspeed, không set param liên tục trong loop.
    """
    speed_mps = clamp_speed(speed_mps, max_speed)
    vehicle.groundspeed = speed_mps
    return speed_mps


# =========================================================
# TELEMETRY / EVENT
# =========================================================

def send_telemetry(vehicle):
    pos = vehicle.location.global_relative_frame

    payload = {
        "drone_id": DRONE_ID,
        "lat": pos.lat,
        "lng": pos.lon,
        "alt_m": pos.alt,
        "groundspeed_mps": float(vehicle.groundspeed or 0.0),
        "battery_percent": None,
        "mode": vehicle.mode.name if vehicle.mode else None,
        "armed": vehicle.armed,
        "ts_ms": now_ms()
    }

    try:
        requests.post(f"{BACKEND}/bridge/telemetry", json=payload, timeout=1)
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
        requests.post(f"{BACKEND}/bridge/event", json=payload, timeout=2)
    except Exception:
        pass


# =========================================================
# TAKEOFF
# =========================================================

def arm_takeoff(vehicle, alt):
    print("Waiting armable...")
    while not vehicle.is_armable:
        send_telemetry(vehicle)
        time.sleep(1)

    print("Switch GUIDED...")
    vehicle.mode = VehicleMode("GUIDED")
    while vehicle.mode.name != "GUIDED":
        send_telemetry(vehicle)
        time.sleep(0.5)

    print("Arming...")
    vehicle.armed = True
    while not vehicle.armed:
        send_telemetry(vehicle)
        time.sleep(0.5)

    print(f"Takeoff to {alt} m")
    vehicle.simple_takeoff(alt)

    wait_alt(vehicle, alt)
    print("Altitude reached")


# =========================================================
# GOTO
# =========================================================

def goto(vehicle, lat, lon, alt, base_speed=None, timeout=GOTO_TIMEOUT, max_speed=MAX_CRUISE_SPEED):
    """
    Bay tới waypoint:
    - gửi simple_goto đúng 1 lần
    - giữ cruise speed tới khi còn <= APPROACH_BRAKE_DISTANCE
    - chỉ đổi groundspeed khi cần
    """
    wp = LocationGlobalRelative(lat, lon, alt)

    current = vehicle.location.global_relative_frame
    initial_dist = get_distance(current, wp)

    if base_speed is None:
        cruise_speed = compute_cruise_speed_by_distance(initial_dist)
    else:
        cruise_speed = base_speed

    cruise_speed = clamp_speed(cruise_speed, max_speed)

    print(
        f"GOTO lat={lat:.6f}, lon={lon:.6f}, alt={alt:.2f}, "
        f"dist={initial_dist:.2f}m, cruise={cruise_speed:.2f}m/s"
    )

    # set speed trước rồi mới goto
    current_cmd_speed = set_vehicle_speed(vehicle, cruise_speed, max_speed=max_speed)

    # Gửi 1 lần duy nhất
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
            print(f"Speed update -> target_speed={current_cmd_speed:.2f} m/s")

        if now - last_print >= GOTO_LOG_PERIOD:
            print(
                f"GOTO dist={dist:.2f}m alt={alt_now:.2f}m alt_err={alt_err:.2f}m "
                f"target_speed={current_cmd_speed:.2f}m/s actual_speed={actual_speed:.2f}m/s"
            )
            last_print = now

        if dist <= WAYPOINT_RADIUS and alt_err <= ALT_TOLERANCE:
            print("Waypoint reached")
            break

        if now - start > timeout:
            print("Waypoint timeout")
            break

        send_telemetry(vehicle)
        time.sleep(0.3)


# =========================================================
# SERVO DROP
# =========================================================

def payload_drop(vehicle):
    print("Drop payload")

    open_msg = vehicle.message_factory.command_long_encode(
        0, 0,
        mavutil.mavlink.MAV_CMD_DO_SET_SERVO,
        0,
        9,      # servo channel
        1900,   # pwm open
        0, 0, 0, 0, 0
    )
    vehicle.send_mavlink(open_msg)
    vehicle.flush()

    time.sleep(0.8)

    close_msg = vehicle.message_factory.command_long_encode(
        0, 0,
        mavutil.mavlink.MAV_CMD_DO_SET_SERVO,
        0,
        9,      # servo channel
        1100,   # pwm close
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

    print("Fly to delivery point")
    goto(vehicle, lat, lon, cruise_alt)

    send_event(mission_id, "ARRIVED")

    print("Descend for delivery")
    goto(
        vehicle,
        lat,
        lon,
        DELIVERY_ALT,
        base_speed=DESCEND_SPEED,
        timeout=90,
        max_speed=DESCEND_SPEED
    )

    print("Brief stabilize")
    time.sleep(DELIVERY_HOLD_SEC)

    print("Drop payload now")
    payload_drop(vehicle)
    send_event(mission_id, "DELIVERED")

    print("Climb back to cruise altitude")
    goto(
        vehicle,
        lat,
        lon,
        cruise_alt,
        base_speed=2.5,
        timeout=90,
        max_speed=3.0
    )


# =========================================================
# RETURN HOME
# =========================================================

def return_home(vehicle, cruise_alt, mission_id):
    home = vehicle.home_location

    if home is None:
        print("Waiting home location...")
        while home is None:
            home = vehicle.home_location
            send_telemetry(vehicle)
            time.sleep(1)

    print("Return home at cruise altitude")
    goto(
        vehicle,
        home.lat,
        home.lon,
        cruise_alt,
        base_speed=RETURN_MAX_SPEED,
        timeout=GOTO_TIMEOUT,
        max_speed=RETURN_MAX_SPEED
    )

    print("Descend near home by GPS")
    goto(
        vehicle,
        home.lat,
        home.lon,
        HOME_LAND_ALT,
        base_speed=1.8,
        timeout=120,
        max_speed=2.2
    )

    print("Switch to LAND")
    vehicle.mode = VehicleMode("LAND")
    while vehicle.mode.name != "LAND":
        send_telemetry(vehicle)
        time.sleep(0.5)

    while vehicle.armed:
        send_telemetry(vehicle)
        time.sleep(1)

    print("Landed and disarmed")
    send_event(mission_id, "COMPLETED")


# =========================================================
# MISSION EXECUTION
# =========================================================

def execute_mission(vehicle, mission):
    wps = mission.get("waypoints", [])
    if not wps:
        print("Mission has no waypoints")
        return

    cruise_alt = max(10.0, float(wps[0]["alt_m"]))
    print(f"Cruise altitude: {cruise_alt:.2f} m")

    arm_takeoff(vehicle, cruise_alt)

    for i, wp in enumerate(wps, start=1):
        print(f"Delivery point {i}/{len(wps)}")
        deliver(vehicle, wp, cruise_alt, mission["id"])

    return_home(vehicle, cruise_alt, mission["id"])


# =========================================================
# MISSION POLL
# =========================================================

def mission_loop(vehicle):
    last_id = None

    while True:
        mission = None

        try:
            r = requests.get(
                f"{BACKEND}/bridge/missions/next",
                params={"drone_id": DRONE_ID},
                timeout=2
            )

            if r.status_code == 200:
                data = r.json()
                if isinstance(data, dict):
                    mission = data.get("mission")

        except Exception as e:
            print("Mission fetch error:", e)

        if mission and "id" in mission and "waypoints" in mission:
            mid = mission["id"]

            if mid != last_id:
                print("NEW MISSION:", mid)
                last_id = mid

                try:
                    execute_mission(vehicle, mission)
                except Exception as e:
                    print("Mission execution error:", e)
                    send_event(mid, "FAILED", detail=str(e))

        send_telemetry(vehicle)
        time.sleep(2)


# =========================================================
# MAIN
# =========================================================

print("Connecting vehicle...")
vehicle = connect(CONNECTION, wait_ready=True)

print("Connected")
time.sleep(2)

vehicle.wait_ready("parameters")

print("Setting flight parameters...")

params = {
    "WP_SPD": 1000,       # cm/s = 10 m/s
    "WP_ACC": 700,        # cm/s^2 = 7 m/s^2
    "WP_ACC_Z": 250,      # vertical accel
    "WP_SPD_UP": 350,     # cm/s = 3.5 m/s
    "WP_SPD_DN": 250,     # cm/s = 2.5 m/s
    "WP_RADIUS_M": 100        # cm = 1.0 m
}

for k, v in params.items():
    try:
        vehicle.parameters[k] = v
        time.sleep(0.5)
        print(f"Set {k} = {vehicle.parameters[k]}")
    except Exception as e:
        print(f"Failed to set {k}: {e}")

# chờ home ổn định
try:
    print("Waiting home location init...")
    for _ in range(20):
        if vehicle.home_location is not None:
            break
        send_telemetry(vehicle)
        time.sleep(1)
except Exception:
    pass

if vehicle.mode.name == "RTL":
    print("Failsafe triggered: vehicle currently in RTL")

print("Start mission polling...")
mission_loop(vehicle)