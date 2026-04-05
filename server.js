require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const pool = require('./db'); // Import DB từ file db.js
const crypto = require('crypto');
const app = express();
const server = http.createServer(app);

// Cấu hình Socket.io cho phép mọi frontend kết nối
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// Middleware xử lý JSON và CORS
app.use(cors());
app.use(express.json());

// Băm mật khẩu bằng SHA256 y hệt Python
function hashPassword(password) {
  return crypto.createHash('sha256').update(password, 'utf8').digest('hex');
}

// Tạo thời gian (ms)
function nowMs() {
  return Date.now();
}

// Middleware: Kiểm tra đăng nhập (tương đương get_current_user / require_user bên Python)
async function requireUser(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  const token = authHeader.replace('Bearer ', '').trim();
  try {
    // Tìm session
    const sessionRes = await pool.query('SELECT username FROM sessions WHERE token = $1', [token]);
    if (sessionRes.rows.length === 0) return res.status(401).json({ error: 'unauthorized' });

    // Tìm user
    const userRes = await pool.query('SELECT * FROM users WHERE username = $1', [sessionRes.rows[0].username]);
    if (userRes.rows.length === 0) return res.status(401).json({ error: 'unauthorized' });

    req.user = userRes.rows[0]; // Lưu thông tin user vào req để các API sau xài
    req.token = token;
    next();
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

// Middleware: Kiểm tra quyền Admin (tương đương require_admin)
async function requireAdmin(req, res, next) {
  await requireUser(req, res, () => {
    if (req.user.role !== 'admin') {
      return res.status(403).json({ error: 'forbidden' });
    }
    next();
  });
}

// Hàm format lại user để trả về frontend (ẩn password)
function publicUser(user) {
  return {
    id: user.id,
    username: user.username,
    role: user.role,
    full_name: user.full_name || '',
    email: user.email || '',
    phone: user.phone || '',
    address: user.address || '',
    created_ts: parseInt(user.created_ts)
  };
}

// ==========================================
// AUTH API
// ==========================================

// 1. Đăng ký
app.post('/auth/register', async (req, res) => {
  const username = (req.body.username || '').trim();
  const password = req.body.password || '';

  if (username.length < 3) return res.status(400).json({ error: 'username_too_short' });
  if (password.length < 4) return res.status(400).json({ error: 'password_too_short' });

  try {
    const checkUser = await pool.query('SELECT username FROM users WHERE username = $1', [username]);
    if (checkUser.rows.length > 0) return res.status(400).json({ error: 'username_exists' });

    const uid = `user_${username}`;
    await pool.query(
      `INSERT INTO users (id, username, password_hash, role, full_name, email, phone, address, created_ts) 
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [uid, username, hashPassword(password), 'user', '', '', '', '', nowMs()]
    );

    const newUser = await pool.query('SELECT * FROM users WHERE username = $1', [username]);
    res.json({ ok: true, user: publicUser(newUser.rows[0]) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 2. Đăng nhập
app.post('/auth/login', async (req, res) => {
  const username = (req.body.username || '').trim();
  const password = req.body.password || '';

  try {
    const userRes = await pool.query('SELECT * FROM users WHERE username = $1', [username]);
    const user = userRes.rows[0];

    if (!user || user.password_hash !== hashPassword(password)) {
      return res.status(400).json({ error: 'invalid_credentials' });
    }

    // Tạo token hex 32 byte giống hàm secrets.token_hex(32) của Python
    const token = crypto.randomBytes(32).toString('hex');

    await pool.query(
      `INSERT INTO sessions (token, username, created_ts) VALUES ($1, $2, $3)
       ON CONFLICT (token) DO UPDATE SET username = EXCLUDED.username, created_ts = EXCLUDED.created_ts`,
      [token, username, nowMs()]
    );

    res.json({ ok: true, token: token, user: publicUser(user) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 3. Lấy thông tin user hiện tại
app.get('/auth/me', requireUser, (req, res) => {
  res.json({ ok: true, user: publicUser(req.user) });
});

// 4. Đăng xuất
app.post('/auth/logout', requireUser, async (req, res) => {
  try {
    await pool.query('DELETE FROM sessions WHERE token = $1', [req.token]);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 5. Đổi mật khẩu
app.put('/auth/change-password', requireUser, async (req, res) => {
  const { old_password, new_password, confirm_new_password } = req.body;

  if (hashPassword(old_password) !== req.user.password_hash) return res.status(400).json({ error: 'wrong_old_password' });
  if (new_password.length < 4) return res.status(400).json({ error: 'password_too_short' });
  if (new_password !== confirm_new_password) return res.status(400).json({ error: 'confirm_password_not_match' });
  if (old_password === new_password) return res.status(400).json({ error: 'same_as_old_password' });

  try {
    await pool.query('UPDATE users SET password_hash = $1 WHERE username = $2', [hashPassword(new_password), req.user.username]);
    res.json({ ok: true, message: 'password_changed' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// USER PROFILE API
// ==========================================
app.put('/users/me', requireUser, async (req, res) => {
  const { full_name, email, phone, address } = req.body;
  try {
    await pool.query(
      `UPDATE users SET full_name = $1, email = $2, phone = $3, address = $4 WHERE username = $5`,
      [(full_name || '').trim(), (email || '').trim(), (phone || '').trim(), (address || '').trim(), req.user.username]
    );
    const updatedUser = await pool.query('SELECT * FROM users WHERE username = $1', [req.user.username]);
    res.json({ ok: true, user: publicUser(updatedUser.rows[0]) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/users', requireAdmin, async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM users ORDER BY created_ts DESC');
    res.json(result.rows.map(publicUser));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// KÊNH REALTIME (SOCKET.IO)
// ==========================================
io.on('connection', (socket) => {
  console.log(`⚡ Drone/Frontend đã kết nối. ID: ${socket.id}`);

  // Khi Python trên Drone bắn Telemetry lên
  socket.on('bridge_telemetry', async (data) => {
    const status = data.armed ? 'BUSY' : 'IDLE'; 
    
    // Broadcast KHÔNG ĐỘ TRỄ xuống mọi trình duyệt đang mở Console
    io.emit('frontend_telemetry', {
      ...data,
      status: status,
      age_ms: age_ms 
    });

    // Lưu ngầm xuống Database (Không bắt luồng realtime phải chờ)
    try {
      await pool.query(
        `UPDATE drones SET status = $1, pi_last_json = $2, pi_last_seen = $3 WHERE id = $4`,
        [status, JSON.stringify(data), Date.now(), data.drone_id]
      );
    } catch (err) {
      console.error('Lỗi update db telemetry:', err.message);
    }
  });

  // Lắng nghe sự kiện (Event) từ Drone (Đã đến nơi, Đã thả hàng...)
  socket.on('bridge_event', async (eventData) => {
    
    // 1. TỰ ĐỘNG TÌM MISSION_ID NẾU BỊ TRỐNG (Dành cho Ground Relay)
    if (!eventData.mission_id && eventData.drone_id) {
      try {
        const activeMissionRes = await pool.query(
          `SELECT id FROM missions WHERE drone_id = $1 AND UPPER(status) NOT IN ('DONE', 'FAILED', 'CANCELLED') ORDER BY created_ts DESC LIMIT 1`,
          [eventData.drone_id]
        );
        
        if (activeMissionRes.rows.length > 0) {
          eventData.mission_id = activeMissionRes.rows[0].id; // Tự động điền ID cho relay
          console.log(`[Relay Auto-Map] Đã tự động map event cho mission: ${eventData.mission_id}`);
        } else {
          console.log(`[Relay Warning] Nhận event ${eventData.type} từ ${eventData.drone_id} nhưng không có mission nào đang chạy.`);
          return; // Không có mission thì ngưng xử lý
        }
      } catch (err) {
        console.error('[Relay Error] Lỗi truy vấn mission tự động:', err.message);
        return;
      }
    }

    console.log(`🚁 Drone Event: ${eventData.type} - Mission: ${eventData.mission_id}`);
    
    // 2. Bắn xuống Frontend (Lưu ý: Bắn sau khi đã có mission_id để Web không bị lỗi)
    io.emit('frontend_event', eventData); 

    // 3. Cập nhật Database ngầm
    try {
      if (eventData.type === 'ARRIVED') {
        await pool.query(`UPDATE orders SET status = 'ARRIVED' FROM missions WHERE orders.id = missions.order_id AND missions.id = $1`, [eventData.mission_id]);
      } else if (eventData.type === 'DELIVERED') {
        await pool.query(`UPDATE orders SET status = 'DELIVERED' FROM missions WHERE orders.id = missions.order_id AND missions.id = $1`, [eventData.mission_id]);
      } else if (eventData.type === 'COMPLETED') {
        await pool.query(`UPDATE orders SET status = 'COMPLETED' FROM missions WHERE orders.id = missions.order_id AND missions.id = $1`, [eventData.mission_id]);
        await pool.query(`UPDATE missions SET status = 'DONE' WHERE id = $1`, [eventData.mission_id]);
      } else if (eventData.type === 'FAILED') {
        await pool.query(`UPDATE orders SET status = 'FAILED' FROM missions WHERE orders.id = missions.order_id AND missions.id = $1`, [eventData.mission_id]);
        await pool.query(`UPDATE missions SET status = 'FAILED' WHERE id = $1`, [eventData.mission_id]);
      }
    } catch (err) { 
      console.error('Lỗi lưu event:', err.message); 
    }
  });
  
  socket.on('disconnect', () => {
    console.log(`Kết nối bị ngắt. ID: ${socket.id}`);
  });
});

// ==========================================
// HELPERS CHO ORDERS & MISSIONS
// ==========================================

// Parse dữ liệu từ CSDL (chuyển JSON Text thành Object)
function rowToOrder(row) {
  if (!row) return null;
  return {
    id: row.id,
    created_by: row.created_by,
    dropoff: JSON.parse(row.dropoff_json),
    note: row.note,
    status: row.status,
    created_ts: parseInt(row.created_ts)
  };
}

function rowToMission(row) {
  if (!row) return null;
  return {
    id: row.id,
    order_id: row.order_id,
    drone_id: row.drone_id,
    status: row.status,
    altitude_m: row.altitude_m,
    warehouse_lat: row.warehouse_lat,
    warehouse_lng: row.warehouse_lng,
    waypoints: JSON.parse(row.waypoints_json),
    created_by: row.created_by,
    created_ts: parseInt(row.created_ts)
  };
}

// Hàm tự động tạo ID đơn hàng tiếp theo (y hệt get_next_order_id của Python)
async function getNextOrderId() {
  const res = await pool.query(
    `SELECT COALESCE(MAX(CAST(SUBSTRING(id FROM 7) AS INTEGER)), 0) AS max_num FROM orders WHERE id ~ '^order_[0-9]+$'`
  );
  const maxNum = parseInt(res.rows[0].max_num || 0);
  return `order_${maxNum + 1}`;
}

function getNextMissionId() {
  return `mission_${nowMs()}`;
}

// Lấy mission đang chạy của một đơn hàng
async function getActiveMissionByOrderId(orderId) {
  const res = await pool.query(
    `SELECT * FROM missions WHERE order_id = $1 AND UPPER(status) NOT IN ('DONE', 'FAILED', 'CANCELLED') ORDER BY created_ts DESC LIMIT 1`,
    [orderId]
  );
  return rowToMission(res.rows[0]);
}


// ==========================================
// ORDERS API (ĐƠN HÀNG)
// ==========================================

// 1. Tạo đơn hàng mới
app.post('/orders', requireUser, async (req, res) => {
  const { dropoff, note } = req.body;
  try {
    const oid = await getNextOrderId();
    const status = 'CREATED';
    const created_ts = nowMs();

    await pool.query(
      `INSERT INTO orders (id, created_by, dropoff_json, note, status, created_ts) VALUES ($1, $2, $3, $4, $5, $6)`,
      [oid, req.user.username, JSON.stringify(dropoff), note, status, created_ts]
    );

    const newOrder = { id: oid, created_by: req.user.username, dropoff, note, status, created_ts };
    
    // MA THUẬT REALTIME: Bắn thông báo cho tất cả Admin đang mở Console
    io.emit('frontend_event', { type: 'NEW_ORDER', detail: `Đơn mới: ${oid}`, ts_ms: created_ts });

    res.json(newOrder);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 2. Lấy danh sách đơn hàng
app.get('/orders', requireUser, async (req, res) => {
  try {
    let result;
    if (req.user.role === 'admin') {
      result = await pool.query('SELECT * FROM orders ORDER BY created_ts DESC');
    } else {
      result = await pool.query('SELECT * FROM orders WHERE created_by = $1 ORDER BY created_ts DESC', [req.user.username]);
    }
    res.json(result.rows.map(rowToOrder));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 3. Hủy đơn hàng
app.post('/orders/:order_id/cancel', requireUser, async (req, res) => {
  const orderId = req.params.order_id;
  try {
    const orderRes = await pool.query('SELECT * FROM orders WHERE id = $1', [orderId]);
    if (orderRes.rows.length === 0) return res.status(404).json({ error: 'order_not_found' });
    const order = rowToOrder(orderRes.rows[0]);

    const isAdmin = req.user.role === 'admin';
    const isOwner = order.created_by === req.user.username;

    if (!isAdmin && !isOwner) return res.status(403).json({ error: 'forbidden' });

    const blockedStatuses = ['DELIVERED', 'COMPLETED', 'DONE', 'CANCELLED'];
    if (blockedStatuses.includes(order.status.toUpperCase())) {
      return res.status(400).json({ error: 'cannot_cancel_order' });
    }

    // Cập nhật trạng thái Order
    await pool.query(`UPDATE orders SET status = 'CANCELLED' WHERE id = $1`, [orderId]);

    // Nếu có Mission đang dở dang thì Cancel luôn Mission
    const mission = await getActiveMissionByOrderId(orderId);
    if (mission && !['DONE', 'FAILED', 'CANCELLED'].includes(mission.status.toUpperCase())) {
      await pool.query(`UPDATE missions SET status = 'CANCELLED' WHERE id = $1`, [mission.id]);
    }

    res.json({ ok: true, order_id: orderId, status: 'CANCELLED' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 4. Admin Dispatch (Tiến hành giao)
app.post('/orders/:order_id/dispatch', requireAdmin, async (req, res) => {
  const orderId = req.params.order_id;
  try {
    const orderRes = await pool.query('SELECT * FROM orders WHERE id = $1', [orderId]);
    if (orderRes.rows.length === 0) return res.status(404).json({ error: 'order_not_found' });
    const order = rowToOrder(orderRes.rows[0]);

    const statusUpper = order.status.toUpperCase();
    if (statusUpper === 'CANCELLED') return res.status(400).json({ error: 'order_cancelled' });
    if (['DELIVERED', 'COMPLETED', 'DONE'].includes(statusUpper)) return res.status(400).json({ error: 'order_completed' });

    const existingMission = await getActiveMissionByOrderId(orderId);
    if (existingMission) return res.status(400).json({ error: 'mission_already_exists' });

    const mid = getNextMissionId();
    const waypoints = [{ lat: order.dropoff.lat, lng: order.dropoff.lng }];
    const missionStatus = 'START_REQUESTED';
    const ts = nowMs();

    // Tạo Mission mới
    await pool.query(
      `INSERT INTO missions (id, order_id, drone_id, status, altitude_m, warehouse_lat, warehouse_lng, waypoints_json, created_by, created_ts) 
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
      [mid, orderId, 'drone_1', missionStatus, 0.0, 0.0, 0.0, JSON.stringify(waypoints), req.user.username, ts]
    );

    // Cập nhật Order thành ASSIGNED
    await pool.query(`UPDATE orders SET status = 'ASSIGNED' WHERE id = $1`, [orderId]);

    const missionObj = {
      id: mid, order_id: orderId, drone_id: 'drone_1', status: missionStatus,
      altitude_m: 0.0, warehouse_lat: 0.0, warehouse_lng: 0.0, waypoints,
      created_by: req.user.username, created_ts: ts
    };

    res.json({ ok: true, mission: missionObj });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// ==========================================
// MISSIONS API (ĐIỀU PHỐI BAY)
// ==========================================

// 1. Tạo Mission thủ công
app.post('/missions', requireAdmin, async (req, res) => {
  const { order_id, drone_id } = req.body;
  try {
    const orderRes = await pool.query('SELECT * FROM orders WHERE id = $1', [order_id]);
    if (orderRes.rows.length === 0) return res.status(404).json({ error: 'order_not_found' });
    const order = rowToOrder(orderRes.rows[0]);

    if (order.status.toUpperCase() === 'CANCELLED') return res.status(400).json({ error: 'order_cancelled' });

    const existingMission = await getActiveMissionByOrderId(order_id);
    if (existingMission) return res.status(400).json({ error: 'mission_already_exists' });

    const mid = getNextMissionId();
    const waypoints = [{ lat: order.dropoff.lat, lng: order.dropoff.lng }];
    const ts = nowMs();

    await pool.query(
      `INSERT INTO missions (id, order_id, drone_id, status, altitude_m, warehouse_lat, warehouse_lng, waypoints_json, created_by, created_ts) 
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
      [mid, order_id, drone_id, 'ASSIGNED', 0.0, 0.0, 0.0, JSON.stringify(waypoints), req.user.username, ts]
    );

    await pool.query(`UPDATE orders SET status = 'QUEUED' WHERE id = $1`, [order_id]);

    const missionObj = {
      id: mid, order_id, drone_id, status: 'ASSIGNED',
      altitude_m: 0.0, warehouse_lat: 0.0, warehouse_lng: 0.0, waypoints,
      created_by: req.user.username, created_ts: ts
    };
    res.json(missionObj);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 2. Bắt đầu Mission
app.post('/missions/:mission_id/start', requireAdmin, async (req, res) => {
  const missionId = req.params.mission_id;
  try {
    const missionRes = await pool.query('SELECT * FROM missions WHERE id = $1', [missionId]);
    if (missionRes.rows.length === 0) return res.status(404).json({ error: 'mission_not_found' });
    const mission = rowToMission(missionRes.rows[0]);

    if (mission.status.toUpperCase() === 'CANCELLED') return res.status(400).json({ error: 'mission_cancelled' });

    await pool.query(`UPDATE missions SET status = 'START_REQUESTED' WHERE id = $1`, [missionId]);
    await pool.query(`UPDATE orders SET status = 'ASSIGNED' WHERE id = $1`, [mission.order_id]);

    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 3. Lấy danh sách Mission
app.get('/missions', requireUser, async (req, res) => {
  try {
    let result;
    if (req.user.role === 'admin') {
      result = await pool.query('SELECT * FROM missions ORDER BY created_ts DESC');
    } else {
      result = await pool.query(
        `SELECT m.* FROM missions m JOIN orders o ON m.order_id = o.id WHERE o.created_by = $1 ORDER BY m.created_ts DESC`,
        [req.user.username]
      );
    }
    res.json(result.rows.map(rowToMission));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// BRIDGE & DRONE API (GIAO TIẾP VỚI DRONE)
// ==========================================

// 1. Drone hỏi xin Mission tiếp theo
app.get('/bridge/missions/next', async (req, res) => {
  const droneId = req.query.drone_id;
  try {
    const result = await pool.query(
      `SELECT * FROM missions WHERE drone_id = $1 AND status = 'START_REQUESTED' ORDER BY created_ts ASC LIMIT 1`,
      [droneId]
    );

    if (result.rows.length === 0) return res.json({ mission: null });

    const mission = rowToMission(result.rows[0]);

    // Drone bắt đầu bay -> Cập nhật trạng thái
    await pool.query(`UPDATE missions SET status = 'RUNNING' WHERE id = $1`, [mission.id]);
    await pool.query(`UPDATE orders SET status = 'IN_FLIGHT' WHERE id = $1`, [mission.order_id]);

    const updatedMission = await pool.query(`SELECT * FROM missions WHERE id = $1`, [mission.id]);
    res.json({ mission: rowToMission(updatedMission.rows[0]) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 2. Lấy thông tin Drone hiện tại (Dành cho Frontend load lần đầu)
app.get('/drones/:drone_id', requireUser, async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM drones WHERE id = $1', [req.params.drone_id]);
    if (result.rows.length === 0) return res.status(404).json({ error: 'drone_not_found' });

    const row = result.rows[0];
    const lastData = row.pi_last_json ? JSON.parse(row.pi_last_json) : null;
    
    res.json({
      id: row.id,
      name: row.name,
      status: row.status,
      last: lastData,
      telemetry_source: 'pi_bridge',
      telemetry_age_ms: row.pi_last_seen ? Date.now() - parseInt(row.pi_last_seen) : null
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ==========================================
// KHỞI ĐỘNG SERVER
// ==========================================
const PORT = process.env.PORT || 8000;
server.listen(PORT, () => {
  console.log(`🚀 JANUS Backend đang chạy tại: http://localhost:${PORT}`);
});