const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false // Bắt buộc phải có khi xài DB trên Cloud (Render/Supabase)
  }
});

// Test thử kết nối
pool.connect((err, client, release) => {
  if (err) {
    return console.error('Lỗi kết nối Database:', err.stack);
  }
  console.log('✅ Đã kết nối thành công tới PostgreSQL Janus');
  release();
});

module.exports = pool;