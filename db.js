const { Pool } = require('pg');
const dns = require('dns').promises;
require('dotenv').config();

let pool;

(async () => {
  try {
    // Resolve IPv4 address from Supabase host
    const { address } = await dns.lookup(process.env.PG_HOST, { family: 4 });

    pool = new Pool({
      user: process.env.PG_USER,
      password: process.env.PG_PASSWORD,
      host: address, // Use IPv4 directly
      port: process.env.PG_PORT,
      database: process.env.PG_DATABASE,
      ssl: {
        rejectUnauthorized: false,
      },
    });

    module.exports.pool = pool;
  } catch (error) {
    console.error('[DB INIT ERROR]', error);
    process.exit(1); // Crash the app if DB init fails
  }
})();
