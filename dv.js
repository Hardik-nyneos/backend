const { Client } = require("pg");

require("dotenv").config();
const db = new Client({
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  ssl: {
    rejectUnauthorized: false,
  },
});

async function seedAdmin() {
  try {
    await db.connect();
    console.log("✅ Connected to database");
    // Add approved_at column to exposure_headers if not exists
    await db.query(`
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM information_schema.columns 
          WHERE table_name='exposure_headers' AND column_name='rejected_at'
        ) THEN
          ALTER TABLE exposure_headers ADD COLUMN rejected_at TIMESTAMP;
        END IF;
      END$$;
    `);
    console.log("Checked/added column: approved_at in exposure_headers");
    // Fetch and print exposure_header_id where exposure_type is PO or SO
    const res = await db.query(`
      SELECT exposure_header_id, exposure_type, document_id
      FROM exposure_headers
      WHERE exposure_type = 'PO' OR exposure_type = 'SO';
    `);
    console.log("\nExposure Headers (PO or SO):");
    console.table(res.rows);
    await db.end();
  } catch (err) {
    console.error("❌ Error:", err.stack);
    await db.end();
  }
}

seedAdmin();
