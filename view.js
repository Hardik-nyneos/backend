const { Client } = require("pg");

const db = new Client({
  user: "avnadmin",
  password: "AVNS_L6PcvF7OBRIZu5QDpZ4",
  host: "pg-nyneos-kanavlt885-nyneos.g.aivencloud.com",
  port: 15247,
  database: "defaultdb",
  ssl: {
    rejectUnauthorized: false,
  },
});

const tables = [
  //   "roles",
  "role_permissions",
  //   "permissions",
  //   "users",
  //   "user_roles",
];

(async () => {
  try {
    await db.connect();
    // Show all tables in the current database
    const tablesRes = await db.query(`
      SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;
    `);
    console.log(`\n📋 Total tables in DB: ${tablesRes.rows.length}`);
    console.table(tablesRes.rows);
    // Visualize selected tables
    for (const table of tables) {
      const res = await db.query(
        `
SELECT
  conname AS constraint_name,
  contype AS constraint_type,
  pg_get_constraintdef(oid) AS definition
FROM
  pg_constraint
WHERE
  conrelid = 'masterentity'::regclass;


  
      `
        // [table, table]
      );
      console.log(`\n📋 Table: ${table}`);
      console.table(res.rows);
    }
    db.end();
  } catch (err) {
    console.error("❌ Error:", err.stack);
    db.end();
  }
})();
