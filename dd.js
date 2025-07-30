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

const tables = [
  "roles",
  "role_permissions",
  "permissions",
  "users",
  "user_roles",
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
          ordinal_position AS "No",
          column_name AS "Column Name",
          data_type AS "Type",
          CASE 
            WHEN is_nullable = 'NO' THEN 
              CASE 
                WHEN column_name IN (
                  SELECT a.attname
                  FROM pg_index i
                  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                  WHERE i.indrelid = $1::regclass AND i.indisprimary
                ) THEN 'PRIMARY KEY'
                ELSE 'NOT NULL'
              END
            ELSE 'NULLABLE'
          END AS "Requirement"
        FROM information_schema.columns
        WHERE table_name = $2
        ORDER BY ordinal_position;
      `,
        [table, table]
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
