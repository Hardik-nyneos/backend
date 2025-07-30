const { pool } = require('../db');

exports.showAllTables = async (req, res) => {
  const tables = ['users', 'roles', 'permissions', 'user_roles', 'role_permissions'];
  const result = {};
  try {
    for (const table of tables) {
      const data = await pool.query(`SELECT * FROM ${table}`);
      result[table] = data.rows;
    }
    res.json({ success: true, data: result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

exports.showTableStructure = async (req, res) => {
  try {
    const tables = ["users", "roles", "permissions", "user_roles", "role_permissions"];
    const structure = {};
    for (const table of tables) {
      const result = await pool.query(
        `SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = $1`,
        [table]
      );
      structure[table] = result.rows;
    }
    res.json({ success: true, structure });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};
