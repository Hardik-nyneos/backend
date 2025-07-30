const { pool } = require("../db");
const globalSession = require("../globalSession.js");

exports.createRole = async (req, res) => {
  const {
    name,
    rolecode,
    description,
    office_start_time_ist,
    office_end_time_ist,
    created_by,
  } = req.body;
  if (!name || !rolecode) {
    return res
      .status(400)
      .json({ success: false, error: "name and rolecode are required" });
  }
  try {
    const result = await pool.query(
      `INSERT INTO roles (name, rolecode, description, office_start_time_ist, office_end_time_ist, status, created_by) VALUES ($1, $2, $3, $4, $5, 'pending', $6) RETURNING *`,
      [
        name,
        rolecode,
        description,
        office_start_time_ist,
        office_end_time_ist,
        created_by,
      ]
    );
    res.status(201).json({ success: true, role: result.rows[0] });
  } catch (err) {
    res.status(400).json({ success: false, error: err.message });
  }
};

exports.getRoles = async (req, res) => {
  const { status } = req.query;
  try {
    let query = "SELECT * FROM roles";
    let params = [];
    if (status) {
      query += " WHERE status = $1";
      params.push(status);
    }
    const result = await pool.query(query, params);
    res.json({ success: true, roles: result.rows });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

exports.approveRole = async (req, res) => {
  const { id } = req.params;
  const { approved_by, approval_comment } = req.body;
  try {
    const result = await pool.query(
      `UPDATE roles SET status = 'Approved', approved_by = $1, approved_at = NOW(), approval_comment = $2 WHERE id = $3 RETURNING *`,
      [approved_by, approval_comment, id]
    );
    if (result.rowCount === 0)
      return res
        .status(404)
        .json({ success: false, message: "Role not found" });
    res.json({ success: true, role: result.rows[0] });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};
exports.approveMultipleRoles = async (req, res) => {
  const { roleIds, approved_by, approval_comment } = req.body;

  if (!Array.isArray(roleIds) || roleIds.length === 0 || !approved_by) {
    return res.status(400).json({
      success: false,
      message: "roleIds and approved_by are required",
    });
  }

  try {
    // First, fetch current statuses of roles
    const { rows: existingRoles } = await pool.query(
      `SELECT id, status FROM roles WHERE id = ANY($1::int[])`,
      [roleIds]
    );

    const toDelete = existingRoles
      .filter(role => role.status === "Delete-Approval")
      .map(role => role.id);

    const toApprove = existingRoles
      .filter(role => role.status !== "Delete-Approval")
      .map(role => role.id);

    const results = {
      deleted: [],
      approved: [],
    };

    // Delete roles with status = 'Delete-Approval'
    if (toDelete.length > 0) {
      const deleted = await pool.query(
        `DELETE FROM roles WHERE id = ANY($1::int[]) RETURNING *`,
        [toDelete]
      );
      results.deleted = deleted.rows;
    }

    // Approve remaining roles
    if (toApprove.length > 0) {
      const approved = await pool.query(
        `UPDATE roles 
         SET status = 'approved', approved_by = $1, approved_at = NOW(), approval_comment = $2 
         WHERE id = ANY($3::int[]) 
         RETURNING *`,
        [approved_by, approval_comment || "", toApprove]
      );
      results.approved = approved.rows;
    }

    res.status(200).json({ success: true, ...results });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

exports.rejectRole = async (req, res) => {
  const { id } = req.params;
  const { rejected_by, approval_comment } = req.body;
  try {
    const result = await pool.query(
      `UPDATE roles SET status = 'Rejected', rejected_by = $1, rejected_at = NOW(), approval_comment = $2 WHERE id = $3 RETURNING *`,
      [rejected_by, approval_comment, id]
    );
    if (result.rowCount === 0)
      return res
        .status(404)
        .json({ success: false, message: "Role not found" });
    res.json({ success: true, role: result.rows[0] });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

exports.getRolesPageData = async (req, res) => {
  // Get roleName from session
  const session = globalSession.UserSessions[0];
  if (!session) {
    return res.status(404).json({ error: "No active session found" });
  }
  const roleName = session.role;
  let rolesPerms = {};
  if (roleName) {
    const roleResult = await pool.query(
      "SELECT id FROM roles WHERE name = $1",
      [roleName]
    );
    if (roleResult.rows.length > 0) {
      const role_id = roleResult.rows[0].id;
      const permResult = await pool.query(
        `SELECT p.page_name, p.tab_name, p.action, rp.allowed
         FROM role_permissions rp
         JOIN permissions p ON rp.permission_id = p.id
         WHERE rp.role_id = $1 AND (rp.status = 'Approved' OR rp.status = 'approved')`,
        [role_id]
      );
      // Build permissions structure for 'roles'
      for (const row of permResult.rows) {
        if (row.page_name !== "roles") continue;
        const tab = row.tab_name;
        const action = row.action;
        const allowed = row.allowed;
        if (!rolesPerms.pagePermissions) rolesPerms.pagePermissions = {};
        if (!rolesPerms.tabs) rolesPerms.tabs = {};
        if (tab === null) {
          rolesPerms.pagePermissions[action] = allowed;
        } else {
          if (!rolesPerms.tabs[tab]) rolesPerms.tabs[tab] = {};
          rolesPerms.tabs[tab][action] = allowed;
        }
      }
    }
  }
  try {
    const result = await pool.query("SELECT * FROM roles");
    const roleData = result.rows.map((r) => ({
      id: r.id,
      name: r.name,
      role_code: r.role_code || "",
      description: r.description,
      startTime: r.office_start_time_ist || r.start_time || "",
      endTime: r.office_end_time_ist || r.end_time || "",
      createdAt: r.created_at ? r.created_at.toISOString() : "",
      status: r.status || "",
      createdBy: r.created_by || "",
      approvedBy: r.approved_by || null,
      approveddate: r.approved_at ? r.approved_at.toISOString() : null,
    }));
    res.json({ permissions: rolesPerms, roleData });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

exports.deleteRole = async (req, res) => {
  const { id } = req.params;
  try {
    const result = await pool.query(
      "UPDATE roles SET status = 'Delete-Approval' WHERE id = $1 RETURNING *",
      [id]
    );
    if (result.rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Role not found" });
    }
    res.json({ success: true, deleted: result.rows[0] });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

exports.getJustRoles = async (req, res) => {
  const { status } = req.query;
  try {
    // const query = "SELECT DISTINCT name FROM roles where status != 'Rejected' and status != 'Deleted' and status != 'Awaiting-Approval' and status != 'Awaiting-Delete-Approval'";
    // const query = "SELECT DISTICT name FROM roles where status == 'approved'";
    const query = "Select DISTINCT name from roles where status = 'approved';";
    const result = await pool.query(query);

    const roleNames = result.rows.map((row) => row.name);

    res.json({ success: true, roles: roleNames });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};
exports.getPendingRoles = async (req, res) => {
  // Get roleName from session
  const session = globalSession.UserSessions[0];
  if (!session) {
    return res.status(404).json({ error: "No active session found" });
  }
  const roleName = session.role;
  let rolesPerms = {};
  if (roleName) {
    const roleResult = await pool.query(
      "SELECT id FROM roles WHERE name = $1",
      [roleName]
    );
    if (roleResult.rows.length > 0) {
      const role_id = roleResult.rows[0].id;
      const permResult = await pool.query(
        `SELECT p.page_name, p.tab_name, p.action, rp.allowed
         FROM role_permissions rp
         JOIN permissions p ON rp.permission_id = p.id
         WHERE rp.role_id = $1 AND (rp.status = 'Approved' OR rp.status = 'approved')`,
        [role_id]
      );
      // Build permissions structure for 'roles'
      for (const row of permResult.rows) {
        if (row.page_name !== "roles") continue;
        const tab = row.tab_name;
        const action = row.action;
        const allowed = row.allowed;
        if (!rolesPerms.pagePermissions) rolesPerms.pagePermissions = {};
        if (!rolesPerms.tabs) rolesPerms.tabs = {};
        if (tab === null) {
          rolesPerms.pagePermissions[action] = allowed;
        } else {
          if (!rolesPerms.tabs[tab]) rolesPerms.tabs[tab] = {};
          rolesPerms.tabs[tab][action] = allowed;
        }
      }
    }
  }
  try {
    const result = await pool.query(
      "SELECT * FROM roles WHERE status IN ($1, $2, $3)",
      ["pending", "Awaiting-Approval","Delete-Approval"]
    );
    const roleData = result.rows.map((r) => ({
      id: r.id,
      name: r.name,
      role_code: r.role_code || "",
      description: r.description,
      startTime: r.office_start_time_ist || r.start_time || "",
      endTime: r.office_end_time_ist || r.end_time || "",
      createdAt: r.created_at ? r.created_at.toISOString() : "",
      status: r.status || "",
      createdBy: r.created_by || "",
      approvedBy: r.approved_by || null,
      approveddate: r.approved_at ? r.approved_at.toISOString() : null,
    }));
    res.json({ permissions: rolesPerms, roleData });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

exports.rejectMultipleRoles = async (req, res) => {
  const { roleIds, rejected_by, rejection_comment } = req.body;

  if (!Array.isArray(roleIds) || roleIds.length === 0 || !rejected_by) {
    return res.status(400).json({
      success: false,
      message: "roleIds and rejected_by are required",
    });
  }

  try {
    const result = await pool.query(
      `UPDATE roles 
       SET status = 'rejected', approved_by = $1, approved_at = NOW(), approval_comment = $2 
       WHERE id = ANY($3::int[]) 
       RETURNING *`,
      [rejected_by, rejection_comment || "", roleIds]
    );

    res.status(200).json({ success: true, updated: result.rows });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};
exports.updateRole = async (req, res) => {
  const { id } = req.params;
  const fields = { ...req.body, status: "Awaiting-Approval" };
  const keys = Object.keys(fields);
  if (keys.length === 0)
    return res
      .status(400)
      .json({ success: false, message: "No fields to update" });

  const setClause = keys.map((k, i) => `${k} = $${i + 1}`).join(", ");

  try {
    const result = await pool.query(
      `UPDATE roles SET ${setClause} WHERE id = $${
        keys.length + 1
      } RETURNING *`,
      [...keys.map((k) => fields[k]), id]
    );

    if (result.rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "Role not found" });
    }

    res.json({ success: true, role: result.rows[0] });
  } catch (err) {
    res.status(400).json({ success: false, error: err.message });
  }
};
