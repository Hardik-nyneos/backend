const { pool } = require("../db");

exports.upsertRolePermissions = async (req, res) => {
  const { roleName, pages } = req.body;
  if (!roleName || !pages || typeof pages !== "object") {
    return res
      .status(400)
      .json({ success: false, error: "roleName and pages object required" });
  }
  try {
    const roleResult = await pool.query(
      "SELECT id FROM roles WHERE name = $1",
      [roleName]
    );
    if (roleResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: "Role not found" });
    }
    const role_id = roleResult.rows[0].id;

    // Flatten new structure
    const perms = [];
    for (const [page, pageObj] of Object.entries(pages)) {
      if (pageObj.pagePermissions) {
        for (const [action, allowed] of Object.entries(pageObj.pagePermissions)) {
          perms.push({ page, tab: null, action, allowed });
        }
      }
      if (pageObj.tabs) {
        for (const [tab, tabObj] of Object.entries(pageObj.tabs)) {
          for (const [action, allowed] of Object.entries(tabObj)) {
            perms.push({ page, tab, action, allowed });
          }
        }
      }
    }

    // Step 1: Get all unique permissions (page, tab, action)
    const uniquePerms = perms.map(({ page, tab, action }) => [page, tab, action]);
    // Remove duplicates
    const uniquePermsSet = new Set(uniquePerms.map(JSON.stringify));
    const uniquePermsArr = Array.from(uniquePermsSet).map(JSON.parse);

    // Step 2: Bulk select existing permissions
    let permissionIdMap = {};
    if (uniquePermsArr.length > 0) {
      const values = uniquePermsArr
        .map((_, i) => `($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`)
        .join(", ");
      const flat = uniquePermsArr.flat();
      const selectQuery = `SELECT id, page_name, tab_name, action FROM permissions WHERE (page_name, tab_name, action) IN (${values})`;
      const selectResult = await pool.query(selectQuery, flat);
      for (const row of selectResult.rows) {
        permissionIdMap[`${row.page_name}|${row.tab_name}|${row.action}`] = row.id;
      }
    }

    // Step 3: Bulk insert missing permissions
    const missingPerms = uniquePermsArr.filter(([page, tab, action]) => {
      return !permissionIdMap.hasOwnProperty(`${page}|${tab}|${action}`);
    });
    if (missingPerms.length > 0) {
      const values = missingPerms
        .map((_, i) => `($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`)
        .join(", ");
      const flat = missingPerms.flat();
      const insertQuery = `INSERT INTO permissions (page_name, tab_name, action) VALUES ${values} RETURNING id, page_name, tab_name, action`;
      const insertResult = await pool.query(insertQuery, flat);
      for (const row of insertResult.rows) {
        permissionIdMap[`${row.page_name}|${row.tab_name}|${row.action}`] = row.id;
      }
    }

    // Step 4: Bulk upsert role_permissions
    const rolePermValues = perms.map(({ page, tab, action, allowed }) => {
      const permission_id = permissionIdMap[`${page}|${tab}|${action}`];
      return [role_id, permission_id, allowed];
    });
    if (rolePermValues.length > 0) {
      const values = rolePermValues
        .map((_, i) => `($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`)
        .join(", ");
      const flat = rolePermValues.flat();
      const upsertQuery = `INSERT INTO role_permissions (role_id, permission_id, allowed) VALUES ${values} ON CONFLICT (role_id, permission_id) DO UPDATE SET allowed = EXCLUDED.allowed`;
      await pool.query(upsertQuery, flat);
    }

    res.json({ success: true, results: perms });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

function flattenPermissionsWithHasAccess(pages) {
  const perms = [];
  for (const [page, pageObj] of Object.entries(pages)) {
    if ("hasAccess" in pageObj) {
      perms.push({
        page,
        tab: null,
        action: "hasAccess",
        allowed: pageObj.hasAccess,
      });
    }
    for (const [tab, tabObj] of Object.entries(pageObj)) {
      if (tab === "hasAccess") continue;
      if ("hasAccess" in tabObj) {
        perms.push({
          page,
          tab,
          action: "hasAccess",
          allowed: tabObj.hasAccess,
        });
      }
      for (const [action, allowed] of Object.entries(tabObj)) {
        if (action === "hasAccess") continue;
        perms.push({ page, tab, action, allowed });
      }
    }
  }
  return perms;
}

exports.WithHasAccess = async (req, res) => {
  const { roleName, pages } = req.body;
  if (!roleName || !pages || typeof pages !== "object") {
    return res
      .status(400)
      .json({ success: false, error: "roleName and pages object required" });
  }
  try {
    const roleResult = await pool.query(
      "SELECT id FROM roles WHERE name = $1",
      [roleName]
    );
    if (roleResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: "Role not found" });
    }
    const role_id = roleResult.rows[0].id;
    const perms = flattenPermissionsWithHasAccess(pages);

    const results = [];
    for (const { page, tab, action, allowed } of perms) {
      let permResult = await pool.query(
        "SELECT id FROM permissions WHERE page_name = $1 AND tab_name = $2 AND action = $3",
        [page, tab, action]
      );
      let permission_id;
      if (permResult.rows.length === 0) {
        const insertPerm = await pool.query(
          "INSERT INTO permissions (page_name, tab_name, action) VALUES ($1, $2, $3) RETURNING id",
          [page, tab, action]
        );
        permission_id = insertPerm.rows[0].id;
      } else {
        permission_id = permResult.rows[0].id;
      }
      await pool.query(
        `INSERT INTO role_permissions (role_id, permission_id, allowed)
         VALUES ($1, $2, $3)
         ON CONFLICT (role_id, permission_id)
         DO UPDATE SET allowed = EXCLUDED.allowed`,
        [role_id, permission_id, allowed]
      );
      results.push({ page, tab, action, allowed });
    }
    res.json({ success: true, results });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

// POST /api/permissions/permissionjson
exports.getRolePermissionsJson = async (req, res) => {
  const { roleName } = req.body;
  if (!roleName) {
    return res.status(400).json({ success: false, error: "roleName required" });
  }
  try {
    const roleResult = await pool.query(
      "SELECT id FROM roles WHERE name = $1",
      [roleName]
    );
    if (roleResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: "Role not found" });
    }
    const role_id = roleResult.rows[0].id;

    // Only include permissions with status 'Approved' or 'approved'
    const permResult = await pool.query(
      `SELECT p.page_name, p.tab_name, p.action, rp.allowed
       FROM role_permissions rp
       JOIN permissions p ON rp.permission_id = p.id
       WHERE rp.role_id = $1 AND (rp.status = 'Approved' OR rp.status = 'approved')`,
      [role_id]
    );

    // Reconstruct new structure
    const pages = {};
    for (const row of permResult.rows) {
      const page = row.page_name;
      const tab = row.tab_name;
      const action = row.action;
      const allowed = row.allowed;

      if (!pages[page]) pages[page] = {};
      if (tab === null) {
        if (!pages[page].pagePermissions) pages[page].pagePermissions = {};
        pages[page].pagePermissions[action] = allowed;
      } else {
        if (!pages[page].tabs) pages[page].tabs = {};
        if (!pages[page].tabs[tab]) pages[page].tabs[tab] = {};
        pages[page].tabs[tab][action] = allowed;
      }
    }

    res.json({ roleName, pages });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

// Update the status of all role permissions for a given roleName
// POST /api/permissions/update-role-permissions-status-by-name
// Body: { "roleName": "SomeRole", "status": "updated" }
exports.updateRolePermissionsStatusByName = async (req, res) => {
  const { roleName, status } = req.body;
  if (!roleName || !status) {
    return res
      .status(400)
      .json({ success: false, error: "roleName and status are required" });
  }
  try {
    // Get role_id from roles table using roleName
    const roleResult = await pool.query(
      "SELECT id FROM roles WHERE name = $1",
      [roleName]
    );
    if (roleResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: "Role not found" });
    }
    const roleId = roleResult.rows[0].id;

    // Update the status of all role permissions for the fetched roleId
    const updateResult = await pool.query(
      "UPDATE role_permissions SET status = $1 WHERE role_id = $2 RETURNING *",
      [status, roleId]
    );

    res.json({ success: true, updatedPermissions: updateResult.rows });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};

// Get distinct roleIds and their statuses, and map them to roleNames
// GET /api/permissions/roles-status
exports.getRolesStatus = async (req, res) => {
  try {
    // Fetch distinct roleIds and their statuses from role_permissions table
    const rolePermissionsResult = await pool.query(
      "SELECT DISTINCT role_id, status FROM role_permissions"
    );

    const rolesStatus = [];

    for (const row of rolePermissionsResult.rows) {
      const roleId = row.role_id;
      const status = row.status;

      // Fetch roleName for the roleId from roles table
      const roleResult = await pool.query(
        "SELECT name FROM roles WHERE id = $1",
        [roleId]
      );

      if (roleResult.rows.length > 0) {
        const roleName = roleResult.rows[0].name;
        rolesStatus.push({ roleName, status });
      }
    }

    res.json({ success: true, rolesStatus });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
};
