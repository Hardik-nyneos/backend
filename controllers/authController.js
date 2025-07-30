const { pool } = require("../db");
// const { globalSession } = require('./globalSession.js'); // Not from main.js!

// In your auth controller
// authController.js
const globalSession = require("../globalSession.js"); // Import the entire object

exports.loginUser = async (req, res) => {
  const { email, password } = req.body;

  try {
    const result = await pool.query(
      `
    SELECT 
  u.id AS user_id,
  u.employee_name,
  u.email,
  u.status AS user_status,
  r.id AS role_id,
  r.name AS role_name,
  r.rolecode,
  r.status AS role_status
FROM users u
LEFT JOIN user_roles ur ON u.id = ur.user_id
LEFT JOIN roles r ON ur.role_id = r.id
WHERE u.email = $1 AND u.password = $2;

    `,
      [email, password]
    );

    if (result.rows.length === 0) {
      return res.status(401).json({ message: "Invalid credentials" });
    }

    const user = result.rows[0];
    const sessionData = {
      userId: user.user_id,
      name: user.employee_name,
      email: user.email,
      role: user.role_name,
      rolecode: user.rolecode,
      permissions: user.permissions,
      lastLoginTime: new Date().toISOString(),
      isLoggedIn: true,
    };

    globalSession.addSession(sessionData);

    console.log("Current sessions after login:", globalSession.UserSessions);

    return res.json({
      message: "Login successful",
      user: sessionData,
    });
  } catch (err) {
    console.error("[LOGIN ERROR]", err);
    return res.status(500).json({ message: "Internal server error" });
  }
};
module.exports.logoutUser = async (req, res) => {
  const { userId } = req.body;
  
  if (!userId) {
    return res.status(400).json({ success: false, message: "User ID is required" });
  }

  // Convert userId to number if it comes as string
  const numericUserId = Number(userId);
  
  console.log("Attempting to logout user:", numericUserId);
  console.log("Current sessions before:", globalSession.UserSessions);
  
  const remainingSessions = globalSession.clearSession(numericUserId);
  
  console.log("Remaining sessions after:", remainingSessions);
  console.log("Internal session map:", globalSession._getSessionMap());
  
  res.json({ 
    success: true, 
    message: "Logout successful",
    remainingSessions: remainingSessions.length
  });
};

/*nikunj bhai  */
exports.getSidebarPermissions = async (req, res) => {
  const session = globalSession.UserSessions.find(
    (s) => s.email === req.body.email
  );
  const userId = session ? session.userId : null;
  if (!userId) {
    return res.status(401).json({ message: "User not logged in" });
  }
  try {
    const result = await pool.query(
      `
      SELECT p.page_name, p.tab_name, p.action, rp.allowed
      FROM user_roles ur
      JOIN roles r ON ur.role_id = r.id
      JOIN role_permissions rp ON r.id = rp.role_id
      JOIN permissions p ON rp.permission_id = p.id
      WHERE ur.user_id = $1
    `,
      [userId]
    );

    const allPages = [
      "permissions",
      "hedging-proposal",
      "entity",
      "fxstatusdash",
      "exposure-bucketing",
      "hedging-dashboard",
      "exposure-upload",
      "masters",
      "dashboard",
      "roles",
      "user-creation",
      "hierarchical",
    ];

    const pages = {};
    for (const page of allPages) {
      pages[page] = result.rows.some(
        (row) =>
          row.page_name &&
          row.page_name.toLowerCase() === page.toLowerCase() &&
          row.action === "hasAccess" &&
          row.allowed === true
      );
    }

    return res.json({ pages });
  } catch (err) {
    console.error("[SIDEBAR PERMISSIONS ERROR]", err);
    return res.status(500).json({ message: "Internal server error" });
  }
};
