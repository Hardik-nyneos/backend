const globalSession = require("../globalSession");
const { pool } = require("../db");
const csv = require("csv-parser");
const path = require("path");
const fs = require("fs");

const getUserVars = async (req, res) => {
  const session = globalSession.UserSessions[0];

  if (!session) {
    return res.status(404).json({ error: "No active session found" });
  }

  const [firstName, ...restName] = session.name?.split(" ") || ["", ""];
  const secondName = restName.join(" ") || "";

  const loginDate = new Date(session.lastLoginTime || new Date());
  const dateLoggedIn = loginDate.toISOString().split("T")[0]; // "YYYY-MM-DD"
  const timeLoggedIn = loginDate.toTimeString().split(" ")[0]; // "HH:MM:SS"

  try {
    const query = "SELECT * FROM notifications WHERE user_id = $1";
    const result = await pool.query(query, [session.userId]);

    const messages = result.rows.map((row) => ({
      date: row.date,
      priority: row.priority,
      deadline: row.deadline,
      text: row.text,
    }));

    const userVars = {
      roleName: session.role,
      firstName,
      secondName,
      dateLoggedIn,
      timeLoggedIn,
      isLoggedIn: session.isLoggedIn,
      userEmailId: session.email,
      notification: {
        messages,
      },
    };

    res.json(userVars);
  } catch (err) {
    console.error("Error fetching notifications:", err);
    res.status(500).json({ error: "Failed to fetch notifications" });
  }
};

const getHedgingProposalsAggregated = async (req, res) => {
  try {
    // 1. Get current user session
    const session = globalSession.UserSessions[0];
    if (!session) {
      return res.status(404).json({ error: "No active session found" });
    }
    const userId = session.userId;

    // 2. Get user's business unit name
    const userResult = await pool.query(
      "SELECT business_unit_name FROM users WHERE id = $1",
      [userId]
    );
    if (!userResult.rows.length) {
      return res.status(404).json({ error: "User not found" });
    }
    const userBu = userResult.rows[0].business_unit_name;
    if (!userBu) {
      return res.status(404).json({ error: "User has no business unit assigned" });
    }

    // 3. Find all descendant business units using recursive CTE
    const entityResult = await pool.query(
      "SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
      [userBu]
    );
    if (!entityResult.rows.length) {
      return res.status(404).json({ error: "Business unit entity not found" });
    }
    const rootEntityId = entityResult.rows[0].entity_id;
    const descendantsResult = await pool.query(`
      WITH RECURSIVE descendants AS (
        SELECT entity_id, entity_name FROM masterEntity WHERE entity_id = $1
        UNION ALL
        SELECT me.entity_id, me.entity_name
        FROM masterEntity me
        INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
        INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
        WHERE (me.approval_status = 'Approved' OR me.approval_status = 'approved') AND (me.is_deleted = false OR me.is_deleted IS NULL)
      )
      SELECT entity_name FROM descendants
    `, [rootEntityId]);
    const buNames = descendantsResult.rows.map(r => r.entity_name);
    if (!buNames.length) {
      return res.status(404).json({ error: "No accessible business units found" });
    }

    await pool.query(
  `INSERT INTO hedging_proposal (exposure_header_id)
   SELECT exposure_header_id
   FROM exposure_headers
   WHERE entity = ANY($1)
     AND exposure_header_id NOT IN (
       SELECT exposure_header_id FROM hedging_proposal
     )`,
  [buNames]
);
    // 4. Join exposure_headers, exposure_line_items, exposure_bucketing, hedging_proposal
    // Only include headers where bucketing.status = 'approved'
    
    const query = `
      SELECT 
        h.entity AS business_unit,
        h.currency,
        h.exposure_type,
        ARRAY_AGG(h.exposure_header_id) AS contributing_header_ids,
        SUM(COALESCE(b.month_1, 0)) AS hedge_month1,
        SUM(COALESCE(b.month_2, 0)) AS hedge_month2,
        SUM(COALESCE(b.month_3, 0)) AS hedge_month3,
        SUM(COALESCE(b.month_4, 0)) AS hedge_month4,
        SUM(COALESCE(b.month_4_6, 0)) AS hedge_month4to6,
        SUM(COALESCE(b.month_6plus, 0)) AS hedge_month6plus,
        SUM(COALESCE(b.old_month1, 0)) AS old_hedge_month1,
        SUM(COALESCE(b.old_month2, 0)) AS old_hedge_month2,
        SUM(COALESCE(b.old_month3, 0)) AS old_hedge_month3,
        SUM(COALESCE(b.old_month4, 0)) AS old_hedge_month4,
        SUM(COALESCE(b.old_month4to6, 0)) AS old_hedge_month4to6,
        SUM(COALESCE(b.old_month6plus, 0)) AS old_hedge_month6plus,
        MAX(hp.comments) AS comments,
        MAX(hp.status_hedging) AS status
      FROM exposure_headers h
      JOIN exposure_bucketing b ON h.exposure_header_id = b.exposure_header_id AND (b.status_bucketing = 'approved' OR b.status_bucketing = 'Approved')
      JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
      LEFT JOIN hedging_proposal hp ON h.exposure_header_id = hp.exposure_header_id
      WHERE h.entity = ANY($1)
      GROUP BY h.entity, h.currency, h.exposure_type
    `;
    const result = await pool.query(query, [buNames]);

    const proposals = result.rows.map((row) => ({
      id: row.contributing_header_ids[0],
      business_unit: row.business_unit,
      currency: row.currency,
      exposure_type: row.exposure_type,
      hedge_month1: Number(row.hedge_month1),
      hedge_month2: Number(row.hedge_month2),
      hedge_month3: Number(row.hedge_month3),
      hedge_month4: Number(row.hedge_month4),
      hedge_month4to6: Number(row.hedge_month4to6),
      hedge_month6plus: Number(row.hedge_month6plus),
      old_hedge_month1: Number(row.old_hedge_month1),
      old_hedge_month2: Number(row.old_hedge_month2),
      old_hedge_month3: Number(row.old_hedge_month3),
      old_hedge_month4: Number(row.old_hedge_month4),
      old_hedge_month4to6: Number(row.old_hedge_month4to6),
      old_hedge_month6plus: Number(row.old_hedge_month6plus),
      comments: row.comments,
      status: row.status,
    }));

    res.json({ success: true, proposals });
  } catch (err) {
    console.error("Error aggregating hedging proposals:", err);
    res.status(500).json({ error: "Failed to aggregate proposals" });
  }
};

module.exports = {
  getUserVars,
  getHedgingProposalsAggregated,
};
