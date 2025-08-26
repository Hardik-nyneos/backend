const editExposureHeadersLineItemsJoined = async (req, res) => {
  const { id } = req.params; // id is a line item id (primary key of exposure_line_items)
  const fields = req.body;
  try {
    // Get the joined row to determine header and line item ids
    const joinResult = await pool.query(
      `SELECT h.*, l.* FROM exposure_headers h JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id WHERE l.exposure_header_id = $1`,
      [id]
    );
    if (joinResult.rows.length === 0) {
      return res.status(404).json({ success: false, message: "Row not found" });
    }
    const row = joinResult.rows[0];
    const exposure_header_id = row.exposure_header_id;
    // Get columns for each table
    const headerColsRes = await pool.query(
      `SELECT column_name FROM information_schema.columns WHERE table_name = 'exposure_headers'`
    );
    const lineColsRes = await pool.query(
      `SELECT column_name FROM information_schema.columns WHERE table_name = 'exposure_line_items'`
    );
    const headerCols = headerColsRes.rows.map((r) => r.column_name);
    const lineCols = lineColsRes.rows.map((r) => r.column_name);
    // Split fields
    const headerFields = {};
    const lineFields = {};
    for (const key of Object.keys(fields)) {
      if (headerCols.includes(key)) headerFields[key] = fields[key];
      if (lineCols.includes(key)) lineFields[key] = fields[key];
    }
    // Update header if needed
    if (Object.keys(headerFields).length > 0) {
      const keys = Object.keys(headerFields);
      const setClause =
        keys.map((k, i) => `${k} = $${i + 1}`).join(", ") +
        ", approval_status = 'Pending'";
      const values = [...keys.map((k) => headerFields[k]), exposure_header_id];
      await pool.query(
        `UPDATE exposure_headers SET ${setClause} WHERE exposure_header_id = $${
          keys.length + 1
        }`,
        values
      );
    }
    // Update line item if needed
    if (Object.keys(lineFields).length > 0) {
      const keys = Object.keys(lineFields);
      const setClause = keys.map((k, i) => `${k} = $${i + 1}`).join(", ");
      const values = [...keys.map((k) => lineFields[k]), id];
      await pool.query(
        `UPDATE exposure_line_items SET ${setClause} WHERE exposure_header_id = $${
          keys.length + 1
        }`,
        values
      );
    }
    // Return the updated joined row
    const updatedJoin = await pool.query(
      `SELECT h.*, l.* FROM exposure_headers h JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id WHERE l.exposure_header_id = $1`,
      [id]
    );
    res.json({ success: true, row: updatedJoin.rows[0] });
  } catch (err) {
    console.error("Error editing joined exposure headers/line items:", err);
    res.status(500).json({ success: false, error: err.message });
  }
};

const hedgeLinksDetails = async (req, res) => {
  try {
    // 0. Get current user session and allowed buNames (same as expfwdLinkingBookings)
    const session = globalSession.UserSessions[0];
    if (!session) {
      return res.status(404).json({ error: "No active session found" });
    }
    const userId = session.userId;
    const userResult = await pool.query(
      "SELECT business_unit_name FROM users WHERE id = $1",
      [userId]
    );
    if (!userResult.rows.length) {
      return res.status(404).json({ error: "User not found" });
    }
    const userBu = userResult.rows[0].business_unit_name;
    if (!userBu) {
      return res
        .status(404)
        .json({ error: "User has no business unit assigned" });
    }
    const entityResult = await pool.query(
      "SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
      [userBu]
    );
    if (!entityResult.rows.length) {
      return res.status(404).json({ error: "Business unit entity not found" });
    }
    const rootEntityId = entityResult.rows[0].entity_id;
    const descendantsResult = await pool.query(
      `WITH RECURSIVE descendants AS (
        SELECT entity_id, entity_name FROM masterEntity WHERE entity_id = $1
        UNION ALL
        SELECT me.entity_id, me.entity_name
        FROM masterEntity me
        INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
        INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
        WHERE (me.approval_status = 'Approved' OR me.approval_status = 'approved') AND (me.is_deleted = false OR me.is_deleted IS NULL)
      )
      SELECT entity_name FROM descendants`,
      [rootEntityId]
    );
    const buNames = descendantsResult.rows.map((r) => r.entity_name);
    if (!buNames.length) {
      return res
        .status(404)
        .json({ error: "No accessible business units found" });
    }

    // Query exposure_hedge_links joined to exposure_headers, filter by buNames
    const result = await pool.query(
      `SELECT l.*, h.document_id, f.internal_reference_id
       FROM exposure_hedge_links l
       LEFT JOIN exposure_headers h ON l.exposure_header_id = h.exposure_header_id
       LEFT JOIN forward_bookings f ON l.booking_id = f.system_transaction_id
       WHERE h.entity = ANY($1) AND l.is_active = TRUE`,
      [buNames]
    );
    res.json(result.rows);
  } catch (err) {
    console.error("Error in hedgeLinksDetails:", err);
    res.status(500).json({ error: "Failed to fetch hedge links details" });
  }
};
// GET /api/exposures/expfwdLinkingBookings
const expfwdLinkingBookings = async (req, res) => {
  try {
    // 0. Get current user session and allowed buNames
    const session = globalSession.UserSessions[0];
    if (!session) {
      return res.status(404).json({ error: "No active session found" });
    }
    const userId = session.userId;
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
    const entityResult = await pool.query(
      "SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
      [userBu]
    );
    if (!entityResult.rows.length) {
      return res.status(404).json({ error: "Business unit entity not found" });
    }
    const rootEntityId = entityResult.rows[0].entity_id;
    const descendantsResult = await pool.query(
      `WITH RECURSIVE descendants AS (
        SELECT entity_id, entity_name FROM masterEntity WHERE entity_id = $1
        UNION ALL
        SELECT me.entity_id, me.entity_name
        FROM masterEntity me
        INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
        INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
        WHERE (me.approval_status = 'Approved' OR me.approval_status = 'approved') AND (me.is_deleted = false OR me.is_deleted IS NULL)
      )
      SELECT entity_name FROM descendants`,
      [rootEntityId]
    );
    const buNames = descendantsResult.rows.map((r) => r.entity_name);
    if (!buNames.length) {
      return res.status(404).json({ error: "No accessible business units found" });
    }

    // 1. Get all forward bookings
    const bookingsResult = await pool.query(`
      SELECT system_transaction_id, entity_level_0, order_type, quote_currency, maturity_date, booking_amount, counterparty_dealer
      FROM forward_bookings
      WHERE processing_status = 'approved' OR processing_status = 'Approved'
    `);
    const bookings = bookingsResult.rows.filter((b) => buNames.includes(b.entity_level_0));

    // 2. Get linked amounts for all system_transaction_ids
    const bookingIds = bookings.map((b) => b.system_transaction_id);
    let hedgeLinks = [];
    if (bookingIds.length > 0) {
      const hedgeResult = await pool.query(
        `SELECT booking_id, SUM(hedged_amount) AS linked_amount
         FROM exposure_hedge_links
         WHERE booking_id = ANY($1)
         GROUP BY booking_id`,
        [bookingIds]
      );
      hedgeLinks = hedgeResult.rows;
    }
    const hedgeMap = {};
    for (const row of hedgeLinks) {
      hedgeMap[row.booking_id] = Number(row.linked_amount) || 0;
    }

    // 3. Get bu unit compliance (approved, not deleted masterEntity)
    const buResult = await pool.query(
      `SELECT entity_id, entity_name FROM masterEntity WHERE (approval_status = 'Approved' OR approval_status = 'approved')`
    );
    const buCompliance = {};
    for (const row of buResult.rows) {
      buCompliance[row.entity_name] = true;
    }

    // 4. Build response
    const response = bookings.map((b) => {
      const linkedAmount = hedgeMap[b.system_transaction_id] || 0;
      return {
        bu: b.entity_level_0,
        system_transaction_id: b.system_transaction_id,
        type: b.order_type,
        currency: b.quote_currency,
        maturity_date: b.maturity_date,
        amount: b.booking_amount,
        linked_amount: linkedAmount,
        bu_unit_compliance: !!buCompliance[b.entity_level_0],
        Bank: b.counterparty_dealer,
      };
    });
    res.json(response);
  } catch (err) {
    console.error("Error in expfwdLinkingBookings:", err);
    res
      .status(500)
      .json({ error: "Failed to fetch expfwdLinkingBookings data" });
  }
};
// GET /api/exposures/expfwdLinking
const expfwdLinking = async (req, res) => {
  try {
    // 0. Get current user session and allowed buNames
    const session = globalSession.UserSessions[0];
    if (!session) {
      return res.status(404).json({ error: "No active session found" });
    }
    const userId = session.userId;
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
    const entityResult = await pool.query(
      "SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
      [userBu]
    );
    if (!entityResult.rows.length) {
      return res.status(404).json({ error: "Business unit entity not found" });
    }
    const rootEntityId = entityResult.rows[0].entity_id;
    const descendantsResult = await pool.query(
      `WITH RECURSIVE descendants AS (
        SELECT entity_id, entity_name FROM masterEntity WHERE entity_id = $1
        UNION ALL
        SELECT me.entity_id, me.entity_name
        FROM masterEntity me
        INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
        INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
        WHERE (me.approval_status = 'Approved' OR me.approval_status = 'approved') AND (me.is_deleted = false OR me.is_deleted IS NULL)
      )
      SELECT entity_name FROM descendants`,
      [rootEntityId]
    );
    const buNames = descendantsResult.rows.map((r) => r.entity_name);
    if (!buNames.length) {
      return res.status(404).json({ error: "No accessible business units found" });
    }

    // 1. Get all exposure headers
    const headersResult = await pool.query(`
      SELECT exposure_header_id, entity, exposure_type, currency, document_date, total_open_amount, counterparty_name
      FROM exposure_headers
      WHERE approval_status = 'Approved' OR approval_status = 'approved'
    `);
    const headers = headersResult.rows.filter((h) => buNames.includes(h.entity));

    // 2. Get hedge amounts for all exposure_header_ids
    const headerIds = headers.map((h) => h.exposure_header_id);
    let hedgeLinks = [];
    if (headerIds.length > 0) {
      const hedgeResult = await pool.query(
        `SELECT exposure_header_id, SUM(hedged_amount) AS hedge_amount
         FROM exposure_hedge_links
         WHERE exposure_header_id = ANY($1)
         GROUP BY exposure_header_id`,
        [headerIds]
      );
      hedgeLinks = hedgeResult.rows;
    }
    const hedgeMap = {};
    for (const row of hedgeLinks) {
      hedgeMap[row.exposure_header_id] = Number(row.hedge_amount) || 0;
    }

    // 3. Get bu unit compliance (bu-wise compliance, check with entity)
    // Use logic from getRenderVars: get all approved, not deleted entity names
    const buResult = await pool.query(
      `SELECT entity_id, entity_name FROM masterEntity WHERE (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)`
    );
    const buCompliance = {};
    for (const row of buResult.rows) {
      buCompliance[row.entity_name] = true;
    }

    // 4. Build response
    const response = headers.map((h) => {
      const hedgeAmount =
        hedgeMap[h.exposure_header_id] &&
        hedgeMap[h.exposure_header_id] < Number(h.total_open_amount)
          ? hedgeMap[h.exposure_header_id]
          : 0;
      return {
        bu: h.entity,
        exposure_header_id: h.exposure_header_id,
        type: h.exposure_type,
        currency: h.currency,
        maturity_date: h.document_date,
        amount: h.total_open_amount,
        hedge_amount: hedgeAmount,
        bu_unit_compliance: !!buCompliance[h.entity],
        Bank: h.counterparty_name,
      };
    });
    res.json(response);
  } catch (err) {
    console.error("Error in expfwdLinking:", err);
    res.status(500).json({ error: "Failed to fetch expfwdLinking data" });
  }
};
// GET /api/exposures/maturity-expiry-count-7days-headers
const getMaturityExpiryCount7DaysFromHeaders = async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT document_date FROM exposure_headers WHERE document_date IS NOT NULL"
    );
    const now = new Date();
    let count7 = 0;
    for (const row of result.rows) {
      const maturityDate = new Date(row.document_date);
      if (isNaN(maturityDate.getTime())) continue;
      const diffDays = Math.ceil((maturityDate - now) / (1000 * 60 * 60 * 24));
      if (diffDays >= 0 && diffDays <= 7) {
        count7++;
      }
    }
    res.json({ value: count7 });
  } catch (err) {
    console.error(
      "Error fetching maturity expiry count for 7 days from headers:",
      err
    );
    res.status(500).json({
      error: "Failed to fetch maturity expiry count for 7 days from headers",
    });
  }
};
// GET /api/exposures/top-currencies-headers
const getTopCurrenciesFromHeaders = async (req, res) => {
  const rates = {
    USD: 1.0,
    AUD: 0.68,
    CAD: 0.75,
    CHF: 1.1,
    CNY: 0.14,
    RMB:0.14,
    EUR: 1.09,
    GBP: 1.28,
    JPY: 0.0067,
    SEK: 0.095,
    INR: 0.0117,
  };
  try {
    const result = await pool.query(
      "SELECT total_open_amount, currency FROM exposure_headers"
    );
    const currencyTotals = {};
    for (const row of result.rows) {
      const currency = (row.currency || "").toUpperCase();
      // const amount = Number(row.total_open_amount) || 0;
      const amount = Math.abs(Number(row.total_open_amount) || 0);
      const usdValue = amount * (rates[currency] || 1.0);
      currencyTotals[currency] = (currencyTotals[currency] || 0) + usdValue;
    }
    // Sort currencies by value descending and take top 5
    const sorted = Object.entries(currencyTotals).sort((a, b) => b[1] - a[1]);
    const topCurrencies = sorted.slice(0, 5).map(([currency, value], idx) => ({
      currency,
      value: Number(value.toFixed(1)),
      color:
        idx === 0
          ? "bg-green-400"
          : idx === 1
          ? "bg-blue-400"
          : idx === 2
          ? "bg-yellow-400"
          : idx === 3
          ? "bg-red-400"
          : "bg-purple-400",
    }));
    res.json(topCurrencies);
  } catch (err) {
    console.error("Error fetching top currencies from headers:", err);
    res
      .status(500)
      .json({ error: "Failed to fetch top currencies from headers" });
  }
};
// GET /api/exposures/bu-maturity-currency-summary-joined
const getBuMaturityCurrencySummaryJoined = async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT h.entity AS business_unit, h.currency, h.exposure_type,
              h.total_open_amount,
              b.month_1, b.month_2, b.month_3, b.month_4, b.month_4_6, b.month_6plus
       FROM exposure_headers h
       JOIN exposure_bucketing b ON h.exposure_header_id = b.exposure_header_id`
    );

    const summary = {};
    const maturityBuckets = [
      "month_1",
      "month_2",
      "month_3",
      "month_4",
      "month_4_6",
      "month_6plus",
    ];
    const bucketLabels = {
      month_1: "1 Month",
      month_2: "2 Month",
      month_3: "3 Month",
      month_4: "4 Month",
      month_4_6: "4-6 Month",
      month_6plus: "6 Month +",
    };

    for (const row of result.rows) {
      const bu = row.business_unit || "Unknown";
      const currency = (row.currency || "Unknown").toUpperCase();
      const exposureType = (row.exposure_type || "").toUpperCase();
      for (const bucket of maturityBuckets) {
        // const amount = Number(row[bucket]) || 0;
        const amount = Math.abs(Number(row[bucket]) || 0);
        if (amount === 0) continue;
        if (!summary[bucket]) summary[bucket] = {};
        if (!summary[bucket][bu]) summary[bucket][bu] = {};
        if (!summary[bucket][bu][currency])
          summary[bucket][bu][currency] = { payables: 0, receivables: 0 };
        if (exposureType === "PO"||exposureType==="creditors") {
          summary[bucket][bu][currency].payables += amount;
        } else if (exposureType === "SO" || exposureType === "LC"||exposureType==="debitors") {
          summary[bucket][bu][currency].receivables += amount;
        }
      }
    }

    const response = [];
    for (const bucket in summary) {
      const maturityLabel = bucketLabels[bucket] || bucket;
      for (const bu in summary[bucket]) {
        for (const currency in summary[bucket][bu]) {
          const { payables, receivables } = summary[bucket][bu][currency];
          response.push({
            maturity: maturityLabel,
            business_unit: bu,
            currency,
            payables,
            receivables,
          });
        }
      }
    }

    res.json(response);
  } catch (err) {
    console.error("Error fetching joined maturity summary:", err);
    res.status(500).json({ error: "Failed to fetch joined maturity summary" });
  }
};
// controllers/exposureUploadController.js
// Handles endpoints for exposure upload related logic
const path = require("path");
const fs = require("fs");
const globalSession = require("../globalSession");
const { pool } = require("../db");
const csv = require("csv-parser");
const multer = require("multer");
const XLSX = require("xlsx");
// Multer setup for multi-file form-data
const upload = multer({ dest: path.join(__dirname, "../uploads") });

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

const getRenderVars = async (req, res) => {
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
      return res
        .status(404)
        .json({ error: "User has no business unit assigned" });
    }

    // 3. Find all descendant business units using recursive CTE
    // First, get the entity_id for the user's business unit
    const entityResult = await pool.query(
      "SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
      [userBu]
    );
    if (!entityResult.rows.length) {
      return res.status(404).json({ error: "Business unit entity not found" });
    }
    const rootEntityId = entityResult.rows[0].entity_id;

    // Recursive CTE to get all descendant entity_ids
    const descendantsResult = await pool.query(
      `
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
    `,
      [rootEntityId]
    );

    const buNames = descendantsResult.rows.map((r) => r.entity_name);
    if (!buNames.length) {
      return res
        .status(404)
        .json({ error: "No accessible business units found" });
    }

    // 4. Filter exposures by business_unit in buNames
    const exposuresResult = await pool.query(
      `SELECT * FROM exposures WHERE business_unit = ANY($1)`,
      [buNames]
    );

    // Fetch permissions for 'exposure-upload' page for this role
    const roleName = session.role;
    let exposureUploadPerms = {};
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
        // Build permissions structure for 'exposure-upload'
        for (const row of permResult.rows) {
          if (row.page_name !== "exposure-upload") continue;
          const tab = row.tab_name;
          const action = row.action;
          const allowed = row.allowed;
          if (!exposureUploadPerms["exposure-upload"])
            exposureUploadPerms["exposure-upload"] = {};
          if (tab === null) {
            if (!exposureUploadPerms["exposure-upload"].pagePermissions)
              exposureUploadPerms["exposure-upload"].pagePermissions = {};
            exposureUploadPerms["exposure-upload"].pagePermissions[action] =
              allowed;
          } else {
            if (!exposureUploadPerms["exposure-upload"].tabs)
              exposureUploadPerms["exposure-upload"].tabs = {};
            if (!exposureUploadPerms["exposure-upload"].tabs[tab])
              exposureUploadPerms["exposure-upload"].tabs[tab] = {};
            exposureUploadPerms["exposure-upload"].tabs[tab][action] = allowed;
          }
        }
      }
    }
    res.json({
      ...(exposureUploadPerms["exposure-upload"]
        ? { "exposure-upload": exposureUploadPerms["exposure-upload"] }
        : {}),
      buAccessible: buNames,
      pageData: exposuresResult.rows,
    });
  } catch (err) {
    console.error("Error fetching exposures:", err);
    res.status(500).json({ error: "Failed to fetch exposures" });
  }
};

const getUserJourney = (req, res) => {
  res.json({
    process: "viewAllExposures",
    nextPageToCall: "exposure-Bucketing",
    actionCalledFrom: "submit",
  });
};

const getPendingApprovalVars = async (req, res) => {
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
      return res
        .status(404)
        .json({ error: "User has no business unit assigned" });
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

    const descendantsResult = await pool.query(
      `
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
    `,
      [rootEntityId]
    );

    const buNames = descendantsResult.rows.map((r) => r.entity_name);
    if (!buNames.length) {
      return res
        .status(404)
        .json({ error: "No accessible business units found" });
    }

    // 4. Filter exposures by business_unit in buNames and pending status
    const pendingExposuresResult = await pool.query(
      `SELECT * FROM exposures WHERE (status = 'pending' OR status = 'Pending' OR status = 'Delete-approval' OR status = 'Delete-Approval') AND business_unit = ANY($1)`,
      [buNames]
    );

    res.json({
      isLoadable: true,
      allExposuresTab: false,
      pendingApprovalTab: true,
      uploadingTab: false,
      btnApprove: true,
      buAccessible: buNames,
      pageData: pendingExposuresResult.rows,
    });
  } catch (err) {
    console.error("Error fetching pending exposures:", err);
    res.status(500).json({ error: "Failed to fetch pending exposures" });
  }
};

const exposuresColumns = [
  "reference_no",
  "type",
  "business_unit",
  "vendor_beneficiary",
  "po_amount",
  "po_currency",
  "maturity_expiry_date",
  "linked_id",
  "status",
  "file_reference_id",
  "upload_date",
  "purchase_invoice",
  "po_date",
  "shipping_bill_date",
  "supplier_name",
  "expected_payment_date",
  "comments",
  "created_at",
  "updated_at",
  "uploaded_by",
  "po_detail",
  "inco",
  "advance",
  "month1",
  "month2",
  "month3",
  "month4",
  "month4to6",
  "month6plus",
  "old_month1",
  "old_month2",
  "old_month3",
  "old_month4",
  "old_month4to6",
  "old_month6plus",
  "hedge_month1",
  "hedge_month2",
  "hedge_month3",
  "hedge_month4",
  "hedge_month4to6",
  "hedge_month6plus",
  "old_hedge_month1",
  "old_hedge_month2",
  "old_hedge_month3",
  "old_hedge_month4",
  "old_hedge_month4to6",
  "old_hedge_month6plus",
  "status_hedge",
];

const uploadExposuresFromCSV = async (req, res) => {
  const filePath = path.join(__dirname, "../", req.file.path);
  const rows = [];
  // 1. Get current user session and allowed business units
  const session = globalSession.UserSessions[0];
  if (!session) {
    return res.status(404).json({ error: "No active session found" });
  }
  const userId = session.userId;
  // Get user's business unit name
  let buNames = [];
  try {
    const userResult = await pool.query(
      "SELECT business_unit_name FROM users WHERE id = $1",
      [userId]
    );
    if (!userResult.rows.length) {
      return res.status(404).json({ error: "User not found" });
    }
    const userBu = userResult.rows[0].business_unit_name;
    if (!userBu) {
      return res
        .status(404)
        .json({ error: "User has no business unit assigned" });
    }
    // Find all descendant business units using recursive CTE
    const entityResult = await pool.query(
      "SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
      [userBu]
    );
    if (!entityResult.rows.length) {
      return res.status(404).json({ error: "Business unit entity not found" });
    }
    const rootEntityId = entityResult.rows[0].entity_id;
    const descendantsResult = await pool.query(
      `
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
    `,
      [rootEntityId]
    );
    buNames = descendantsResult.rows.map((r) => r.entity_name);
    if (!buNames.length) {
      return res
        .status(404)
        .json({ error: "No accessible business units found" });
    }
  } catch (err) {
    console.error("Error fetching allowed business units:", err);
    return res
      .status(500)
      .json({ error: "Failed to fetch allowed business units" });
  }

  fs.createReadStream(filePath)
    .pipe(csv())
    .on("data", (row) => {
      const cleanedRow = {};
      for (let key in row) {
        const normalizedKey = key.trim().toLowerCase();
        if (exposuresColumns.includes(normalizedKey)) {
          let value = row[key]?.trim() || null;
          if (value === "") value = null;
          if (/^month|amount|advance/.test(normalizedKey) && value !== null) {
            value = parseInt(value);
            if (isNaN(value)) value = null;
          }
          if (/date/.test(normalizedKey) && value !== null) {
            const dateObj = new Date(value);
            value = isNaN(dateObj.getTime())
              ? null
              : dateObj.toISOString().slice(0, 10);
          }
          cleanedRow[normalizedKey] = value;
        }
      }
      cleanedRow["status"] = "Pending";
      rows.push(cleanedRow);
    })
    .on("end", async () => {
      try {
        // Validate all rows' business_unit
        const invalidRows = rows
          .filter((row) => !buNames.includes(row["business_unit"]))
          .map((row) => row["reference_no"] || "(no reference_no)");
        if (invalidRows.length > 0) {
          fs.unlinkSync(filePath);
          return res.status(400).json({
            error: "Some rows have business_unit not allowed for this user.",
            invalidReferenceNos: invalidRows,
          });
        }
        // All rows valid, insert all
        for (let row of rows) {
          const keys = Object.keys(row);
          if (keys.length === 0) continue;
          const values = keys.map((k) => row[k]);
          const placeholders = keys.map((_, i) => `$${i + 1}`).join(", ");
          const query = `
            INSERT INTO exposures (${keys.join(", ")})
            VALUES (${placeholders})
          `;
          await pool.query(query, values);
        }
        fs.unlinkSync(filePath);
        res.status(200).json({ message: "All rows inserted successfully." });
      } catch (err) {
        console.error("DB Insert Error:", err);
        res.status(500).json({ error: "Failed to insert data." });
      }
    })
    .on("error", (err) => {
      console.error("CSV Parse Error:", err);
      res.status(500).json({ error: "Failed to parse CSV file." });
    });
};

const deleteExposure = async (req, res) => {
  const { id, requested_by, delete_comment } = req.body;

  if (!id || !requested_by) {
    return res
      .status(400)
      .json({ success: false, message: "id and requested_by are required" });
  }

  try {
    const ids = Array.isArray(id) ? id : [id]; // Normalize to array

    const { rowCount } = await pool.query(
      `UPDATE exposures
       SET status = 'Delete-Approval'
       WHERE id = ANY($1::uuid[])`,
      [ids]
    );

    if (rowCount === 0) {
      return res
        .status(404)
        .json({ success: false, message: "No matching exposures found" });
    }

    res.status(200).json({
      success: true,
      message: `${rowCount} exposure(s) marked for delete approval`,
    });
  } catch (err) {
    console.error("deleteExposure error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
};

const approveMultipleExposures = async (req, res) => {
  const { exposureIds, approved_by, approval_comment } = req.body;

  if (!Array.isArray(exposureIds) || exposureIds.length === 0 || !approved_by) {
    return res.status(400).json({
      success: false,
      message: "exposureIds and approved_by are required",
    });
  }

  try {
    // Fetch current statuses
    const { rows: existingExposures } = await pool.query(
      `SELECT id, status FROM exposures WHERE id = ANY($1::uuid[])`,
      [exposureIds]
    );

    const toDelete = existingExposures
      .filter((row) => row.status === "Delete-Approval")
      .map((row) => row.id);

    const toApprove = existingExposures
      .filter((row) => row.status !== "Delete-Approval")
      .map((row) => row.id);

    const results = {
      deleted: [],
      approved: [],
    };

    // Delete exposures
    if (toDelete.length > 0) {
      const deleted = await pool.query(
        `DELETE FROM exposures WHERE id = ANY($1::uuid[]) RETURNING *`,
        [toDelete]
      );
      results.deleted = deleted.rows;
    }

    // Approve remaining exposures
    if (toApprove.length > 0) {
      const approved = await pool.query(
        `UPDATE exposures
         SET status = 'Approved'
         WHERE id = ANY($1::uuid[])
         RETURNING *`,
        [toApprove]
      );
      results.approved = approved.rows;
    }

    getMaturityExpiryCount7DaysFromHeaders,
      res.status(200).json({ success: true, ...results });
  } catch (err) {
    console.error("approveMultipleExposures error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
};

const rejectMultipleExposures = async (req, res) => {
  const { exposureIds, rejected_by, rejection_comment } = req.body;

  if (!Array.isArray(exposureIds) || exposureIds.length === 0 || !rejected_by) {
    return res.status(400).json({
      success: false,
      message: "exposureIds and rejected_by are required",
    });
  }

  try {
    const result = await pool.query(
      `UPDATE exposures
       SET status = 'Rejected'
       WHERE id = ANY($1::uuid[])
       RETURNING *`,
      [exposureIds]
    );

    res.status(200).json({ success: true, rejected: result.rows });
  } catch (err) {
    console.error("rejectMultipleExposures error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
};

const getBuMaturityCurrencySummary = async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT business_unit, po_currency, type,
              month_1, month_2, month_3, month_4, month_4_6, month_6plus
       FROM exposures`
    );

    const summary = {};
    const maturityBuckets = [
      "month_1",
      "month_2",
      "month_3",
      "month_4",
      "month_4_6",
      "month_6plus",
    ];

    const bucketLabels = {
      month_1: "1 Month",
      month_2: "2 Month",
      month_3: "3 Month",
      month_4: "4 Month",
      month_4_6: "4-6 Month",
      month_6plus: "6 Month +",
    };

    for (const row of result.rows) {
      const bu = row.business_unit || "Unknown";
      const currency = (row.po_currency || "Unknown").toUpperCase();
      const type = (row.type || "").toLowerCase();

      for (const bucket of maturityBuckets) {
        const amount = Number(row[bucket]) || 0;
        if (amount === 0) continue;

        if (!summary[bucket]) summary[bucket] = {};
        if (!summary[bucket][bu]) summary[bucket][bu] = {};
        if (!summary[bucket][bu][currency])
          summary[bucket][bu][currency] = { payable: 0, receivable: 0 };

        if (["payable", "po"].includes(type)) {
          summary[bucket][bu][currency].payable += amount;
        } else if (["receivable", "so"].includes(type)) {
          summary[bucket][bu][currency].receivable += amount;
        }
      }
    }

    const response = [];
    for (const bucket in summary) {
      const maturityLabel = bucketLabels[bucket] || bucket;
      for (const bu in summary[bucket]) {
        for (const currency in summary[bucket][bu]) {
          const { payable, receivable } = summary[bucket][bu][currency];
          response.push({
            maturity: maturityLabel,
            bu,
            currency,
            payable,
            receivable,
          });
        }
      }
    }

    res.json(response);
  } catch (err) {
    console.error("Error fetching maturity summary:", err);
    res.status(500).json({ error: "Failed to fetch summary" });
  }
};

const getTopCurrencies = async (req, res) => {
  const rates = {
    USD: 1.0,
    AUD: 0.68,
    CAD: 0.75,
    CHF: 1.1,
    CNY: 0.14,
    EUR: 1.09,
    GBP: 1.28,
    JPY: 0.0067,
    SEK: 0.095,
    INR: 0.0117,
  };
  try {
    const result = await pool.query(
      "SELECT po_amount, po_currency FROM exposures"
    );
    const currencyTotals = {};
    for (const row of result.rows) {
      const currency = (row.po_currency || "").toUpperCase();
      const amount = Number(row.po_amount) || 0;
      const usdValue = amount * (rates[currency] || 1.0);
      currencyTotals[currency] = (currencyTotals[currency] || 0) + usdValue;
    }
    // Sort currencies by value descending and take top 5
    const sorted = Object.entries(currencyTotals).sort((a, b) => b[1] - a[1]);
    const topCurrencies = sorted.slice(0, 5).map(([currency, value], idx) => ({
      currency,
      value: Number(value.toFixed(1)),
      color:
        idx === 0
          ? "bg-green-400"
          : idx === 1
          ? "bg-blue-400"
          : idx === 2
          ? "bg-yellow-400"
          : idx === 3
          ? "bg-red-400"
          : "bg-purple-400",
    }));
    res.json(topCurrencies);
  } catch (err) {
    console.error("Error fetching top currencies:", err);
    res.status(500).json({ error: "Failed to fetch top currencies" });
  }
};

const getPoAmountUsdSum = async (req, res) => {
  // Exchange rates to USD
  const rates = {
    USD: 1.0,
    AUD: 0.68,
    CAD: 0.75,
    CHF: 1.1,
    CNY: 0.14,
    EUR: 1.09,
    GBP: 1.28,
    JPY: 0.0067,
    SEK: 0.095,
    INR: 0.0117,
  };
  try {
    const result = await pool.query(
      "SELECT po_amount, po_currency FROM exposures"
    );
    let totalUsd = 0;
    for (const row of result.rows) {
      const amount = Number(row.po_amount) || 0;
      const currency = (row.po_currency || "").toUpperCase();
      const rate = rates[currency] || 1.0;
      totalUsd += amount * rate;
    }
    res.json({ totalUsd });
  } catch (err) {
    console.error("Error calculating PO amount sum in USD:", err);
    res.status(500).json({ error: "Failed to calculate PO amount sum in USD" });
  }
};

// GET /api/exposures/total-open-amount-usd-sum-headers
const getTotalOpenAmountUsdSumFromHeaders = async (req, res) => {
  // Exchange rates to USD
  const rates = {
    USD: 1.0,
    AUD: 0.68,
    CAD: 0.75,
    CHF: 1.1,
    CNY: 0.14,
    RMB:0.14,
    EUR: 1.09,
    GBP: 1.28,
    JPY: 0.0067,
    SEK: 0.095,
    INR: 0.0117,
  };
  try {
    const result = await pool.query(
      "SELECT total_open_amount, currency FROM exposure_headers"
    );
    let totalUsd = 0;
    for (const row of result.rows) {
      // const amount = Number(row.total_open_amount) || 0;
      const amount = Math.abs(Number(row.total_open_amount) || 0);
      const currency = (row.currency || "").toUpperCase();
      const rate = rates[currency] || 1.0;
      totalUsd += amount * rate;
    }
    res.json({ totalUsd });
  } catch (err) {
    console.error(
      "Error calculating total_open_amount sum in USD from headers:",
      err
    );
    res.status(500).json({
      error: "Failed to calculate total_open_amount sum in USD from headers",
    });
  }
};

// GET /api/exposures/payables
const getPayablesByCurrency = async (req, res) => {
  const rates = {
    USD: 1.0,
    AUD: 0.68,
    CAD: 0.75,
    CHF: 1.1,
    CNY: 0.14,
    EUR: 1.09,
    GBP: 1.28,
    JPY: 0.0067,
    SEK: 0.095,
    INR: 0.0117,
  };
  try {
    const result = await pool.query(
      "SELECT po_amount, po_currency FROM exposures WHERE type = 'po' OR type = 'payable' OR type = 'PO'"
    );
    const currencyTotals = {};
    for (const row of result.rows) {
      const currency = (row.po_currency || "").toUpperCase();
      const amount = Number(row.po_amount) || 0;
      currencyTotals[currency] =
        (currencyTotals[currency] || 0) + amount * (rates[currency] || 1.0);
    }
    const payablesData = Object.entries(currencyTotals).map(
      ([currency, amount]) => ({
        currency,
        amount: `$${(amount / 1000).toFixed(1)}K`,
      })
    );
    res.json(payablesData);
  } catch (err) {
    console.error("Error fetching payables by currency:", err);
    res.status(500).json({ error: "Failed to fetch payables by currency" });
  }
};

// GET /api/exposures/payables-headers
const getPayablesByCurrencyFromHeaders = async (req, res) => {
  const rates = {
    USD: 1.0,
    AUD: 0.68,
    CAD: 0.75,
    CHF: 1.1,
    CNY: 0.14,
    RMB:0.14,
    EUR: 1.09,
    GBP: 1.28,
    JPY: 0.0067,
    SEK: 0.095,
    INR: 0.0117,
  };
  try {
    const result = await pool.query(
      "SELECT total_open_amount, currency FROM exposure_headers WHERE (exposure_type = 'PO' OR exposure_type = 'creditors' OR exposure_type = 'grn') AND (approval_status = 'Approved' OR approval_status = 'approved');"
    );
    const currencyTotals = {};
    for (const row of result.rows) {
      const currency = (row.currency || "").toUpperCase();
      // const amount = Number(row.total_open_amount) || 0;
      const amount = Math.abs(Number(row.total_open_amount) || 0);
      currencyTotals[currency] =
        (currencyTotals[currency] || 0) + amount * (rates[currency] || 1.0);
    }
    const payablesData = Object.entries(currencyTotals).map(
      ([currency, amount]) => ({
        currency,
        amount: `$${(amount / 1000).toFixed(1)}K`,
      })
    );
    res.json(payablesData);
  } catch (err) {
    console.error("Error fetching payables by currency from headers:", err);
    res
      .status(500)
      .json({ error: "Failed to fetch payables by currency from headers" });
  }
};

// GET /api/exposures/receivables
const getReceivablesByCurrency = async (req, res) => {
  const rates = {
    USD: 1.0,
    AUD: 0.68,
    CAD: 0.75,
    CHF: 1.1,
    CNY: 0.14,
    EUR: 1.09,
    GBP: 1.28,
    JPY: 0.0067,
    SEK: 0.095,
    INR: 0.0117,
  };
  try {
    const result = await pool.query(
      "SELECT po_amount, po_currency FROM exposures WHERE type = 'so' OR type = 'receivable'"
    );
    const currencyTotals = {};
    for (const row of result.rows) {
      const currency = (row.po_currency || "").toUpperCase();
      const amount = Number(row.po_amount) || 0;
      currencyTotals[currency] =
        (currencyTotals[currency] || 0) + amount * (rates[currency] || 1.0);
    }
    const receivablesData = Object.entries(currencyTotals).map(
      ([currency, amount]) => ({
        currency,
        amount: `$${(amount / 1000).toFixed(1)}K`,
      })
    );
    res.json(receivablesData);
  } catch (err) {
    console.error("Error fetching receivables by currency:", err);
    res.status(500).json({ error: "Failed to fetch receivables by currency" });
  }
};

// GET /api/exposures/receivables-headers
const getReceivablesByCurrencyFromHeaders = async (req, res) => {
  const rates = {
    USD: 1.0,
    AUD: 0.68,
    CAD: 0.75,
    CHF: 1.1,
    CNY: 0.14,
    RMB: 0.14,
    EUR: 1.09,
    GBP: 1.28,
    JPY: 0.0067,
    SEK: 0.095,
    INR: 0.0117,
  };
  try {
    const result = await pool.query(
      "SELECT total_open_amount, currency, exposure_type FROM exposure_headers WHERE exposure_type = 'SO' OR exposure_type = 'LC' OR exposure_type = 'debitors' AND (approval_status = 'Approved' OR approval_status = 'approved');"
    );
    const currencyTotals = {};
    for (const row of result.rows) {
      const currency = (row.currency || "").toUpperCase();
      // const amount = Number(row.total_open_amount) || 0;
      const amount = Math.abs(Number(row.total_open_amount) || 0);
      currencyTotals[currency] =
        (currencyTotals[currency] || 0) + amount * (rates[currency] || 1.0);
    }
    const receivablesData = Object.entries(currencyTotals).map(
      ([currency, amount]) => ({
        currency,
        amount: `$${(amount / 1000).toFixed(1)}K`,
      })
    );
    res.json(receivablesData);
  } catch (err) {
    console.error("Error fetching receivables by currency from headers:", err);
    res
      .status(500)
      .json({ error: "Failed to fetch receivables by currency from headers" });
  }
};

// GET /api/exposures/getpoAmountByCurrency
const getAmountByCurrency = async (req, res) => {
  const rates = {
    USD: 1.0,
    AUD: 0.68,
    CAD: 0.75,
    CHF: 1.1,
    CNY: 0.14,
    EUR: 1.09,
    GBP: 1.28,
    JPY: 0.0067,
    SEK: 0.095,
    INR: 0.0117,
  };
  try {
    const result = await pool.query(
      "SELECT po_amount, po_currency FROM exposures"
    );
    const currencyTotals = {};
    for (const row of result.rows) {
      const currency = (row.po_currency || "").toUpperCase();
      const amount = Number(row.po_amount) || 0;
      currencyTotals[currency] =
        (currencyTotals[currency] || 0) + amount * (rates[currency] || 1.0);
    }
    const payablesData = Object.entries(currencyTotals).map(
      ([currency, amount]) => ({
        currency,
        amount: `$${(amount / 1000).toFixed(1)}K`,
      })
    );

    res.json(payablesData);
  } catch (err) {
    console.error("Error fetching payables by currency:", err);
    res.status(500).json({ error: "Failed to fetch payables by currency" });
  }
};

// GET /api/exposures/getAmountByCurrency-headers
const getAmountByCurrencyFromHeaders = async (req, res) => {
  const rates = {
    USD: 1.0,
    AUD: 0.68,
    CAD: 0.75,
    CHF: 1.1,
    CNY: 0.14,
    RMB: 0.14,
    EUR: 1.09,
    GBP: 1.28,
    JPY: 0.0067,
    SEK: 0.095,
    INR: 0.0117,
  };
  try {
    const result = await pool.query(
      "SELECT total_open_amount, currency FROM exposure_headers"
    );
    const currencyTotals = {};
    for (const row of result.rows) {
      const currency = (row.currency || "").toUpperCase();
      // const amount = Number(row.total_open_amount) || 0;
      const amount = Math.abs(Number(row.total_open_amount) || 0);
      currencyTotals[currency] =
        (currencyTotals[currency] || 0) + amount * (rates[currency] || 1.0);
    }
    const payablesData = Object.entries(currencyTotals).map(
      ([currency, amount]) => ({
        currency,
        amount: `$${(amount / 1000).toFixed(1)}K`,
      })
    );

    res.json(payablesData);
  } catch (err) {
    console.error("Error fetching payables by currency from headers:", err);
    res
      .status(500)
      .json({ error: "Failed to fetch payables by currency from headers" });
  }
};

const getBusinessUnitCurrencySummary = async (req, res) => {
  // Exchange rates to USD
  const rates = {
    USD: 1.0,
    AUD: 0.68,
    CAD: 0.75,
    CHF: 1.1,
    CNY: 0.14,
    EUR: 1.09,
    GBP: 1.28,
    JPY: 0.0067,
    SEK: 0.095,
    INR: 0.0117,
  };
  try {
    const result = await pool.query(
      "SELECT business_unit, po_currency, po_amount FROM exposures"
    );
    // Aggregate by business_unit and currency
    const buMap = {};
    for (const row of result.rows) {
      const bu = row.business_unit || "Unknown";
      const currency = (row.po_currency || "Unknown").toUpperCase();
      const amount = Number(row.po_amount) || 0;
      const usdAmount = amount * (rates[currency] || 1.0);
      if (!buMap[bu]) buMap[bu] = {};
      if (!buMap[bu][currency]) buMap[bu][currency] = 0;
      buMap[bu][currency] += usdAmount;
    }
    // Format output
    const output = Object.entries(buMap).map(([bu, currencies]) => {
      const total = Object.values(currencies).reduce((a, b) => a + b, 0);
      return {
        name: bu,
        total: `$${(total / 1000).toFixed(1)}K`,
        currencies: Object.entries(currencies).map(([code, amount]) => ({
          code,
          amount: `$${(amount / 1000).toFixed(1)}K`,
        })),
      };
    });
    res.json(output);
  } catch (err) {
    console.error("Error fetching business unit currency summary:", err);
    res
      .status(500)
      .json({ error: "Failed to fetch business unit currency summary" });
  }
};

// GET /api/exposures/business-unit-currency-summary-headers
const getBusinessUnitCurrencySummaryFromHeaders = async (req, res) => {
  // Exchange rates to USD
  const rates = {
    USD: 1.0,
    AUD: 0.68,
    CAD: 0.75,
    CHF: 1.1,
    CNY: 0.14,
    RMB: 0.14,
    EUR: 1.09,
    GBP: 1.28,
    JPY: 0.0067,
    SEK: 0.095,
    INR: 0.0117,
  };
  try {
    const result = await pool.query(
      "SELECT entity, currency, total_open_amount FROM exposure_headers WHERE (approval_status = 'Approved' OR approval_status = 'approved');"
    );
    // Aggregate by entity (business_unit) and currency
    const buMap = {};
    for (const row of result.rows) {
      const bu = row.entity || "Unknown";
      const currency = (row.currency || "Unknown").toUpperCase();
      // const amount = Number(row.total_open_amount) || 0;
      const amount = Math.abs(Number(row.total_open_amount) || 0);
      const usdAmount = amount * (rates[currency] || 1.0);
      if (!buMap[bu]) buMap[bu] = {};
      if (!buMap[bu][currency]) buMap[bu][currency] = 0;
      buMap[bu][currency] += usdAmount;
    }
    // Format output
    const output = Object.entries(buMap).map(([bu, currencies]) => {
      const total = Object.values(currencies).reduce((a, b) => a + b, 0);
      return {
        name: bu,
        total: `$${(total / 1000).toFixed(1)}K`,
        currencies: Object.entries(currencies).map(([code, amount]) => ({
          code,
          amount: `$${(amount / 1000).toFixed(1)}K`,
        })),
      };
    });
    res.json(output);
  } catch (err) {
    console.error(
      "Error fetching business unit currency summary from headers:",
      err
    );
    res.status(500).json({
      error: "Failed to fetch business unit currency summary from headers",
    });
  }
};

const getMaturityExpirySummary = async (req, res) => {
  // Exchange rates to USD
  const rates = {
    USD: 1.0,
    AUD: 0.68,
    CAD: 0.75,
    CHF: 1.1,
    CNY: 0.14,
    EUR: 1.09,
    GBP: 1.28,
    JPY: 0.0067,
    SEK: 0.095,
    INR: 0.0117,
  };
  try {
    const result = await pool.query(
      "SELECT po_amount, po_currency, maturity_expiry_date FROM exposures WHERE maturity_expiry_date IS NOT NULL"
    );
    const now = new Date();
    let sum7 = 0,
      sum30 = 0,
      sumTotal = 0;
    for (const row of result.rows) {
      const amount = Number(row.po_amount) || 0;
      const currency = (row.po_currency || "USD").toUpperCase();
      const rate = rates[currency] || 1.0;
      const usdAmount = amount * rate;
      const maturityDate = new Date(row.maturity_expiry_date);
      if (isNaN(maturityDate.getTime())) continue;
      const diffDays = Math.ceil((maturityDate - now) / (1000 * 60 * 60 * 24));
      if (diffDays >= 0) {
        sumTotal += usdAmount;
        if (diffDays <= 7) sum7 += usdAmount;
        if (diffDays <= 30) sum30 += usdAmount;
      }
    }
    const output = [
      { label: "Next 7 Days", value: `$${(sum7 / 1000).toFixed(1)}K` },
      { label: "Next 30 Days", value: `$${(sum30 / 1000).toFixed(1)}K` },
      { label: "Total Upcoming", value: `$${(sumTotal / 1000).toFixed(1)}K` },
    ];
    res.json(output);
  } catch (err) {
    console.error("Error fetching maturity expiry summary:", err);
    res.status(500).json({ error: "Failed to fetch maturity expiry summary" });
  }
};

// GET /api/exposures/maturity-expiry-summary-headers
const getMaturityExpirySummaryFromHeaders = async (req, res) => {
  // Exchange rates to USD
  const rates = {
    USD: 1.0,
    AUD: 0.68,
    CAD: 0.75,
    CHF: 1.1,
    CNY: 0.14,
    RMB:0.14,
    EUR: 1.09,
    GBP: 1.28,
    JPY: 0.0067,
    SEK: 0.095,
    INR: 0.0117,
  };
  try {
    const result = await pool.query(
      "SELECT total_open_amount, currency, document_date FROM exposure_headers WHERE document_date IS NOT NULL"
    );
    const now = new Date();
    let sum7 = 0,
      sum30 = 0,
      sumTotal = 0;
    for (const row of result.rows) {
      // const amount = Number(row.total_open_amount) || 0;
      const amount = Math.abs(Number(row.total_open_amount) || 0);
      const currency = (row.currency || "USD").toUpperCase();
      const rate = rates[currency] || 1.0;
      const usdAmount = amount * rate;
      const maturityDate = new Date(row.document_date);
      if (isNaN(maturityDate.getTime())) continue;
      const diffDays = Math.ceil((maturityDate - now) / (1000 * 60 * 60 * 24));
      if (diffDays >= 0) {
        sumTotal += usdAmount;
        if (diffDays <= 7) sum7 += usdAmount;
        if (diffDays <= 30) sum30 += usdAmount;
      }
    }
    const output = [
      { label: "Next 7 Days", value: `$${(sum7 / 1000).toFixed(1)}K` },
      { label: "Next 30 Days", value: `$${(sum30 / 1000).toFixed(1)}K` },
      { label: "Total Upcoming", value: `$${(sumTotal / 1000).toFixed(1)}K` },
    ];
    res.json(output);
  } catch (err) {
    console.error("Error fetching maturity expiry summary from headers:", err);
    res
      .status(500)
      .json({ error: "Failed to fetch maturity expiry summary from headers" });
  }
};
const getMaturityExpiryCount7Days = async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT maturity_expiry_date FROM exposures WHERE maturity_expiry_date IS NOT NULL"
    );
    const now = new Date();
    let count7 = 0;
    for (const row of result.rows) {
      const maturityDate = new Date(row.maturity_expiry_date);
      if (isNaN(maturityDate.getTime())) continue;
      const diffDays = Math.ceil((maturityDate - now) / (1000 * 60 * 60 * 24));
      if (diffDays >= 0 && diffDays <= 7) {
        count7++;
      }
    }
    res.json({ value: count7 });
  } catch (err) {
    console.error("Error fetching maturity expiry count for 7 days:", err);
    res
      .status(500)
      .json({ error: "Failed to fetch maturity expiry count for 7 days" });
  }
};
/*--------------------------------      newest  codee      ------------------------------------------------------- */
// Batch upload endpoint for staging tables
const { v4: uuidv4 } = require("uuid");

// Helper to parse a file (csv/xls/xlsx) and return array of objects
function parseUploadFile(filePath, mimetype) {
  return new Promise((resolve, reject) => {
    const ext = path.extname(filePath).toLowerCase();
    if (ext === ".csv" || mimetype === "text/csv") {
      const rows = [];
      fs.createReadStream(filePath)
        .pipe(csv())
        .on("data", (row) => rows.push(row))
        .on("end", () => resolve(rows))
        .on("error", (err) => reject(err));
    } else if (
      ext === ".xls" ||
      ext === ".xlsx" ||
      mimetype ===
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ||
      mimetype === "application/vnd.ms-excel"
    ) {
      try {
        const workbook = XLSX.readFile(filePath);
        const sheetName = workbook.SheetNames[0];
        const rows = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], {
          defval: null,
        });
        resolve(rows);
      } catch (err) {
        reject(err);
      }
    } else {
      reject(new Error("Unsupported file type"));
    }
  });
}
// New batch upload endpoint for form-data file upload
// const batchUploadStagingData = async (req, res) => {
//   try {
//     const session = globalSession.UserSessions[0];
//     if (!session)
//       return res.status(404).json({ error: "No active session found" });

//     // req.files: { input_letters_of_credit: [file, ...], ... }
//     const files = [];
//     if (req.files && req.files.input_letters_of_credit) {
//       for (const file of req.files.input_letters_of_credit) {
//         files.push({
//           dataType: "LC",
//           file,
//           tableName: "input_letters_of_credit",
//           filename: file.originalname,
//         });
//       }
//     }
//     if (req.files && req.files.input_purchase_orders) {
//       for (const file of req.files.input_purchase_orders) {
//         files.push({
//           dataType: "PO",
//           file,
//           tableName: "input_purchase_orders",
//           filename: file.originalname,
//         });
//       }
//     }
//     if (req.files && req.files.input_sales_orders) {
//       for (const file of req.files.input_sales_orders) {
//         files.push({
//           dataType: "SO",
//           file,
//           tableName: "input_sales_orders",
//           filename: file.originalname,
//         });
//       }
//     }
//     if (files.length === 0) {
//       return res.status(400).json({ error: "No valid files found in request" });
//     }

//     // --- Business Unit Compliance Check (shared for all files) ---
//     const userId = session.userId;
//     let buNames = [];
//     try {
//       const userResult = await pool.query(
//         "SELECT business_unit_name FROM users WHERE id = $1",
//         [userId]
//       );
//       if (!userResult.rows.length) {
//         return res.status(404).json({ error: "User not found" });
//       }
//       const userBu = userResult.rows[0].business_unit_name;
//       if (!userBu) {
//         return res
//           .status(404)
//           .json({ error: "User has no business unit assigned" });
//       }
//       const entityResult = await pool.query(
//         "SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
//         [userBu]
//       );
//       if (!entityResult.rows.length) {
//         return res
//           .status(404)
//           .json({ error: "Business unit entity not found" });
//       }
//       const rootEntityId = entityResult.rows[0].entity_id;
//       const descendantsResult = await pool.query(
//         `
//         WITH RECURSIVE descendants AS (
//           SELECT entity_id, entity_name FROM masterEntity WHERE entity_id = $1
//           UNION ALL
//           SELECT me.entity_id, me.entity_name
//           FROM masterEntity me
//           INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
//           INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
//           WHERE (me.approval_status = 'Approved' OR me.approval_status = 'approved') AND (me.is_deleted = false OR me.is_deleted IS NULL)
//         )
//         SELECT entity_name FROM descendants
//       `,
//         [rootEntityId]
//       );
//       buNames = descendantsResult.rows.map((r) => r.entity_name);
//       if (!buNames.length) {
//         return res
//           .status(404)
//           .json({ error: "No accessible business units found" });
//       }
//     } catch (err) {
//       console.error("Error fetching allowed business units:", err);
//       return res
//         .status(500)
//         .json({ error: "Failed to fetch allowed business units" });
//     }

//     // --- Process each file independently ---
//     const results = [];
//     for (const fileObj of files) {
//       const { dataType, file, tableName, filename } = fileObj;
//       let dataArr = [];
//       try {
//         dataArr = await parseUploadFile(file.path, file.mimetype);
//       } catch (err) {
//         results.push({
//           filename,
//           error: `Failed to parse file: ${err.message}`,
//         });
//         fs.unlinkSync(file.path);
//         continue;
//       }
//       if (!Array.isArray(dataArr) || dataArr.length === 0) {
//         results.push({ filename, error: "No data to upload" });
//         fs.unlinkSync(file.path);
//         continue;
//       }
//       // Generate a new upload_batch_id for this file
//       const uploadBatchId = uuidv4();
//       // Check all rows' business unit using the correct column for each type
//       let buCol = null;
//       if (dataType === "LC") buCol = "applicant_name";
//       else if (dataType === "PO" || dataType === "SO") buCol = "entity";
//       const invalidRows = dataArr
//         .filter((row) => !buNames.includes(row[buCol]))
//         .map(
//           (row) =>
//             row["reference_no"] ||
//             row["document_no"] ||
//             row["system_lc_number"] ||
//             "(no ref)"
//         );
//       if (invalidRows.length > 0) {
//         results.push({
//           filename,
//           error: "Some rows have business_unit not allowed for this user.",
//           invalidReferenceNos: invalidRows,
//         });
//         fs.unlinkSync(file.path);
//         continue; // skip this file
//       }
//       // Try to insert all rows, if any error, skip the file
//       const now = new Date();
//       let insertedRows = 0;
//       let fileError = null;
//       try {
//         for (let i = 0; i < dataArr.length; i++) {
//           const row = dataArr[i];
//           row.upload_batch_id = uploadBatchId;
//           row.row_number = i + 1;
//           // Convert DD-MM-YYYY to YYYY-MM-DD for all date fields
//           for (const key of Object.keys(row)) {
//             if (/date/i.test(key) && typeof row[key] === "string") {
//               // Match DD-MM-YYYY
//               const m = row[key].match(/^(\d{2})-(\d{2})-(\d{4})$/);
//               if (m) {
//                 row[key] = `${m[3]}-${m[2]}-${m[1]}`;
//               }
//             }
//           }
//           // row.uploaded_by = session.userId;
//           // row.upload_date = now;
//           const keys = Object.keys(row);
//           const values = keys.map((k) => row[k]);
//           const placeholders = keys.map((_, idx) => `$${idx + 1}`).join(", ");
//           const query = `INSERT INTO ${tableName} (${keys.join(
//             ", "
//           )}) VALUES (${placeholders})`;
//           await pool.query(query, values);
//           insertedRows++;
//         }
//       } catch (err) {
//         fileError = err.message || "Failed to insert data";
//         // Rollback: delete all rows for this batch
//         await pool.query(
//           `DELETE FROM ${tableName} WHERE upload_batch_id = $1`,
//           [uploadBatchId]
//         );
//         results.push({ filename, error: fileError });
//         fs.unlinkSync(file.path);
//         continue;
//       }
//       // --- Absorb batch into exposure_headers and exposure_line_items ---
//       try {
//         const { rows: mappings } = await pool.query(
//           `SELECT * FROM upload_mappings WHERE exposure_type = $1 ORDER BY target_table_name, target_field_name`,
//           [dataType]
//         );
//         const { rows: stagedRows } = await pool.query(
//           `SELECT * FROM ${tableName} WHERE upload_batch_id = $1`,
//           [uploadBatchId]
//         );
//         for (const staged of stagedRows) {
//           // --- Build exposure_header ---
//           const header = {};
//           const headerDetails = {};
//           for (const m of mappings.filter(
//             (m) => m.target_table_name === "exposure_headers"
//           )) {
//             let val = null;
//             if (m.source_column_name === dataType) val = dataType;
//             else if (m.source_column_name === "Open") val = "Open";
//             else if (m.source_column_name === "true") val = true;
//             else if (m.source_column_name === tableName) val = staged;
//             else val = staged[m.source_column_name];
//             if (m.target_field_name === "additional_header_details")
//               headerDetails[m.source_column_name] = val;
//             else header[m.target_field_name] = val;
//           }
//           header["additional_header_details"] = headerDetails;
//           // --- Insert exposure_header ---
//           const headerFields = Object.keys(header);
//           const headerVals = headerFields.map((k) => header[k]);
//           const headerPlaceholders = headerFields
//             .map((_, idx) => `$${idx + 1}`)
//             .join(", ");
//           const headerInsert = `INSERT INTO exposure_headers (${headerFields.join(
//             ", "
//           )}) VALUES (${headerPlaceholders}) RETURNING exposure_header_id, document_id, exposure_type, total_original_amount`;
//           const { rows: headerRes } = await pool.query(
//             headerInsert,
//             headerVals
//           );
//           const exposureHeaderId = headerRes[0].exposure_header_id;
//           // --- Build exposure_line_item(s) ---
//           const line = {};
//           const lineDetails = {};
//           for (const m of mappings.filter(
//             (m) => m.target_table_name === "exposure_line_items"
//           )) {
//             let val = null;
//             if (m.source_column_name === dataType) val = dataType;
//             else if (m.source_column_name === "1") val = 1;
//             else if (m.source_column_name === tableName) val = staged;
//             else val = staged[m.source_column_name];
//             if (m.target_field_name === "additional_line_details")
//               lineDetails[m.source_column_name] = val;
//             else line[m.target_field_name] = val;
//           }
//           line["additional_line_details"] = lineDetails;
//           line["exposure_header_id"] = exposureHeaderId;
//           // --- Insert exposure_line_item ---
//           // Remove linked_exposure_header_id if present
//           if ("linked_exposure_header_id" in line) {
//             delete line["linked_exposure_header_id"];
//           }
//           const lineFields = Object.keys(line);
//           const lineVals = lineFields.map((k) => line[k]);
//           const linePlaceholders = lineFields
//             .map((_, idx) => `$${idx + 1}`)
//             .join(", ");
//           const lineInsert = `INSERT INTO exposure_line_items (${lineFields.join(
//             ", "
//           )}) VALUES (${linePlaceholders})`;
//           await pool.query(lineInsert, lineVals);
//         }
//         results.push({
//           success: true,
//           filename,
//           message: `Batch uploaded and absorbed to exposures`,
//           uploadBatchId,
//           insertedRows: stagedRows.length,
//         });
//       } catch (err) {
//         // Rollback: delete all rows for this batch
//         await pool.query(
//           `DELETE FROM ${tableName} WHERE upload_batch_id = $1`,
//           [uploadBatchId]
//         );
//         results.push({
//           success: false,
//           filename,
//           error: err.message || "Failed to absorb batch data",
//         });
//       }
//       // Always delete the uploaded file
//       fs.unlinkSync(file.path);
//     }
//     // Return results for all files
//     res.status(200).json({ results });
//   } catch (err) {
//     console.error("batchUploadStagingData error:", err);
//     res
//       .status(500)
//       .json({ success: false, error: "Failed to upload batch data" });
//   }
// };

const batchUploadStagingData = async (req, res) => {
  try {
    const session = globalSession.UserSessions[0];
    if (!session)
      return res.status(404).json({ error: "No active session found" });

    // req.files: { input_letters_of_credit: [file, ...], ... }
    const files = [];
    if (req.files && req.files.input_grn) {
      for (const file of req.files.input_grn) {
        files.push({
          dataType: "grn",
          file,
          tableName: "input_grn",
          filename: file.originalname,
        });
      }
    }
    if (req.files && req.files.input_letters_of_credit) {
      for (const file of req.files.input_letters_of_credit) {
        files.push({
          dataType: "LC",
          file,
          tableName: "input_letters_of_credit",
          filename: file.originalname,
        });
      }
    }
    if (req.files && req.files.input_purchase_orders) {
      for (const file of req.files.input_purchase_orders) {
        files.push({
          dataType: "PO",
          file,
          tableName: "input_purchase_orders",
          filename: file.originalname,
        });
      }
    }
    if (req.files && req.files.input_sales_orders) {
      for (const file of req.files.input_sales_orders) {
        files.push({
          dataType: "SO",
          file,
          tableName: "input_sales_orders",
          filename: file.originalname,
        });
      }
    }
    // Add support for input_creditors
    if (req.files && req.files.input_creditors) {
      for (const file of req.files.input_creditors) {
        files.push({
          dataType: "creditors",
          file,
          tableName: "input_creditors",
          filename: file.originalname,
        });
      }
    }
    // Add support for input_debitors
    if (req.files && req.files.input_debitors) {
      for (const file of req.files.input_debitors) {
        files.push({
          dataType: "debitors",
          file,
          tableName: "input_debitors",
          filename: file.originalname,
        });
      }
    }
    if (files.length === 0) {
      return res.status(400).json({ error: "No valid files found in request" });
    }

    // --- Business Unit Compliance Check (shared for all files) ---
    const userId = session.userId;
    let buNames = [];
    try {
      const userResult = await pool.query(
        "SELECT business_unit_name FROM users WHERE id = $1",
        [userId]
      );
      if (!userResult.rows.length) {
        return res.status(404).json({ error: "User not found" });
      }
      const userBu = userResult.rows[0].business_unit_name;
      if (!userBu) {
        return res
          .status(404)
          .json({ error: "User has no business unit assigned" });
      }
      const entityResult = await pool.query(
        "SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
        [userBu]
      );
      if (!entityResult.rows.length) {
        return res
          .status(404)
          .json({ error: "Business unit entity not found" });
      }
      const rootEntityId = entityResult.rows[0].entity_id;
      const descendantsResult = await pool.query(
        `
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
      `,
        [rootEntityId]
      );
      buNames = descendantsResult.rows.map((r) => r.entity_name);
      if (!buNames.length) {
        return res
          .status(404)
          .json({ error: "No accessible business units found" });
      }
    } catch (err) {
      console.error("Error fetching allowed business units:", err);
      return res
        .status(500)
        .json({ error: "Failed to fetch allowed business units" });
    }

    // --- Process each file independently ---
    const results = [];
    for (const fileObj of files) {
      const { dataType, file, tableName, filename } = fileObj;
      let dataArr = [];
      try {
        dataArr = await parseUploadFile(file.path, file.mimetype);
      } catch (err) {
        results.push({
          filename,
          error: `Failed to parse file: ${err.message}`,
        });
        fs.unlinkSync(file.path);
        continue;
      }
      if (!Array.isArray(dataArr) || dataArr.length === 0) {
        results.push({ filename, error: "No data to upload" });
        fs.unlinkSync(file.path);
        continue;
      }
      // Generate a new upload_batch_id for this file
      const uploadBatchId = uuidv4();
      // Check all rows' business unit using the correct column for each type
      let buCol = null;
      if (dataType === "LC") buCol = "applicant_name";
      else if (dataType === "PO" || dataType === "SO") buCol = "entity";
      else if (dataType === "creditors" || dataType === "debitors"||dataType === "grn")
        buCol = "company";
      // Constraint: if buCol is 'company', require 'bank_reference' to be present
      // if (buCol === "company") {
      //   const missingBankRefRows = dataArr
      //     .filter((row) => !row["bank_reference"] || row["bank_reference"].trim() === "")
      //     .map((row) => row["reference_no"] || row["document_no"] || row["system_lc_number"] || "(no ref)");
      //   if (missingBankRefRows.length > 0) {
      //     results.push({
      //       filename,
      //       error: "Some rows are missing bank_reference.",
      //       invalidReferenceNos: missingBankRefRows,
      //     });
      //     fs.unlinkSync(file.path);
      //     continue;
      //   }
      // }
      const invalidRows = dataArr
        .filter((row) => !buNames.includes(row[buCol]))
        .map(
          (row) =>
            row["reference_no"] ||
            row["document_no"] ||
            row["system_lc_number"] ||
            row["bank_reference"] ||
            "(no ref)"
        );
      if (invalidRows.length > 0) {
        results.push({
          filename,
          error: "Some rows have business_unit not allowed for this user.",
          invalidReferenceNos: invalidRows,
        });
        fs.unlinkSync(file.path);
        continue; // skip this file
      }
      // Try to insert all rows, if any error, skip the file
      const now = new Date();
      let insertedRows = 0;
      let fileError = null;
      try {
        for (let i = 0; i < dataArr.length; i++) {
          const row = dataArr[i];
          row.upload_batch_id = uploadBatchId;
          row.row_number = i + 1;
          // Convert DD-MM-YYYY to YYYY-MM-DD for all date fields
          for (const key of Object.keys(row)) {
            if (/date|timestamp/i.test(key) && row[key]) {
              if (typeof row[key] === "string") {
                // Match DD-MM-YYYY
                const m = row[key].match(/^(\d{2})-(\d{2})-(\d{4})$/);
                if (m) {
                  row[key] = `${m[3]}-${m[2]}-${m[1]}`;
                } else if (/^\d{5}$/.test(row[key])) {
                  // Excel serial date (5 digits)
                  const serial = parseInt(row[key], 10);
                  const excelEpoch = new Date(Date.UTC(1899, 11, 30));
                  const jsDate = new Date(excelEpoch.getTime() + serial * 86400000);
                  row[key] = jsDate.toISOString().slice(0, 10);
                }
              } else if (typeof row[key] === "number" && row[key] > 30000 && row[key] < 60000) {
                // Excel serial date as number
                const serial = row[key];
                const excelEpoch = new Date(Date.UTC(1899, 11, 30));
                const jsDate = new Date(excelEpoch.getTime() + serial * 86400000);
                row[key] = jsDate.toISOString().slice(0, 10);
              }
            }
             // Sanitize numeric fields: remove commas
             if (typeof row[key] === "string" && row[key].match(/^[-+]?\d{1,3}(,\d{3})*(\.\d+)?$/)) {
               row[key] = row[key].replace(/,/g, "");
             }
          }
          // for (const key of Object.keys(row)) {
          //   if (/date|posting_date|net_due_date/i.test(key) && row[key]) {
          //     if (typeof row[key] === "string") {
          //       // Match DD-MM-YYYY
          //       const m = row[key].match(/^([0-9]{2})-([0-9]{2})-([0-9]{4})$/);
          //       if (m) {
          //         row[key] = `${m[3]}-${m[2]}-${m[1]}`;
          //         continue;
          //       }
          //       // Match YYYY-MM-DD
          //       if (/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(row[key])) {
          //         continue;
          //       }
          //       // Match Excel serial date (5 digits)
          //       if (/^\d{5}$/.test(row[key])) {
          //         const serial = parseInt(row[key], 10);
          //         const excelEpoch = new Date(Date.UTC(1899, 11, 30));
          //         const jsDate = new Date(
          //           excelEpoch.getTime() + serial * 86400000
          //         );
          //         row[key] = jsDate.toISOString().slice(0, 10);
          //         continue;
          //       }
          //       // Not valid, throw error
          //       throw new Error(
          //         `Invalid date format in column '${key}' for row ${i + 1}: '${
          //           row[key]
          //         }'`
          //       );
          //     }
          //   }
          // }
          // row.uploaded_by = session.userId;
          // row.upload_date = now;
          const keys = Object.keys(row);
          const values = keys.map((k) => row[k]);
          const placeholders = keys.map((_, idx) => `$${idx + 1}`).join(", ");
          const query = `INSERT INTO ${tableName} (${keys.join(
            ", "
          )}) VALUES (${placeholders})`;
          await pool.query(query, values);
          insertedRows++;
        }
      } catch (err) {
        fileError = err.message || "Failed to insert data";
        // Rollback: delete all rows for this batch
        await pool.query(
          `DELETE FROM ${tableName} WHERE upload_batch_id = $1`,
          [uploadBatchId]
        );
        results.push({ filename, error: fileError });
        fs.unlinkSync(file.path);
        continue;
      }
      // --- Absorb batch into exposure_headers and exposure_line_items ---
      try {
        const { rows: mappings } = await pool.query(
          `SELECT * FROM upload_mappings WHERE exposure_type = $1 ORDER BY target_table_name, target_field_name`,
          [dataType]
        );
        const { rows: stagedRows } = await pool.query(
          `SELECT * FROM ${tableName} WHERE upload_batch_id = $1`,
          [uploadBatchId]
        );
        for (const staged of stagedRows) {
          // --- Build exposure_header ---
          const header = {};
          const headerDetails = {};
          for (const m of mappings.filter(
            (m) => m.target_table_name === "exposure_headers"
          )) {
            let val = null;
            if (m.source_column_name === dataType) val = dataType;
            else if (m.source_column_name === "Open") val = "Open";
            else if (m.source_column_name === "true") val = true;
            else if (m.source_column_name === tableName) val = staged;
            else val = staged[m.source_column_name];
            if (m.target_field_name === "additional_header_details")
              headerDetails[m.source_column_name] = val;
            else header[m.target_field_name] = val;
          }
          header["additional_header_details"] = headerDetails;
          // --- Insert exposure_header ---
          const headerFields = Object.keys(header);
          const headerVals = headerFields.map((k) => header[k]);
          const headerPlaceholders = headerFields
            .map((_, idx) => `$${idx + 1}`)
            .join(", ");
          const headerInsert = `INSERT INTO exposure_headers (${headerFields.join(
            ", "
          )}) VALUES (${headerPlaceholders}) RETURNING exposure_header_id, document_id, exposure_type, total_original_amount`;
          const { rows: headerRes } = await pool.query(
            headerInsert,
            headerVals
          );
          const exposureHeaderId = headerRes[0].exposure_header_id;
          // --- Build exposure_line_item(s) ---
          const line = {};
          const lineDetails = {};
          for (const m of mappings.filter(
            (m) => m.target_table_name === "exposure_line_items"
          )) {
            let val = null;
            if (m.source_column_name === dataType) val = dataType;
            else if (m.source_column_name === "1") val = 1;
            else if (m.source_column_name === tableName) val = staged;
            else val = staged[m.source_column_name];
            if (m.target_field_name === "additional_line_details")
              lineDetails[m.source_column_name] = val;
            else line[m.target_field_name] = val;
          }
          line["additional_line_details"] = lineDetails;
          line["exposure_header_id"] = exposureHeaderId;
          // --- Insert exposure_line_item ---
          // Remove linked_exposure_header_id if present
          if ("linked_exposure_header_id" in line) {
            delete line["linked_exposure_header_id"];
          }
          const lineFields = Object.keys(line);
          const lineVals = lineFields.map((k) => line[k]);
          const linePlaceholders = lineFields
            .map((_, idx) => `$${idx + 1}`)
            .join(", ");
          const lineInsert = `INSERT INTO exposure_line_items (${lineFields.join(
            ", "
          )}) VALUES (${linePlaceholders})`;
          await pool.query(lineInsert, lineVals);
        }
        results.push({
          success: true,
          filename,
          message: `Batch uploaded and absorbed to exposures`,
          uploadBatchId,
          insertedRows: stagedRows.length,
        });
      } catch (err) {
        // Rollback: delete all rows for this batch
        await pool.query(
          `DELETE FROM ${tableName} WHERE upload_batch_id = $1`,
          [uploadBatchId]
        );
        results.push({
          success: false,
          filename,
          error: err.message || "Failed to absorb batch data",
        });
      }
      // Always delete the uploaded file
      fs.unlinkSync(file.path);
    }
    // Return results for all files
    res.status(200).json({ results });
  } catch (err) {
    console.error("batchUploadStagingData error:", err);
    res
      .status(500)
      .json({ success: false, error: "Failed to upload batch data" });
  }
};

// GET /api/exposures/headers-lineitems
const getExposureHeadersLineItems = async (req, res) => {
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
      return res
        .status(404)
        .json({ error: "User has no business unit assigned" });
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

    const descendantsResult = await pool.query(
      `
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
    `,
      [rootEntityId]
    );

    const buNames = descendantsResult.rows.map((r) => r.entity_name);
    if (!buNames.length) {
      return res
        .status(404)
        .json({ error: "No accessible business units found" });
    }

    // 4. Join exposure_headers and exposure_line_items filtered by entity
    const joinResult = await pool.query(
      `SELECT h.*, l.*
       FROM exposure_headers h
       JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
       WHERE h.entity = ANY($1)`,
      [buNames]
    );

    // Fetch permissions for 'exposure-upload' page for this role
    const roleName = session.role;
    let exposureUploadPerms = {};
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
        for (const row of permResult.rows) {
          if (row.page_name !== "exposure-upload") continue;
          const tab = row.tab_name;
          const action = row.action;
          const allowed = row.allowed;
          if (!exposureUploadPerms["exposure-upload"])
            exposureUploadPerms["exposure-upload"] = {};
          if (tab === null) {
            if (!exposureUploadPerms["exposure-upload"].pagePermissions)
              exposureUploadPerms["exposure-upload"].pagePermissions = {};
            exposureUploadPerms["exposure-upload"].pagePermissions[action] =
              allowed;
          } else {
            if (!exposureUploadPerms["exposure-upload"].tabs)
              exposureUploadPerms["exposure-upload"].tabs = {};
            if (!exposureUploadPerms["exposure-upload"].tabs[tab])
              exposureUploadPerms["exposure-upload"].tabs[tab] = {};
            exposureUploadPerms["exposure-upload"].tabs[tab][action] = allowed;
          }
        }
      }
    }
    res.json({
      ...(exposureUploadPerms["exposure-upload"]
        ? { "exposure-upload": exposureUploadPerms["exposure-upload"] }
        : {}),
      buAccessible: buNames,
      pageData: joinResult.rows,
    });
  } catch (err) {
    console.error("Error fetching exposure headers/line items:", err);
    res
      .status(500)
      .json({ error: "Failed to fetch exposure headers/line items" });
  }
};

// GET /api/exposures/pending-headers-lineitems
const getPendingApprovalHeadersLineItems = async (req, res) => {
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
      return res
        .status(404)
        .json({ error: "User has no business unit assigned" });
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

    const descendantsResult = await pool.query(
      `
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
    `,
      [rootEntityId]
    );

    const buNames = descendantsResult.rows.map((r) => r.entity_name);
    if (!buNames.length) {
      return res
        .status(404)
        .json({ error: "No accessible business units found" });
    }

    // 4. Join exposure_headers and exposure_line_items filtered by entity and approval_status pending
    const joinResult = await pool.query(
      `SELECT h.*, l.*
       FROM exposure_headers h
       JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
       WHERE h.entity = ANY($1) AND h.approval_status NOT IN ('Approved', 'approved')`,
      [buNames]
    );

    // Fetch permissions for 'exposure-upload' page for this role
    const roleName = session.role;
    let exposureUploadPerms = {};
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
        for (const row of permResult.rows) {
          if (row.page_name !== "exposure-upload") continue;
          const tab = row.tab_name;
          const action = row.action;
          const allowed = row.allowed;
          if (!exposureUploadPerms["exposure-upload"])
            exposureUploadPerms["exposure-upload"] = {};
          if (tab === null) {
            if (!exposureUploadPerms["exposure-upload"].pagePermissions)
              exposureUploadPerms["exposure-upload"].pagePermissions = {};
            exposureUploadPerms["exposure-upload"].pagePermissions[action] =
              allowed;
          } else {
            if (!exposureUploadPerms["exposure-upload"].tabs)
              exposureUploadPerms["exposure-upload"].tabs = {};
            if (!exposureUploadPerms["exposure-upload"].tabs[tab])
              exposureUploadPerms["exposure-upload"].tabs[tab] = {};
            exposureUploadPerms["exposure-upload"].tabs[tab][action] = allowed;
          }
        }
      }
    }
    res.json({
      ...(exposureUploadPerms["exposure-upload"]
        ? { "exposure-upload": exposureUploadPerms["exposure-upload"] }
        : {}),
      buAccessible: buNames,
      pageData: joinResult.rows,
    });
  } catch (err) {
    console.error("Error fetching pending approval headers/line items:", err);
    res
      .status(500)
      .json({ error: "Failed to fetch pending approval headers/line items" });
  }
};

module.exports = {
  editExposureHeadersLineItemsJoined,
  hedgeLinksDetails,
  expfwdLinkingBookings,
  expfwdLinking,
  getUserVars,
  getRenderVars,
  getUserJourney,
  getPendingApprovalVars,
  uploadExposuresFromCSV,
  batchUploadStagingData,
  deleteExposure,
  approveMultipleExposures,
  rejectMultipleExposures,
  getBuMaturityCurrencySummary,
  getMaturityExpiryCount7DaysFromHeaders,
  getBuMaturityCurrencySummaryJoined,
  getTopCurrencies,
  getTopCurrenciesFromHeaders,
  getPoAmountUsdSum,
  getTotalOpenAmountUsdSumFromHeaders,
  getAmountByCurrencyFromHeaders,
  getAmountByCurrency,
  getReceivablesByCurrencyFromHeaders,
  getReceivablesByCurrency,
  getPayablesByCurrency,
  getPayablesByCurrencyFromHeaders,
  getBusinessUnitCurrencySummaryFromHeaders,
  getBusinessUnitCurrencySummary,
  getMaturityExpirySummaryFromHeaders,
  getMaturityExpirySummary,
  getMaturityExpiryCount7Days,
  getExposureHeadersLineItems,
  getPendingApprovalHeadersLineItems,
  approveMultipleExposureHeaders,
  deleteExposureHeaders,
  rejectMultipleExposureHeaders,
};

// POST /api/exposureUpload/delete-exposure-headers
// Body: { exposureHeaderIds: [id1, id2, ...], requested_by, delete_comment }
async function deleteExposureHeaders(req, res) {
  const { exposureHeaderIds, requested_by, delete_comment } = req.body;
  if (
    !Array.isArray(exposureHeaderIds) ||
    exposureHeaderIds.length === 0 ||
    !requested_by
  ) {
    return res.status(400).json({
      success: false,
      message: "exposureHeaderIds and requested_by are required",
    });
  }
  try {
    // Mark headers for delete-approval
    const { rowCount } = await pool.query(
      `UPDATE exposure_headers SET approval_status = 'Delete-Approval', delete_comment = $1 WHERE exposure_header_id = ANY($2::uuid[])`,
      [delete_comment || null, exposureHeaderIds]
    );
    if (rowCount === 0) {
      return res.status(404).json({
        success: false,
        message: "No matching exposure_headers found",
      });
    }
    // Optionally, mark line items as well (if needed)
    // await pool.query(
    //   `UPDATE exposure_line_items SET approval_status = 'Delete-Approval' WHERE exposure_header_id = ANY($1::uuid[])`,
    //   [exposureHeaderIds]
    // );
    res.status(200).json({
      success: true,
      message: `${rowCount} exposure_header(s) marked for delete approval`,
    });
  } catch (err) {
    console.error("deleteExposureHeaders error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}

// POST /api/exposureUpload/reject-multiple-headers
// Body: { exposureHeaderIds: [id1, id2, ...], rejected_by, rejection_comment }
async function rejectMultipleExposureHeaders(req, res) {
  const { exposureHeaderIds, rejected_by, rejection_comment } = req.body;
  if (
    !Array.isArray(exposureHeaderIds) ||
    exposureHeaderIds.length === 0 ||
    !rejected_by
  ) {
    return res.status(400).json({
      success: false,
      message: "exposureHeaderIds and rejected_by are required",
    });
  }
  try {
    // Reject headers
    const { rows } = await pool.query(
      `UPDATE exposure_headers SET approval_status = 'Rejected', rejected_by = $1, rejection_comment = $2, rejected_at = NOW() WHERE exposure_header_id = ANY($3::uuid[]) RETURNING *`,
      [rejected_by, rejection_comment || null, exposureHeaderIds]
    );
    // Optionally, reject line items as well
    // await pool.query(
    //   `UPDATE exposure_line_items SET approval_status = 'Rejected' WHERE exposure_header_id = ANY($1::uuid[])`,
    //   [exposureHeaderIds]
    // );
    res.status(200).json({ success: true, rejected: rows });
  } catch (err) {
    console.error("rejectMultipleExposureHeaders error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}

// POST /api/exposureUpload/approve-multiple-headers
// Body: { exposureHeaderIds: [id1, id2, ...], approved_by, approval_comment }
// async function approveMultipleExposureHeaders(req, res) {
//   const { exposureHeaderIds, approved_by, approval_comment } = req.body;
//   if (
//     !Array.isArray(exposureHeaderIds) ||
//     exposureHeaderIds.length === 0 ||
//     !approved_by
//   ) {
//     return res.status(400).json({
//       success: false,
//       message: "exposureHeaderIds and approved_by are required",
//     });
//   }
//   const client = await pool.connect();
//   try {
//     await client.query("BEGIN");
//     // Fetch current statuses and types, and additional_header_details
//     const { rows: headers } = await client.query(
//       `SELECT exposure_header_id, approval_status, exposure_type, status, document_id, total_original_amount, total_open_amount, additional_header_details
//        FROM exposure_headers WHERE exposure_header_id = ANY($1::uuid[])`,
//       [exposureHeaderIds]
//     );
//     // Only allow approval if status is 'pending' (case-insensitive), skip if 'Rejected' or 'Approved'
//     const toDelete = headers
//       .filter(
//         (h) =>
//           h.approval_status &&
//           h.approval_status.toLowerCase().includes("delete")
//       )
//       .map((h) => h.exposure_header_id);
//     const toApprove = headers
//       .filter((h) => {
//         const status = h.approval_status
//           ? h.approval_status.toLowerCase()
//           : "pending";
//         // Allow approval if status is 'pending' or 'rejected'
//         return status === "pending" || status === "rejected";
//       })
//       .map((h) => h.exposure_header_id);
//     const skipped = headers
//       .filter((h) => {
//         const status = h.approval_status
//           ? h.approval_status.toLowerCase()
//           : "pending";
//         // Only skip if already 'approved'
//         return status === "approved";
//       })
//       .map((h) => h.exposure_header_id);
//     const results = { deleted: [], approved: [], rolled: [], skipped };
//     // Handle delete-approval: delete header and line items
//     if (toDelete.length > 0) {
//       // Delete from exposure_rollover_log where child or parent matches toDelete to avoid FK violation
//       await client.query(
//         `DELETE FROM exposure_rollover_log WHERE child_header_id = ANY($1::uuid[]) OR parent_header_id = ANY($1::uuid[])`,
//         [toDelete]
//       );
//       // Optionally, log deleted headers before deletion
//       const { rows: deletedHeaders } = await client.query(
//         `DELETE FROM exposure_headers WHERE exposure_header_id = ANY($1::uuid[]) RETURNING *`,
//         [toDelete]
//       );
//       await client.query(
//         `DELETE FROM exposure_line_items WHERE exposure_header_id = ANY($1::uuid[])`,
//         [toDelete]
//       );
//       results.deleted = deletedHeaders;
//       // Reverse rollover if needed (if LC and linked_po_so_number exists)
//       for (const h of deletedHeaders) {
//         let parentDocNo = null;
//         if (
//           h.exposure_type === "LC" &&
//           h.additional_header_details &&
//           h.additional_header_details.input_letters_of_credit &&
//           h.additional_header_details.input_letters_of_credit
//             .linked_po_so_number
//         ) {
//           parentDocNo =
//             h.additional_header_details.input_letters_of_credit
//               .linked_po_so_number;
//         }
//         if (h.exposure_type === "LC" && parentDocNo) {
//           // Find parent exposure_header_id by document_id
//           const { rows: parentRows } = await client.query(
//             `SELECT exposure_header_id FROM exposure_headers WHERE document_id = $1 LIMIT 1`,
//             [parentDocNo]
//           );
//           if (parentRows.length > 0) {
//             const parentId = parentRows[0].exposure_header_id;
//             await client.query(
//               `UPDATE exposure_headers SET total_open_amount = total_open_amount + $1, status = 'Open' WHERE exposure_header_id = $2`,
//               [h.total_original_amount, parentId]
//             );
//           }
//         }
//       }
//     }
//     // Approve remaining headers
//     if (toApprove.length > 0) {
//       // 1. Approve all headers (approval_status only, do not change status)
//       const { rows: approvedHeaders } = await client.query(
//         `UPDATE exposure_headers SET approval_status = 'Approved', approved_by = $1, approval_comment = $2, approved_at = NOW()
//          WHERE exposure_header_id = ANY($3::uuid[])
//          RETURNING *`,
//         [approved_by, approval_comment || null, toApprove]
//       );
//       results.approved = approvedHeaders;

//       // 2. For each approved LC, if rollover is needed, update status as well
//       for (const h of approvedHeaders) {
//         let parentDocNo = null;
//         if (
//           h.exposure_type === "LC" &&
//           h.additional_header_details &&
//           h.additional_header_details.input_letters_of_credit &&
//           h.additional_header_details.input_letters_of_credit
//             .linked_po_so_number
//         ) {
//           parentDocNo =
//             h.additional_header_details.input_letters_of_credit
//               .linked_po_so_number;
//         }
//         if (h.exposure_type === "LC" && parentDocNo) {
//           // Find parent exposure_header_id by document_id
//           const { rows: parentRows } = await client.query(
//             `SELECT exposure_header_id FROM exposure_headers WHERE document_id = $1 LIMIT 1`,
//             [parentDocNo]
//           );
//           if (parentRows.length > 0) {
//             const parentId = parentRows[0].exposure_header_id;
//             // Subtract the original amount from parent's open amount
//             await client.query(
//               `UPDATE exposure_headers SET total_open_amount = total_open_amount - $1, status = 'Rolled' WHERE exposure_header_id = $2`,
//               [h.total_original_amount, parentId]
//             );
//             // Set status to 'Rolled' for this LC (status change only for rolled)
//             await client.query(
//               `UPDATE exposure_headers SET status = 'Rolled' WHERE exposure_header_id = $1`,
//               [h.exposure_header_id]
//             );
//             // Log the rollover
//             await client.query(
//               `INSERT INTO exposure_rollover_log (parent_header_id, child_header_id, rollover_amount, rollover_date, created_at)
//                VALUES ($1, $2, $3, CURRENT_DATE, NOW())`,
//               [parentId, h.exposure_header_id, h.total_original_amount]
//             );
//             results.rolled.push(h);
//           }
//         }
//       }
//     }
//     await client.query("COMMIT");
//     res.status(200).json({ success: true, ...results });
//   } catch (err) {
//     await client.query("ROLLBACK");
//     console.error("approveMultipleExposureHeaders error:", err);
//     res.status(500).json({ success: false, error: err.message });
//   } finally {
//     client.release();
//   }
// }

async function approveMultipleExposureHeaders(req, res) {
  const { exposureHeaderIds, approved_by, approval_comment } = req.body;
  if (
    !Array.isArray(exposureHeaderIds) ||
    exposureHeaderIds.length === 0 ||
    !approved_by
  ) {
    return res.status(400).json({
      success: false,
      message: "exposureHeaderIds and approved_by are required",
    });
  }
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    // Fetch current statuses and types, and additional_header_details
    const { rows: headers } = await client.query(
      `SELECT exposure_header_id, approval_status, exposure_type, status, document_id, total_original_amount, total_open_amount, additional_header_details
       FROM exposure_headers WHERE exposure_header_id = ANY($1::uuid[])`,
      [exposureHeaderIds]
    );
    // Only allow approval if status is 'pending' (case-insensitive), skip if 'Rejected' or 'Approved'
    const toDelete = headers
      .filter(
        (h) =>
          h.approval_status &&
          h.approval_status.toLowerCase().includes("delete")
      )
      .map((h) => h.exposure_header_id);
    const toApprove = headers
      .filter((h) => {
        const status = h.approval_status
          ? h.approval_status.toLowerCase()
          : "pending";
        // Allow approval if status is 'pending' or 'rejected'
        return status === "pending" || status === "rejected";
      })
      .map((h) => h.exposure_header_id);
    const skipped = headers
      .filter((h) => {
        const status = h.approval_status
          ? h.approval_status.toLowerCase()
          : "pending";
        // Only skip if already 'approved'
        return status === "approved";
      })
      .map((h) => h.exposure_header_id);
    const results = { deleted: [], approved: [], rolled: [], skipped };
    // Handle delete-approval: delete header and line items
    if (toDelete.length > 0) {
      // Delete from exposure_rollover_log where child or parent matches toDelete to avoid FK violation
      await client.query(
        `DELETE FROM exposure_rollover_log WHERE child_header_id = ANY($1::uuid[]) OR parent_header_id = ANY($1::uuid[])`,
        [toDelete]
      );
      // Optionally, log deleted headers before deletion
      const { rows: deletedHeaders } = await client.query(
        `DELETE FROM exposure_headers WHERE exposure_header_id = ANY($1::uuid[]) RETURNING *`,
        [toDelete]
      );
      await client.query(
        `DELETE FROM exposure_line_items WHERE exposure_header_id = ANY($1::uuid[])`,
        [toDelete]
      );
      results.deleted = deletedHeaders;
      // Reverse rollover if needed (if LC, GRN, creditors, debitors and linked_id exists)
      for (const h of deletedHeaders) {
        let parentDocNo = null;
        const type = h.exposure_type ? h.exposure_type.toLowerCase() : "";
        // LC logic
        if (
          type === "lc" &&
          h.additional_header_details &&
          h.additional_header_details.input_letters_of_credit &&
          h.additional_header_details.input_letters_of_credit.linked_po_so_number
        ) {
          parentDocNo = h.additional_header_details.input_letters_of_credit.linked_po_so_number;
        }
        // GRN logic
        if (
          type === "grn" &&
          h.additional_header_details &&
          h.additional_header_details.input_grn &&
          h.additional_header_details.input_grn.linked_id
        ) {
          parentDocNo = h.additional_header_details.input_grn.linked_id;
        }
        // Creditors logic
        if (
          type === "creditors" &&
          h.additional_header_details &&
          h.additional_header_details.input_creditors &&
          h.additional_header_details.input_creditors.linked_id
        ) {
          parentDocNo = h.additional_header_details.input_creditors.linked_id;
        }
        // Debitors logic
        if (
          type === "debitors" &&
          h.additional_header_details &&
          h.additional_header_details.input_debitors &&
          h.additional_header_details.input_debitors.linked_id
        ) {
          parentDocNo = h.additional_header_details.input_debitors.linked_id;
        }
        if (parentDocNo) {
          // Find parent exposure_header_id by document_id
          const { rows: parentRows } = await client.query(
            `SELECT exposure_header_id FROM exposure_headers WHERE document_id = $1 LIMIT 1`,
            [parentDocNo]
          );
          if (parentRows.length > 0) {
            const parentId = parentRows[0].exposure_header_id;
            await client.query(
              `UPDATE exposure_headers SET total_open_amount = total_open_amount + $1, status = 'Open' WHERE exposure_header_id = $2`,
              // [h.total_open_amount || h.total_original_amount, parentId]
                 [Math.abs(h.total_open_amount || h.total_original_amount), parentId]
            );
          }
        }
      }
    }
    // Approve remaining headers
    if (toApprove.length > 0) {
      // 1. Approve all headers (approval_status only, do not change status)
      const { rows: approvedHeaders } = await client.query(
        `UPDATE exposure_headers SET approval_status = 'Approved', approved_by = $1, approval_comment = $2, approved_at = NOW()
         WHERE exposure_header_id = ANY($3::uuid[])
         RETURNING *`,
        [approved_by, approval_comment || null, toApprove]
      );
      results.approved = approvedHeaders;

      // 2. For each approved LC, GRN, creditors, debitors, if rollover is needed, update status as well
      for (const h of approvedHeaders) {
        let parentDocNo = null;
        const type = h.exposure_type ? h.exposure_type.toLowerCase() : "";
        // LC logic
        if (
          type === "lc" &&
          h.additional_header_details &&
          h.additional_header_details.input_letters_of_credit &&
          h.additional_header_details.input_letters_of_credit.linked_po_so_number
        ) {
          parentDocNo = h.additional_header_details.input_letters_of_credit.linked_po_so_number;
        }
        // GRN logic
        if (
          type === "grn" &&
          h.additional_header_details &&
          h.additional_header_details.input_grn &&
          h.additional_header_details.input_grn.linked_id
        ) {
          parentDocNo = h.additional_header_details.input_grn.linked_id;
        }
        // Creditors logic
        if (
          type === "creditors" &&
          h.additional_header_details &&
          h.additional_header_details.input_creditors &&
          h.additional_header_details.input_creditors.linked_id
        ) {
          parentDocNo = h.additional_header_details.input_creditors.linked_id;
        }
        // Debitors logic
        if (
          type === "debitors" &&
          h.additional_header_details &&
          h.additional_header_details.input_debitors &&
          h.additional_header_details.input_debitors.linked_id
        ) {
          parentDocNo = h.additional_header_details.input_debitors.linked_id;
        }
        if (parentDocNo) {
          // Find parent exposure_header_id by document_id
          const { rows: parentRows } = await client.query(
            `SELECT exposure_header_id FROM exposure_headers WHERE document_id = $1 LIMIT 1`,
            [parentDocNo]
          );
          if (parentRows.length > 0) {
            const parentId = parentRows[0].exposure_header_id;
            // Subtract the original amount from parent's open amount
            await client.query(
              `UPDATE exposure_headers SET total_open_amount = total_open_amount - $1, status = 'Rolled' WHERE exposure_header_id = $2`,
              // [h.total_original_amount, parentId]
                [Math.abs(h.total_original_amount), parentId]
            );
            // Set status to 'Rolled' for this header (status change only for rolled)
            await client.query(
              `UPDATE exposure_headers SET status = 'Rolled' WHERE exposure_header_id = $1`,
              [h.exposure_header_id]
            );
            // Log the rollover
            await client.query(
              `INSERT INTO exposure_rollover_log (parent_header_id, child_header_id, rollover_amount, rollover_date, created_at)
               VALUES ($1, $2, $3, CURRENT_DATE, NOW())`,
              // [parentId, h.exposure_header_id, h.total_original_amount]
                 [parentId, h.exposure_header_id, Math.abs(h.total_original_amount)]
            );
            results.rolled.push(h);
          }
        }
      }
    }
    await client.query("COMMIT");
    res.status(200).json({ success: true, ...results });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("approveMultipleExposureHeaders error:", err);
    res.status(500).json({ success: false, error: err.message });
  } finally {
    client.release();
  }
}
