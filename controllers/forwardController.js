// Multi-file upload for forward confirmations (CSV/Excel) - UPDATE existing records
async function uploadForwardConfirmationsMulti(req, res) {
  if (!req.files || req.files.length === 0) {
    return res.status(400).json({ error: "No files uploaded" });
  }
  const globalSession = require("../globalSession");
  const session = globalSession.UserSessions[0];
  if (!session) {
    req.files.forEach(f => fs.unlinkSync(f.path));
    return res.status(404).json({ error: "No active session found" });
  }
  const userId = session.userId;
  let buNames = [];
  try {
    const userResult = await pool.query(
      "SELECT business_unit_name FROM users WHERE id = $1",
      [userId]
    );
    if (!userResult.rows.length) {
      req.files.forEach(f => fs.unlinkSync(f.path));
      return res.status(404).json({ error: "User not found" });
    }
    const userBu = userResult.rows[0].business_unit_name;
    if (!userBu) {
      req.files.forEach(f => fs.unlinkSync(f.path));
      return res.status(404).json({ error: "User has no business unit assigned" });
    }
    const entityResult = await pool.query(
      "SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
      [userBu]
    );
    if (!entityResult.rows.length) {
      req.files.forEach(f => fs.unlinkSync(f.path));
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
    buNames = descendantsResult.rows.map((r) => r.entity_name);
    if (!buNames.length) {
      req.files.forEach(f => fs.unlinkSync(f.path));
      return res.status(404).json({ error: "No accessible business units found" });
    }
  } catch (err) {
    req.files.forEach(f => fs.unlinkSync(f.path));
    console.error("Error fetching allowed business units:", err);
    return res.status(500).json({ error: "Failed to fetch allowed business units" });
  }

  // Process each file
  const results = [];
  for (const file of req.files) {
    let rows = [];
    let fileType = path.extname(file.originalname).toLowerCase();
    // Parse file
    try {
      if (fileType === ".csv") {
        rows = await new Promise((resolve, reject) => {
          const arr = [];
          fs.createReadStream(file.path)
            .pipe(csv())
            .on("data", (row) => arr.push(row))
            .on("end", () => resolve(arr))
            .on("error", (err) => reject(err));
        });
      } else if (fileType === ".xls" || fileType === ".xlsx") {
        const workbook = XLSX.readFile(file.path);
        const sheetName = workbook.SheetNames[0];
        rows = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], { defval: null });
      } else {
        throw new Error("Unsupported file type");
      }
    } catch (err) {
      results.push({ filename: file.originalname, error: "Failed to parse file: " + err.message });
      fs.unlinkSync(file.path);
      continue;
    }
    if (!Array.isArray(rows) || rows.length === 0) {
      results.push({ filename: file.originalname, error: "No data to upload" });
      fs.unlinkSync(file.path);
      continue;
    }
    // Update rows
    let successCount = 0;
    let errorRows = [];
    let invalidRows = [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (!buNames.includes(r.entity_level_0)) {
        invalidRows.push({ row: i + 1, entity: r.entity_level_0 });
        continue;
      }
      try {
        // Only update if record exists and status is 'Pending Confirmation'
        const updateQuery = `UPDATE forward_bookings SET
          status = 'Confirmed',
          bank_transaction_id = $1,
          swift_unique_id = $2,
          bank_confirmation_date = $3,
          processing_status = 'pending'
        WHERE internal_reference_id = $4 AND status = 'Pending Confirmation' AND entity_level_0 = $5
        RETURNING *`;
        const updateValues = [
          r.bank_transaction_id,
          r.swift_unique_id,
          r.bank_confirmation_date,
          r.internal_reference_id,
          r.entity_level_0
        ];
        const result = await pool.query(updateQuery, updateValues);
        if (result.rowCount > 0) {
          successCount++;
        } else {
          errorRows.push({ row: i + 1, error: "No matching record found or already confirmed" });
        }
      } catch (err) {
        errorRows.push({ row: i + 1, error: err.message });
      }
    }
    results.push({ filename: file.originalname, updated: successCount, errors: errorRows, invalidRows });
    fs.unlinkSync(file.path);
  }
  res.status(200).json({ success: true, results });
}
// Manual entry for forward confirmations - UPDATE existing record
async function addForwardConfirmationManualEntry(req, res) {
  try {
    const globalSession = require("../globalSession");
    const session = globalSession.UserSessions[0];
    if (!session) {
      return res.status(404).json({ error: "No active session found" });
    }
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
      buNames = descendantsResult.rows.map((r) => r.entity_name);
      if (!buNames.length) {
        return res.status(404).json({ error: "No accessible business units found" });
      }
    } catch (err) {
      console.error("Error fetching allowed business units:", err);
      return res.status(500).json({ error: "Failed to fetch allowed business units" });
    }

    const {
      internal_reference_id,
      entity_level_0,
      bank_transaction_id,
      swift_unique_id,
      bank_confirmation_date
    } = req.body;

    if (!buNames.includes(entity_level_0)) {
      return res.status(403).json({ error: "You do not have access to this business unit" });
    }

    // Only update if record exists and status is 'Pending Confirmation'
    const updateQuery = `UPDATE forward_bookings SET
      status = 'Confirmed',
      bank_transaction_id = $1,
      swift_unique_id = $2,
      bank_confirmation_date = $3,
      processing_status = 'pending'
    WHERE internal_reference_id = $4 AND status = 'Pending Confirmation' AND entity_level_0 = $5
    RETURNING *`;
    const updateValues = [
      bank_transaction_id,
      swift_unique_id,
      bank_confirmation_date,
      internal_reference_id,
      entity_level_0
    ];
    const result = await pool.query(updateQuery, updateValues);
    if (result.rowCount > 0) {
      res.status(200).json({ success: true, updated: result.rows[0] });
    } else {
      res.status(404).json({ success: false, error: "No matching record found or already confirmed" });
    }
  } catch (err) {
    console.error("addForwardConfirmationManualEntry error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}
    
const path = require("path");
const fs = require("fs");
const csv = require("csv-parser");
const XLSX = require("xlsx");
const multer = require("multer");
const upload = multer({ dest: path.join(__dirname, "../uploads") });
// Multi-file upload for forward bookings (CSV/Excel)
async function uploadForwardBookingsMulti(req, res) {
  if (!req.files || req.files.length === 0) {
    return res.status(400).json({ error: "No files uploaded" });
  }
  const globalSession = require("../globalSession");
  const session = globalSession.UserSessions[0];
  if (!session) {
    // Clean up all files
    req.files.forEach(f => fs.unlinkSync(f.path));
    return res.status(404).json({ error: "No active session found" });
  }
  const userId = session.userId;
  let buNames = [];
  try {
    const userResult = await pool.query(
      "SELECT business_unit_name FROM users WHERE id = $1",
      [userId]
    );
    if (!userResult.rows.length) {
      req.files.forEach(f => fs.unlinkSync(f.path));
      return res.status(404).json({ error: "User not found" });
    }
    const userBu = userResult.rows[0].business_unit_name;
    if (!userBu) {
      req.files.forEach(f => fs.unlinkSync(f.path));
      return res.status(404).json({ error: "User has no business unit assigned" });
    }
    const entityResult = await pool.query(
      "SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
      [userBu]
    );
    if (!entityResult.rows.length) {
      req.files.forEach(f => fs.unlinkSync(f.path));
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
    buNames = descendantsResult.rows.map((r) => r.entity_name);
    if (!buNames.length) {
      req.files.forEach(f => fs.unlinkSync(f.path));
      return res.status(404).json({ error: "No accessible business units found" });
    }
  } catch (err) {
    req.files.forEach(f => fs.unlinkSync(f.path));
    console.error("Error fetching allowed business units:", err);
    return res.status(500).json({ error: "Failed to fetch allowed business units" });
  }

  // Process each file
  const results = [];
  for (const file of req.files) {
    let rows = [];
    let fileType = path.extname(file.originalname).toLowerCase();
    // Parse file
    try {
      if (fileType === ".csv") {
        // Parse CSV
        rows = await new Promise((resolve, reject) => {
          const arr = [];
          fs.createReadStream(file.path)
            .pipe(csv())
            .on("data", (row) => arr.push(row))
            .on("end", () => resolve(arr))
            .on("error", (err) => reject(err));
        });
      } else if (fileType === ".xls" || fileType === ".xlsx") {
        // Parse Excel
        const workbook = XLSX.readFile(file.path);
        const sheetName = workbook.SheetNames[0];
        rows = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], { defval: null });
      } else {
        throw new Error("Unsupported file type");
      }
    } catch (err) {
      results.push({ filename: file.originalname, error: "Failed to parse file: " + err.message });
      fs.unlinkSync(file.path);
      continue;
    }
    if (!Array.isArray(rows) || rows.length === 0) {
      results.push({ filename: file.originalname, error: "No data to upload" });
      fs.unlinkSync(file.path);
      continue;
    }
    // Insert rows
    let successCount = 0;
    let errorRows = [];
    let invalidRows = [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      if (!buNames.includes(r.entity_level_0)) {
        invalidRows.push({ row: i + 1, entity: r.entity_level_0 });
        continue;
      }
      try {
        const query = `INSERT INTO forward_bookings (
          internal_reference_id,
          entity_level_0,
          entity_level_1,
          entity_level_2,
          entity_level_3,
          local_currency,
          order_type,
          transaction_type,
          counterparty,
          mode_of_delivery,
          delivery_period,
          add_date,
          settlement_date,
          maturity_date,
          delivery_date,
          currency_pair,
          base_currency,
          quote_currency,
          booking_amount,
          value_type,
          actual_value_base_currency,
          spot_rate,
          forward_points,
          bank_margin,
          total_rate,
          value_quote_currency,
          intervening_rate_quote_to_local,
          value_local_currency,
          internal_dealer,
          counterparty_dealer,
          remarks,
          narration,
          transaction_timestamp,
          processing_status
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34
        )`;
        const values = [
          r.internal_reference_id,
          r.entity_level_0,
          r.entity_level_1,
          r.entity_level_2,
          r.entity_level_3,
          r.local_currency,
          r.order_type,
          r.transaction_type,
          r.counterparty,
          r.mode_of_delivery,
          r.delivery_period,
          r.add_date,
          r.settlement_date,
          r.maturity_date,
          r.delivery_date,
          r.currency_pair,
          r.base_currency,
          r.quote_currency,
          r.booking_amount,
          r.value_type,
          r.actual_value_base_currency,
          r.spot_rate,
          r.forward_points,
          r.bank_margin,
          r.total_rate,
          r.value_quote_currency,
          r.intervening_rate_quote_to_local,
          r.value_local_currency,
          r.internal_dealer,
          r.counterparty_dealer,
          r.remarks,
          r.narration,
          r.transaction_timestamp,
          'pending'
        ];
        await pool.query(query, values);
        successCount++;
      } catch (err) {
        errorRows.push({ row: i + 1, error: err.message });
      }
    }
    results.push({ filename: file.originalname, inserted: successCount, errors: errorRows, invalidRows });
    fs.unlinkSync(file.path);
  }
  res.status(200).json({ success: true, results });
}
// Manual entry for forward bookings

// const globalSession = require("../globalSession");
const { pool } = require("../db");
// const csv = require("csv-parser");
// const multer = require("multer");


async function addForwardBookingManualEntry(req, res) {
  try {
    // --- Get allowed business units for user ---
    const globalSession = require("../globalSession");
    const session = globalSession.UserSessions[0];
    if (!session) {
      return res.status(404).json({ error: "No active session found" });
    }
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
      buNames = descendantsResult.rows.map((r) => r.entity_name);
      if (!buNames.length) {
        return res.status(404).json({ error: "No accessible business units found" });
      }
    } catch (err) {
      console.error("Error fetching allowed business units:", err);
      return res.status(500).json({ error: "Failed to fetch allowed business units" });
    }

    const {
      internal_reference_id,
      entity_level_0,
      entity_level_1,
      entity_level_2,
      entity_level_3,
      local_currency,
      order_type,
      transaction_type,
      counterparty,
      mode_of_delivery,
      delivery_period,
      add_date,
      settlement_date,
      maturity_date,
      delivery_date,
      currency_pair,
      base_currency,
      quote_currency,
      booking_amount,
      value_type,
      actual_value_base_currency,
      spot_rate,
      forward_points,
      bank_margin,
      total_rate,
      value_quote_currency,
      intervening_rate_quote_to_local,
      value_local_currency,
      internal_dealer,
      counterparty_dealer,
      remarks,
      narration,
      transaction_timestamp
    } = req.body;

    // Check entity_level_0
    if (!buNames.includes(entity_level_0)) {
      return res.status(403).json({ error: "You do not have access to this business unit" });
    }

    const query = `INSERT INTO forward_bookings (
      internal_reference_id,
      entity_level_0,
      entity_level_1,
      entity_level_2,
      entity_level_3,
      local_currency,
      order_type,
      transaction_type,
      counterparty,
      mode_of_delivery,
      delivery_period,
      add_date,
      settlement_date,
      maturity_date,
      delivery_date,
      currency_pair,
      base_currency,
      quote_currency,
      booking_amount,
      value_type,
      actual_value_base_currency,
      spot_rate,
      forward_points,
      bank_margin,
      total_rate,
      value_quote_currency,
      intervening_rate_quote_to_local,
      value_local_currency,
      internal_dealer,
      counterparty_dealer,
      remarks,
      narration,
      transaction_timestamp,
      processing_status
    ) VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34
    ) RETURNING *`;
    const values = [
      internal_reference_id,
      entity_level_0,
      entity_level_1,
      entity_level_2,
      entity_level_3,
      local_currency,
      order_type,
      transaction_type,
      counterparty,
      mode_of_delivery,
      delivery_period,
      add_date,
      settlement_date,
      maturity_date,
      delivery_date,
      currency_pair,
      base_currency,
      quote_currency,
      booking_amount,
      value_type,
      actual_value_base_currency,
      spot_rate,
      forward_points,
      bank_margin,
      total_rate,
      value_quote_currency,
      intervening_rate_quote_to_local,
      value_local_currency,
      internal_dealer,
      counterparty_dealer,
      remarks,
      narration,
      transaction_timestamp,
      'pending'
    ];
    const result = await pool.query(query, values);
    res.status(201).json({ success: true, data: result.rows[0] });
  } catch (err) {
    console.error("addForwardBookingManualEntry error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}

module.exports = {
  addForwardBookingManualEntry,
  upload,
  uploadForwardBookingsMulti,
  uploadForwardConfirmationsMulti,
  addForwardConfirmationManualEntry,
};