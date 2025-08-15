const path = require("path");
const fs = require("fs");
const csv = require("csv-parser");
const XLSX = require("xlsx");
const { v4: uuidv4 } = require("uuid");
const { pool } = require("../db");
const globalSession = require("../globalSession");

// POST /api/mtm/upload - Multi-file MTM uploader
async function uploadMTMFiles(req, res) {
  if (!req.files || req.files.length === 0) {
    return res.status(400).json({ error: "No files uploaded" });
  }
  const session = globalSession.UserSessions[0];
  if (!session) {
    req.files.forEach((f) => fs.unlinkSync(f.path));
    return res.status(404).json({ error: "No active session found" });
  }
  // Get allowed business units for user
  const userId = session.userId;
  let buNames = [];
  try {
    const userResult = await pool.query(
      "SELECT business_unit_name FROM users WHERE id = $1",
      [userId]
    );
    if (!userResult.rows.length) {
      req.files.forEach((f) => fs.unlinkSync(f.path));
      return res.status(404).json({ error: "User not found" });
    }
    const userBu = userResult.rows[0].business_unit_name;
    const entityResult = await pool.query(
      "SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
      [userBu]
    );
    if (!entityResult.rows.length) {
      req.files.forEach((f) => fs.unlinkSync(f.path));
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
      req.files.forEach((f) => fs.unlinkSync(f.path));
      return res
        .status(404)
        .json({ error: "No accessible business units found" });
    }
  } catch (err) {
    req.files.forEach((f) => fs.unlinkSync(f.path));
    return res
      .status(500)
      .json({ error: "Failed to fetch allowed business units" });
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
        rows = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], {
          defval: null,
        });
      } else {
        throw new Error("Unsupported file type");
      }
    } catch (err) {
      results.push({
        filename: file.originalname,
        error: "Failed to parse file: " + err.message,
      });
      fs.unlinkSync(file.path);
      continue;
    }
    if (!Array.isArray(rows) || rows.length === 0) {
      results.push({ filename: file.originalname, error: "No data to upload" });
      fs.unlinkSync(file.path);
      continue;
    }
    // Prepare for batch insert
    const batchId = uuidv4();
    let fileError = null;
    try {
      // Lookup all booking_ids and details in advance
      const refIds = rows.map((r) => r.internal_reference_id).filter(Boolean);
      let bookingMap = {};
      let bookingDetailsMap = {};
      let bookingIdList = [];
      if (refIds.length > 0) {
        const bookingRes = await pool.query(
          `SELECT system_transaction_id, internal_reference_id, order_type, booking_amount, maturity_date, total_rate, currency_pair FROM forward_bookings WHERE internal_reference_id = ANY($1)`,
          [refIds]
        );
        for (const row of bookingRes.rows) {
          bookingMap[row.internal_reference_id] = row.system_transaction_id;
          bookingDetailsMap[row.internal_reference_id] = row;
          bookingIdList.push(row.system_transaction_id);
        }
      }
      // Bulk fetch latest ledger entries for all booking_ids
      let ledgerMap = {};
      if (bookingIdList.length > 0) {
        const ledgerRes = await pool.query(
          `SELECT booking_id, running_open_amount, ledger_sequence FROM forward_booking_ledger WHERE booking_id = ANY($1)`,
          [bookingIdList]
        );
        // Find latest ledger_sequence for each booking_id
        for (const row of ledgerRes.rows) {
          if (!ledgerMap[row.booking_id] || row.ledger_sequence > ledgerMap[row.booking_id].ledger_sequence) {
            ledgerMap[row.booking_id] = {
              running_open_amount: row.running_open_amount,
              ledger_sequence: row.ledger_sequence
            };
          }
        }
      }
      let validRows = [];
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        // Business unit check
        if (!buNames.includes(row.entity)) {
          throw new Error(
            `Business unit not allowed: ${row.entity} (row ${i + 1})`
          );
        }
        // Booking ID lookup
        const bookingId = bookingMap[row.internal_reference_id];
        if (!bookingId) {
          throw new Error(
            `Booking not found for internal_reference_id: ${
              row.internal_reference_id
            } (row ${i + 1})`
          );
        }
        // Reconciliation check
        const booking = bookingDetailsMap[row.internal_reference_id];
        if (!booking) {
          throw new Error(
            `Booking details not found for internal_reference_id: ${
              row.internal_reference_id
            } (row ${i + 1})`
          );
        }
        // Notional amount check: use latest running_open_amount from ledger, else booking_amount
        let openAmount = booking.booking_amount;
        if (ledgerMap[bookingId]) {
          openAmount = ledgerMap[bookingId].running_open_amount;
        }
        let mismatchFields = [];
        if (String(row.buy_sell).trim() !== String(booking.order_type).trim()) {
          mismatchFields.push('buy_sell/order_type');
        }
        if (Number(row.notional_amount) !== Number(openAmount)) {
          mismatchFields.push('notional_amount/open_amount');
        }
        if (Number(row.contract_rate) !== Number(booking.total_rate)) {
          mismatchFields.push('contract_rate/total_rate');
        }
        if (String(row.currency_pair).trim() !== String(booking.currency_pair).trim()) {
          mismatchFields.push('currency_pair');
        }
        if (mismatchFields.length > 0) {
          throw new Error(`Reconciliation failed for internal_reference_id: ${row.internal_reference_id} (row ${i + 1}). Mismatched fields: ${mismatchFields.join(', ')}`);
        }
        // Calculate mtm_value: (mtm_rate - contract_rate) * notional_amount
        const mtmValue =
          (Number(row.mtm_rate) - Number(row.contract_rate)) *
          Number(row.notional_amount);
        // Calculate days_to_maturity: difference in days between maturity_date and deal_date
        let daysToMaturity = null;
        try {
          const dealDate = new Date(row.deal_date);
          const maturityDate = new Date(row.maturity_date);
          daysToMaturity = Math.ceil(
            (maturityDate - dealDate) / (1000 * 60 * 60 * 24)
          );
        } catch (e) {
          daysToMaturity = row.days_to_maturity || null;
        }
        validRows.push({
          mtm_id: uuidv4(),
          booking_id: bookingId,
          deal_date: row.deal_date,
          maturity_date: row.maturity_date,
          currency_pair: row.currency_pair,
          buy_sell: row.buy_sell,
          notional_amount: row.notional_amount,
          contract_rate: row.contract_rate,
          mtm_rate: row.mtm_rate,
          mtm_value: mtmValue,
          days_to_maturity: daysToMaturity,
          status: row.status || "pending",
          internal_reference_id: row.internal_reference_id,
          entity: row.entity,
        });
      }
      // Bulk insert using transaction
      await pool.query("BEGIN");
      if (validRows.length > 0) {
        const insertQuery =
          `INSERT INTO forward_mtm (
          mtm_id, booking_id, deal_date, maturity_date, currency_pair, buy_sell, notional_amount, contract_rate, mtm_rate, mtm_value, days_to_maturity, status, internal_reference_id, entity
        ) VALUES ` +
          validRows
            .map(
              (_, idx) =>
                `($${idx * 14 + 1},$${idx * 14 + 2},$${idx * 14 + 3},$${
                  idx * 14 + 4
                },$${idx * 14 + 5},$${idx * 14 + 6},$${idx * 14 + 7},$${
                  idx * 14 + 8
                },$${idx * 14 + 9},$${idx * 14 + 10},$${idx * 14 + 11},$${
                  idx * 14 + 12
                },$${idx * 14 + 13},$${idx * 14 + 14})`
            )
            .join(",") +
          " RETURNING mtm_id";
        const insertValues = validRows.flatMap((r) => [
          r.mtm_id,
          r.booking_id,
          r.deal_date,
          r.maturity_date,
          r.currency_pair,
          r.buy_sell,
          r.notional_amount,
          r.contract_rate,
          r.mtm_rate,
          r.mtm_value,
          r.days_to_maturity,
          r.status,
          r.internal_reference_id,
          r.entity,
        ]);
        await pool.query(insertQuery, insertValues);
      }
      await pool.query("COMMIT");
      results.push({ filename: file.originalname, inserted: validRows.length });
      fs.unlinkSync(file.path);
    } catch (err) {
      await pool.query("ROLLBACK");
      fileError = err.message || "Failed to insert data";
      results.push({ filename: file.originalname, error: fileError });
      fs.unlinkSync(file.path);
      continue;
    }
  }
  const hasErrors = results.some(r => r.error);
  res.json({ success: !hasErrors, results });
}
  
// GET /api/mtm - Fetch MTM data for user's allowed business units
async function getMTMData(req, res) {
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
      return res
        .status(404)
        .json({ error: "No accessible business units found" });
    }
  } catch (err) {
    return res
      .status(500)
      .json({ error: "Failed to fetch allowed business units" });
  }
  // Fetch MTM data for allowed business units
  try {
    const mtmRes = await pool.query(
      `SELECT * FROM forward_mtm WHERE entity = ANY($1)`,
      [buNames]
    );
    res.json({ data: mtmRes.rows });
  } catch (err) {
    res.status(500).json({ error: "Failed to fetch MTM data" });
  }
}

module.exports = {
  uploadMTMFiles,
  getMTMData,
};
