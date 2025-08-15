// GET /api/exposure/summary
// async function getExposureSummary(req, res) {
//   try {
//     // Get current user session (first/only session)
//     const globalSession = require("../globalSession");
//     const session = globalSession.UserSessions[0];
//     if (!session) {
//       return res.status(404).json({ error: "No active session found" });
//     }
//     const userId = session.userId;
//     // Get user's business unit name
//     const userResult = await pool.query(
//       "SELECT business_unit_name FROM users WHERE id = $1",
//       [userId]
//     );
//     if (!userResult.rows.length) {
//       return res.status(404).json({ error: "User not found" });
//     }
//     const userBu = userResult.rows[0].business_unit_name;
//     if (!userBu) {
//       return res.status(404).json({ error: "User has no business unit assigned" });
//     }
//     // Find all descendant business units using recursive CTE
//     const entityResult = await pool.query(
//       "SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
//       [userBu]
//     );
//     if (!entityResult.rows.length) {
//       return res.status(404).json({ error: "Business unit entity not found" });
//     }
//     const rootEntityId = entityResult.rows[0].entity_id;
//     const descendantsResult = await pool.query(
//       `WITH RECURSIVE descendants AS (
//         SELECT entity_id, entity_name FROM masterEntity WHERE entity_id = $1
//         UNION ALL
//         SELECT me.entity_id, me.entity_name
//         FROM masterEntity me
//         INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
//         INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
//         WHERE (me.approval_status = 'Approved' OR me.approval_status = 'approved') AND (me.is_deleted = false OR me.is_deleted IS NULL)
//       )
//       SELECT entity_name FROM descendants`,
//       [rootEntityId]
//     );
//     const buNames = descendantsResult.rows.map((r) => r.entity_name);
//     if (!buNames.length) {
//       return res.status(404).json({ error: "No accessible business units found" });
//     }

//     // Get exposure_headers for accessible entities
//     const exposuresRes = await pool.query(
//       `SELECT exposure_header_id, company_code, entity, entity1, entity2, entity3, exposure_type, document_id, document_date, counterparty_name, currency, total_original_amount, total_open_amount, value_date
//        FROM exposure_headers
//        WHERE entity = ANY($1)`,
//       [buNames]
//     );
//     const exposures = exposuresRes.rows;

//     // For each exposure, get hedged value and calculate unhedged value
//     const summary = [];
//     for (const exp of exposures) {
//       // Get hedged value for this exposure_header_id
//       const hedgeRes = await pool.query(
//         `SELECT COALESCE(SUM(hedged_amount), 0) AS hedged_value FROM exposure_hedge_links WHERE exposure_header_id = $1`,
//         [exp.exposure_header_id]
//       );
//       const hedgedValue = Number(hedgeRes.rows[0].hedged_value) || 0;
//       const unhedgedValue = (Number(exp.total_open_amount) || 0) - hedgedValue;
//       // Format maturity_month as mm-yy from maturity_date
//       let maturityMonth = null;
//       if (exp.document_date) {
//         const d = new Date(exp.document_date);
//         const mm = String(d.getMonth() + 1).padStart(2, '0');
//         const yy = String(d.getFullYear()).slice(-2);
//         maturityMonth = `${mm}-${yy}`;
//       }
//       summary.push({
//         exposure_header_id: exp.exposure_header_id,
//         company_code: exp.company_code,
//         entity: exp.entity,
//         entity1: exp.entity1,
//         entity2: exp.entity2,
//         entity3: exp.entity3,
//         exposure_type: exp.exposure_type,
//         document_id: exp.document_id,
//         document_date: exp.document_date,
//         // maturity_date: exp.maturity_date,
//         maturity_month: maturityMonth,
//         counterparty_name: exp.counterparty_name,
//         currency: exp.currency,
//         total_original_amount: exp.total_original_amount,
//         total_open_amount: exp.total_open_amount,
//         value_date: exp.value_date,
//         hedged_value: hedgedValue,
//         unhedged_value: unhedgedValue
//       });
//     }
//     res.json({ summary });
//   } catch (err) {
//     console.error("getExposureSummary error:", err);
//     res.status(500).json({ error: "Failed to fetch exposure summary" });
//   }
// }


async function getExposureSummary(req, res) {
  try {
    // Get current user session (first/only session)
    const globalSession = require("../globalSession");
    const session = globalSession.UserSessions[0];
    if (!session) {
      return res.status(404).json({ error: "No active session found" });
    }
    const userId = session.userId;
    // Get user's business unit name
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

    // Get exposure_headers for accessible entities
    const exposuresRes = await pool.query(
      `SELECT exposure_header_id, company_code, entity, entity1, entity2, entity3, exposure_type, document_id, document_date, counterparty_name, currency, total_original_amount, total_open_amount, value_date
       FROM exposure_headers
       WHERE entity = ANY($1)`,
      [buNames]
    );
    const exposures = exposuresRes.rows;

    // --- Optimized: Fetch all hedged values in one query ---
    const exposureIds = exposures.map(e => e.exposure_header_id);
    let hedgeMap = {};
    if (exposureIds.length > 0) {
      const hedgeRes = await pool.query(
        `SELECT exposure_header_id, COALESCE(SUM(hedged_amount), 0) AS hedged_value
         FROM exposure_hedge_links
         WHERE exposure_header_id = ANY($1)
         GROUP BY exposure_header_id`,
        [exposureIds]
      );
      for (const row of hedgeRes.rows) {
        hedgeMap[row.exposure_header_id] = Number(row.hedged_value) || 0;
      }
    }
    // Build summary using hash map for O(1) lookup
    const summary = exposures.map(exp => {
      const hedgedValue = hedgeMap[exp.exposure_header_id] || 0;
      const unhedgedValue = (Number(exp.total_open_amount) || 0) - hedgedValue;
      let maturityMonth = null;
      if (exp.document_date) {
        const d = new Date(exp.document_date);
        const mm = String(d.getMonth() + 1).padStart(2, "0");
        const yy = String(d.getFullYear()).slice(-2);
        maturityMonth = `${mm}-${yy}`;
      }
      return {
        exposure_header_id: exp.exposure_header_id,
        company_code: exp.company_code,
        entity: exp.entity,
        entity1: exp.entity1,
        entity2: exp.entity2,
        entity3: exp.entity3,
        exposure_type: exp.exposure_type,
        document_id: exp.document_id,
        document_date: exp.document_date,
        // maturity_date: exp.maturity_date,
        maturity_month: maturityMonth,
        counterparty_name: exp.counterparty_name,
        currency: exp.currency,
        total_original_amount: exp.total_original_amount,
        total_open_amount: exp.total_open_amount,
        value_date: exp.value_date,
        hedged_value: hedgedValue,
        unhedged_value: unhedgedValue,
      };
    });
    res.json({ summary });
  } catch (err) {
    console.error("getExposureSummary error:", err);
    res.status(500).json({ error: "Failed to fetch exposure summary" });
  }
}

// Bulk set processing_status to 'Delete-approval' for given system_transaction_ids
async function bulkDeleteForwardBookings(req, res) {
  try {
    const { system_transaction_ids } = req.body;
    if (!Array.isArray(system_transaction_ids) || system_transaction_ids.length === 0) {
      return res.status(400).json({ error: "system_transaction_ids (array) required" });
    }
    // Set processing_status to 'Delete-approval' for all provided IDs
    const updateQuery = `UPDATE forward_bookings SET processing_status = 'Delete-approval' WHERE system_transaction_id = ANY($1) RETURNING *`;
    const result = await pool.query(updateQuery, [system_transaction_ids]);
    res.status(200).json({ success: true, updated: result.rows });
  } catch (err) {
    console.error("bulkDeleteForwardBookings error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}
// GET /api/forward/linkedSummaryByCategory
async function getLinkedSummaryByCategory(req, res) {
  try {
    // --- Get allowed business units for user (same as uploadForwardConfirmationsMulti) ---
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

    // 1. Fwd Booking (filter by buNames)
    const fwdBookingRes = await pool.query(
      `SELECT * FROM forward_bookings WHERE entity_level_0 = ANY($1)`,
      [buNames]
    );
    const fwdBooking = fwdBookingRes.rows.map(row => ({
      entity_level_0: row.entity_level_0,
      entity_level_1: row.entity_level_1,
      entity_level_2: row.entity_level_2,
      entity_level_3: row.entity_level_3,
      local_currency: row.local_currency,
      order_type: row.order_type,
      transaction_type: row.transaction_type,
      counterparty: row.counterparty,
      mode_of_delivery: row.mode_of_delivery,
      delivery_period: row.delivery_period,
      add_date: row.add_date,
      settlement_date: row.settlement_date,
      maturity_date: row.maturity_date,
      delivery_date: row.delivery_date,
      currency_pair: row.currency_pair,
      base_currency: row.base_currency,
      quote_currency: row.quote_currency,
      booking_amount: row.booking_amount,
      value_type: row.value_type,
      actual_value_base_currency: row.actual_value_base_currency,
      spot_rate: row.spot_rate,
      forward_points: row.forward_points,
      bank_margin: row.bank_margin,
      total_rate: row.total_rate,
      value_quote_currency: row.value_quote_currency,
      intervening_rate_quote_to_local: row.intervening_rate_quote_to_local,
    }));

    // 2. Fwd Rollovers (join to forward_bookings for entity fields, filter by buNames)
    const rollRes = await pool.query(
      `SELECT r.*, b.entity_level_0, b.entity_level_1, b.entity_level_2, b.entity_level_3
      FROM forward_rollovers r
      LEFT JOIN forward_bookings b ON r.booking_id = b.system_transaction_id
      WHERE b.entity_level_0 = ANY($1)`,
      [buNames]
    );
    const fwdRollovers = rollRes.rows.map(row => ({
      rollover_id: row.rollover_id,
      booking_id: row.booking_id,
      entity_level_0: row.entity_level_0,
      entity_level_1: row.entity_level_1,
      entity_level_2: row.entity_level_2,
      entity_level_3: row.entity_level_3,
      amount_rolled_over: row.amount_rolled_over,
      rollover_date: row.rollover_date,
      original_maturity_date: row.original_maturity_date,
      new_maturity_date: row.new_maturity_date,
      rollover_cost: row.rollover_cost,
    }));

    // 3. Fwd Cancellation (join to forward_bookings for entity fields, filter by buNames)
    const cancelRes = await pool.query(
      `SELECT c.*, b.entity_level_0, b.entity_level_1, b.entity_level_2, b.entity_level_3
      FROM forward_cancellations c
      LEFT JOIN forward_bookings b ON c.booking_id = b.system_transaction_id
      WHERE b.entity_level_0 = ANY($1)`,
      [buNames]
    );
    const fwdCancellation = cancelRes.rows.map(row => ({
      entity_level_0: row.entity_level_0,
      entity_level_1: row.entity_level_1,
      entity_level_2: row.entity_level_2,
      entity_level_3: row.entity_level_3,
      cancellation_id: row.cancellation_id,
      booking_id: row.booking_id,
      amount_cancelled: row.amount_cancelled,
      cancellation_date: row.cancellation_date,
      cancellation_rate: row.cancellation_rate,
      realized_gain_loss: row.realized_gain_loss,
      cancellation_reason: row.cancellation_reason,
    }));

    res.json({
      "Fwd Booking": fwdBooking,
      "Fwd Rollovers": fwdRollovers,
      "Fwd Cancellation": fwdCancellation,
    });
  } catch (err) {
    console.error("getLinkedSummaryByCategory error:", err);
    res.status(500).json({ error: "Failed to fetch linked summary by category" });
  }
}
// Link exposure to hedge booking
async function linkExposureHedge(req, res) {
  try {
    const { exposure_header_id, booking_id, hedged_amount } = req.body;
    if (!exposure_header_id || !booking_id || !hedged_amount) {
      return res.status(400).json({ error: "exposure_header_id, booking_id, and hedged_amount are required" });
    }
    // Upsert logic: if link exists, update; else insert
    const upsertQuery = `
      INSERT INTO exposure_hedge_links (exposure_header_id, booking_id, hedged_amount, is_active)
      VALUES ($1, $2, $3, true)
      ON CONFLICT (exposure_header_id, booking_id)
      DO UPDATE SET hedged_amount = EXCLUDED.hedged_amount, is_active = true
      RETURNING *
    `;
    const values = [exposure_header_id, booking_id, hedged_amount];
    const result = await pool.query(upsertQuery, values);
    // Get booking amount from forward_bookings
    const bookingRes = await pool.query(
      'SELECT Booking_Amount FROM forward_bookings WHERE system_transaction_id = $1',
      [booking_id]
    );
    let bookingAmount = 0;
    if (bookingRes.rows.length && bookingRes.rows[0].Booking_Amount !== null && bookingRes.rows[0].Booking_Amount !== undefined) {
      bookingAmount = Number(bookingRes.rows[0].Booking_Amount) || 0;
    }
    // Sum all previous actions (UTILIZATION, CANCELLATION, ROLLOVER, etc.) for this booking
    const sumRes = await pool.query(
      `SELECT COALESCE(SUM(amount_changed), 0) AS total_utilized FROM forward_booking_ledger WHERE booking_id = $1 AND action_type IN ('UTILIZATION', 'CANCELLATION', 'ROLLOVER')`,
      [booking_id]
    );
    let totalUtilized = 0;
    if (sumRes.rows[0].total_utilized !== null && sumRes.rows[0].total_utilized !== undefined) {
      totalUtilized = Number(sumRes.rows[0].total_utilized) || 0;
    }
    // Calculate new open amount
    let hedgedAmt = 0;
    if (hedged_amount !== null && hedged_amount !== undefined) {
      hedgedAmt = Number(hedged_amount) || 0;
    }
    let newOpenAmount = bookingAmount - totalUtilized;
    // Log to forward_booking_ledger as UTILIZATION
    const ledgerQuery = `
      INSERT INTO forward_booking_ledger (
        booking_id, action_type, action_id, action_date, amount_changed, running_open_amount
      ) VALUES ($1, 'UTILIZATION', $2, CURRENT_DATE, $3, $4)
    `;
    await pool.query(ledgerQuery, [booking_id, exposure_header_id, hedged_amount, newOpenAmount]);
    res.status(200).json({ success: true, link: result.rows[0] });
  } catch (err) {
    console.error("linkExposureHedge error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}
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

// Get all forward_bookings relevant to user's accessible enti
async function getEntityRelevantForwardBookings(req, res) {
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
    // Query all forward_bookings for accessible entities
    const query = `SELECT * FROM forward_bookings WHERE entity_level_0 = ANY($1)`;
    const result = await pool.query(query, [buNames]);
    res.status(200).json({ success: true, data: result.rows });
  } catch (err) {
    console.error("getEntityRelevantForwardBookings error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}

// Change processing_status to Approved or Rejected for a forward booking
async function updateForwardBookingProcessingStatus(req, res) {
  try {
    const { system_transaction_id, processing_status } = req.body;
    if (!system_transaction_id || !['Approved', 'Rejected'].includes(processing_status)) {
      return res.status(400).json({ error: "system_transaction_id and valid processing_status (Approved/Rejected) required" });
    }
    const query = `UPDATE forward_bookings SET processing_status = $1 WHERE system_transaction_id = $2 RETURNING *`;
    const values = [processing_status, system_transaction_id];
    const result = await pool.query(query, values);
    if (result.rowCount > 0) {
      res.status(200).json({ success: true, updated: result.rows[0] });
    } else {
      res.status(404).json({ success: false, error: "No matching forward booking found" });
    }
  } catch (err) {
    console.error("updateForwardBookingProcessingStatus error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}

// Bulk approve/reject forward bookings
// async function bulkUpdateForwardBookingProcessingStatus(req, res) {
//   try {
//     const { system_transaction_ids, processing_status } = req.body;
//     if (!Array.isArray(system_transaction_ids) || system_transaction_ids.length === 0 || !['Approved', 'Rejected'].includes(processing_status)) {
//       return res.status(400).json({ error: "system_transaction_ids (array) and valid processing_status (Approved/Rejected) required" });
//     }
//     let query, values;
//     if (processing_status === 'Approved') {
//       query = `UPDATE forward_bookings SET processing_status = $1, status = 'Confirmed' WHERE system_transaction_id = ANY($2) RETURNING *`;
//       values = [processing_status, system_transaction_ids];
//     } else {
//       query = `UPDATE forward_bookings SET processing_status = $1 WHERE system_transaction_id = ANY($2) RETURNING *`;
//       values = [processing_status, system_transaction_ids];
//     }
//     const result = await pool.query(query, values);
//     if (result.rowCount > 0) {
//       res.status(200).json({ success: true, updated: result.rows });
//     } else {
//       res.status(404).json({ success: false, error: "No matching forward bookings found" });
//     }
//   } catch (err) {
//     console.error("bulkUpdateForwardBookingProcessingStatus error:", err);
//     res.status(500).json({ success: false, error: err.message });
//   }
// }
async function bulkUpdateForwardBookingProcessingStatus(req, res) {
  try {
    const { system_transaction_ids, processing_status } = req.body;
    if (!Array.isArray(system_transaction_ids) || system_transaction_ids.length === 0 || !["Approved", "Rejected"].includes(processing_status)) {
      return res.status(400).json({
        error: "system_transaction_ids (array) and valid processing_status (Approved/Rejected) required",
      });
    }
    // Find which records will be deleted
    const delRes = await pool.query(
      `SELECT system_transaction_id FROM forward_bookings WHERE system_transaction_id = ANY($1) AND processing_status = 'Delete-approval'`,
      [system_transaction_ids]
    );
    const deletedIds = delRes.rows.map(r => r.system_transaction_id);
    // Delete them
    if (deletedIds.length > 0) {
      await pool.query(
        `DELETE FROM forward_bookings WHERE system_transaction_id = ANY($1) AND processing_status = 'Delete-approval'`,
        [deletedIds]
      );
    }
    // The rest are eligible for update
    const updateIds = system_transaction_ids.filter(id => !deletedIds.includes(id));
    let updatedRows = [];
    if (updateIds.length > 0) {
      let query, values;
      if (processing_status === "Approved") {
        query = `UPDATE forward_bookings SET processing_status = $1, status = 'Confirmed' WHERE system_transaction_id = ANY($2) RETURNING *`;
        values = [processing_status, updateIds];
      } else {
        query = `UPDATE forward_bookings SET processing_status = $1 WHERE system_transaction_id = ANY($2) RETURNING *`;
        values = [processing_status, updateIds];
      }
      const result = await pool.query(query, values);
      updatedRows = result.rows;
    }
    if (updatedRows.length > 0 || deletedIds.length > 0) {
      res.status(200).json({ success: true, updated: updatedRows, deleted: deletedIds });
    } else {
      res.status(404).json({ success: false, error: "No matching forward bookings found" });
    }
  } catch (err) {
    console.error("bulkUpdateForwardBookingProcessingStatus error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}

module.exports = {
   getLinkedSummaryByCategory,
  addForwardBookingManualEntry,
  upload,
  getExposureSummary,
  uploadForwardBookingsMulti,
  uploadForwardConfirmationsMulti,
  addForwardConfirmationManualEntry,
  linkExposureHedge,
  getEntityRelevantForwardBookings,
  updateForwardBookingProcessingStatus,
  bulkUpdateForwardBookingProcessingStatus,
  bulkDeleteForwardBookings,
  updateForwardBookingFields,
};

async function updateForwardBookingFields(req, res) {
  try {
    const { system_transaction_id } = req.params;
    const fields = req.body;
    if (!system_transaction_id) {
      return res
        .status(400)
        .json({ error: "system_transaction_id is required in params" });
    }
    if (
      !fields ||
      typeof fields !== "object" ||
      Object.keys(fields).length === 0
    ) {
      return res
        .status(400)
        .json({
          error: "At least one field to update must be provided in body",
        });
    }
    // Check if booking exists
    const bookingRes = await pool.query(
      "SELECT * FROM forward_bookings WHERE system_transaction_id = $1",
      [system_transaction_id]
    );
    if (!bookingRes.rows.length) {
      return res
        .status(404)
        .json({ error: "No matching forward booking found" });
    }
    // Get valid columns for forward_bookings
    const colRes = await pool.query(
      `SELECT column_name FROM information_schema.columns WHERE table_name = 'forward_bookings'`
    );
    const validCols = colRes.rows.map((r) => r.column_name);
    // Filter fields to only valid columns (ignore system_transaction_id)
    const updateFields = {};
    for (const key of Object.keys(fields)) {
      if (validCols.includes(key) && key !== "system_transaction_id") {
        updateFields[key] = fields[key];
      }
    }
    // Always set processing_status to 'pending' on any update
    updateFields["processing_status"] = "pending";
    if (Object.keys(updateFields).length === 0) {
      return res.status(400).json({ error: "No valid fields to update" });
    }
    // Build dynamic SET clause
    const keys = Object.keys(updateFields);
    const setClause = keys.map((k, i) => `${k} = $${i + 1}`).join(", ");
    const values = keys.map((k) => updateFields[k]);
    values.push(system_transaction_id);
    const updateQuery = `UPDATE forward_bookings SET ${setClause} WHERE system_transaction_id = $${
      keys.length + 1
    } RETURNING *`;
    const result = await pool.query(updateQuery, values);
    if (result.rowCount > 0) {
      res.status(200).json({ success: true, updated: result.rows[0] });
    } else {
      return res.status(404).json({ error: "No matching forward booking found" });
    }
  } catch (err) {
    console.error("updateForwardBookingFields error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}
