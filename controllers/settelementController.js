// Helper to deactivate exposure_hedge_links for booking_ids
async function deactivateExposureHedgeLinks(booking_ids, pool) {
  if (!Array.isArray(booking_ids) || booking_ids.length === 0) return;
  await pool.query(
    `UPDATE exposure_hedge_links SET is_active = false WHERE booking_id = ANY($1)`,
    [booking_ids]
  );
}
// POST /api/settlement/forward-cancellations
async function createForwardCancellations(req, res) {
  try {
    const {
      booking_ids, // array of UUIDs
      amount_cancelled,
      cancellation_date,
      cancellation_rate,
      realized_gain_loss,
      cancellation_reason
    } = req.body;
    if (!Array.isArray(booking_ids) || booking_ids.length === 0 || !amount_cancelled || !cancellation_date || !cancellation_rate) {
      return res.status(400).json({ error: "booking_ids (array), amount_cancelled, cancellation_date, and cancellation_rate are required" });
    }

    // Insert a row for each booking_id
    const insertQuery = `
      INSERT INTO forward_cancellations (
        booking_id, amount_cancelled, cancellation_date, cancellation_rate, realized_gain_loss, cancellation_reason
      ) VALUES 
        ${booking_ids.map((_, i) => `($${i * 6 + 1}, $${i * 6 + 2}, $${i * 6 + 3}, $${i * 6 + 4}, $${i * 6 + 5}, $${i * 6 + 6})`).join(",\n        ")}
      RETURNING *
    `;
    const insertValues = booking_ids.flatMap(bid => [
      bid,
      amount_cancelled,
      cancellation_date,
      cancellation_rate,
      realized_gain_loss,
      cancellation_reason
    ]);
    const insertResult = await pool.query(insertQuery, insertValues);

    // Update status in forward_bookings for all booking_ids
    await pool.query(
      `UPDATE forward_bookings SET status = 'Cancelled' WHERE system_transaction_id = ANY($1)`,
      [booking_ids]
    );

    // Set is_active = false in exposure_hedge_links for all booking_ids
    await deactivateExposureHedgeLinks(booking_ids, pool);

    res.status(201).json({ success: true, inserted: insertResult.rows.length, cancellations: insertResult.rows });
  } catch (err) {
    console.error("createForwardCancellations error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}

async function getExposuresByBookingIds(req, res) {
  try {
    const { system_transaction_ids } = req.body;
    if (!Array.isArray(system_transaction_ids) || system_transaction_ids.length === 0) {
      return res.status(400).json({ error: "system_transaction_ids (array) required" });
    }
    // Get allowed business units for user
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
        "SELECT entity_id FROM masterentity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
        [userBu]
      );
      if (!entityResult.rows.length) {
        return res.status(404).json({ error: "Business unit entity not found" });
      }
      const rootEntityId = entityResult.rows[0].entity_id;
      const descendantsResult = await pool.query(
        `WITH RECURSIVE descendants AS (
          SELECT entity_id, entity_name FROM masterentity WHERE entity_id = $1
          UNION ALL
          SELECT me.entity_id, me.entity_name
          FROM masterentity me
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
    // Find all exposure_header_ids linked to these booking_ids, filter by allowed entities
    const query = `
      SELECT
        ehl.exposure_header_id,
        eh.document_id,
        eh.exposure_type,
        eh.currency,
        eh.total_open_amount,
        eh.total_original_amount,
        eh.document_date
      FROM exposure_hedge_links ehl
      JOIN exposure_headers eh ON ehl.exposure_header_id = eh.exposure_header_id
      WHERE ehl.booking_id = ANY($1)
        AND (ehl.is_active = true OR ehl.is_active IS NULL)
        AND eh.entity = ANY($2)
    `;
    const result = await pool.query(query, [system_transaction_ids, buNames]);
    res.status(200).json({ success: true, data: result.rows });
  } catch (err) {
    console.error("getExposuresByBookingIds error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}
// GET API: List forward bookings (selected fields, buNames check)
async function getForwardBookingList(req, res) {
  try {
    // Get allowed business units for user
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
        return res
          .status(404)
          .json({ error: "User has no business unit assigned" });
      }
      const entityResult = await pool.query(
        "SELECT entity_id FROM masterentity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
        [userBu]
      );
      if (!entityResult.rows.length) {
        return res
          .status(404)
          .json({ error: "Business unit entity not found" });
      }
      const rootEntityId = entityResult.rows[0].entity_id;
      const descendantsResult = await pool.query(
        `WITH RECURSIVE descendants AS (
          SELECT entity_id, entity_name FROM masterentity WHERE entity_id = $1
          UNION ALL
          SELECT me.entity_id, me.entity_name
          FROM masterentity me
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
      console.error("Error fetching allowed business units:", err);
      return res
        .status(500)
        .json({ error: "Failed to fetch allowed business units" });
    }
    // Query forward_bookings for allowed BUs, select only required fields
    const query = `
  SELECT 
    system_transaction_id,
    internal_reference_id,
    currency_pair,
    booking_amount,
    spot_rate,
    maturity_date,
    order_type,
    counterparty
  FROM forward_bookings
  WHERE entity_level_0 = ANY($1)
    AND status NOT IN ('Cancelled', 'Pending Confirmation')
`;
    const result = await pool.query(query, [buNames]);
    res.status(200).json({ success: true, data: result.rows });
  } catch (err) {
    console.error("getForwardBookingList error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}

// API: Get all forward_bookings for allowed entities and currency (no exposure linkage)
async function getForwardBookingsByEntityAndCurrency(req, res) {
  try {
    const { entity, currency } = req.body;
    if (!entity || !currency) {
      return res.status(400).json({ error: "entity and currency are required" });
    }
    // Get allowed business units for user
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
    // Query: all forward_bookings for allowed BUs, currency, and entity (any level)
    const query = `
      SELECT 
        fb.internal_reference_id AS "Forward Ref",
        COALESCE(
          (
            SELECT running_open_amount 
            FROM forward_booking_ledger fbl 
            WHERE fbl.booking_id = fb.system_transaction_id 
            ORDER BY ledger_sequence DESC LIMIT 1
          ),
          fb.booking_amount
        ) AS "Outstanding Amount",
        fb.spot_rate AS "Spot",
        fb.total_rate AS "Fwd",
        fb.bank_margin AS "Margin",
        fb.counterparty_dealer AS "Bank Name",
        fb.maturity_date AS "Maturity"
      FROM forward_bookings fb
      WHERE fb.quote_currency = $1
        AND (
          fb.entity_level_0 = $2
          OR fb.entity_level_1 = $2
          OR fb.entity_level_2 = $2
          OR fb.entity_level_3 = $2
        )
        AND fb.status = 'Confirmed'
        AND (
          fb.entity_level_0 = ANY($3)
          OR fb.entity_level_1 = ANY($3)
          OR fb.entity_level_2 = ANY($3)
          OR fb.entity_level_3 = ANY($3)
        )
    `;
    const values = [currency, entity, buNames];
    const result = await pool.query(query, values);
    res.status(200).json({ success: true, data: result.rows });
  } catch (err) {
    console.error("getForwardBookingsByEntityAndCurrency error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}
const { pool } = require("../db");

// API: Filter forward_bookings by exposure_header_ids, entity, and currency
async function filterForwardBookingsForSettlement(req, res) {
  try {
    const { exposure_header_ids, entity, currency } = req.body;
    if (!Array.isArray(exposure_header_ids) || exposure_header_ids.length === 0 || !entity || !currency) {
      return res.status(400).json({ error: "exposure_header_ids (array), entity, and currency are required" });
    }
    // Get allowed business units for user
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
    // Query: join exposure_hedge_links and forward_bookings with filters, restrict to allowed BUs
    const query = `
      SELECT 
        fb.internal_reference_id AS "Forward Ref",
        COALESCE(
          (
            SELECT running_open_amount 
            FROM forward_booking_ledger fbl 
            WHERE fbl.booking_id = fb.system_transaction_id 
            ORDER BY ledger_sequence DESC LIMIT 1
          ),
          fb.booking_amount
        ) AS "Outstanding Amount",
        fb.spot_rate AS "Spot",
        fb.total_rate AS "Fwd",
        fb.bank_margin AS "Margin",
        fb.counterparty_dealer AS "Bank Name",
        fb.maturity_date AS "Maturity"
      FROM exposure_hedge_links ehl
      JOIN forward_bookings fb ON ehl.booking_id = fb.system_transaction_id
      WHERE ehl.exposure_header_id = ANY($1)
        AND fb.quote_currency = $2
        AND (
          fb.entity_level_0 = $3
          OR fb.entity_level_1 = $3
          OR fb.entity_level_2 = $3
          OR fb.entity_level_3 = $3
        )
        AND fb.status = 'Confirmed'
        AND (
          fb.entity_level_0 = ANY($4)
          OR fb.entity_level_1 = ANY($4)
          OR fb.entity_level_2 = ANY($4)
          OR fb.entity_level_3 = ANY($4)
        )
    `;
    const values = [exposure_header_ids, currency, entity, buNames];
    const result = await pool.query(query, values);
    res.status(200).json({ success: true, data: result.rows });
  } catch (err) {
    console.error("filterForwardBookingsForSettlement error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}

module.exports = {
  filterForwardBookingsForSettlement,
  getForwardBookingsByEntityAndCurrency,
  getForwardBookingList,
  getExposuresByBookingIds,
  createForwardCancellations
};
