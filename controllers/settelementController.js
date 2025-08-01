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
        AND fb.entity_level_3 = $3
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

module.exports = { filterForwardBookingsForSettlement };
