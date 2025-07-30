// Manual entry for forward bookings

// const globalSession = require("../globalSession");
const { pool } = require("../db");
// const csv = require("csv-parser");
const multer = require("multer");


async function addForwardBookingManualEntry(req, res) {
  try {
    const {
      // system_transaction_id,
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
};