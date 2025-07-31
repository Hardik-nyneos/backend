// GET /api/forwardDash/active-forwards
exports.getActiveForwardsCount = async (req, res) => {
  try {
    const now = new Date();
    const result = await pool.query(
      'SELECT COUNT(*) AS count FROM forward_bookings WHERE maturity_date > $1',
      [now]
    );
    res.json({ ActiveForward: result.rows[0].count });
  } catch (err) {
    res.status(500).json({ error: 'Error fetching active forwards count', details: err.message });
  }
};
// GET /api/forwardDash/recent-trades-dashboard
exports.getRecentTradesDashboard = async (req, res) => {
  try {
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
    const now = new Date();
    const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    const result = await pool.query(
      `SELECT booking_amount, quote_currency, currency_pair, counterparty_dealer, maturity_date
       FROM forward_bookings
       WHERE maturity_date >= $1 AND maturity_date <= $2`,
      [sevenDaysAgo, now]
    );
    let totalTrades = 0;
    let totalVolume = 0;
    const bankMap = {};
    for (const row of result.rows) {
      const amount = Number(row.booking_amount) || 0;
      const currency = (row.quote_currency || '').toUpperCase();
      const rate = rates[currency] || 1.0;
      const amountUsd = amount * rate;
      totalTrades++;
      totalVolume += amountUsd;
      const pair = ((row.currency_pair || '').trim() + ' Forward').trim();
      const bank = row.counterparty_dealer || 'Unknown Bank';
      const key = `${bank}__${pair}`;
      if (!bankMap[key]) {
        bankMap[key] = { pair, bank, amount: 0 };
      }
      bankMap[key].amount += amountUsd;
    }
    // Format helpers
    const formatAmount = (amt) => {
      if (amt >= 1e6) return `$${(amt / 1e6).toFixed(1)}M`;
      if (amt >= 1e3) return `$${(amt / 1e3).toFixed(1)}K`;
      return `$${amt.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
    };
    const banks = Object.values(bankMap).map(b => ({
      pair: b.pair,
      bank: b.bank,
      amount: formatAmount(b.amount)
    }));
    const response = {
      "Total Trades": { value: totalTrades.toString() },
      "Total Volume": { value: formatAmount(totalVolume) },
      "Avg Trade Size": { value: totalTrades > 0 ? formatAmount(totalVolume / totalTrades) : "$0" },
      BANKS: banks
    };
    res.json(response);
  } catch (err) {
    res.status(500).json({ error: 'Error fetching recent trades dashboard', details: err.message });
  }
};
// GET /api/forwardDash/maturity-buckets
exports.getMaturityBuckets = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT booking_amount, quote_currency, maturity_date
      FROM forward_bookings
      WHERE maturity_date IS NOT NULL
    `);
    const now = new Date();
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
    const buckets = {
      'Next 30 Days': { amount: 0, contracts: 0 },
      '31-90 Days': { amount: 0, contracts: 0 },
      '91-180 Days': { amount: 0, contracts: 0 },
      '180+ Days': { amount: 0, contracts: 0 },
    };
    for (const row of result.rows) {
      const amount = Number(row.booking_amount) || 0;
      const currency = (row.quote_currency || '').toUpperCase();
      const rate = rates[currency] || 1.0;
      const amountUsd = amount * rate;
      const maturity = new Date(row.maturity_date);
      const diffDays = Math.ceil((maturity - now) / (1000 * 60 * 60 * 24));
      let bucket = null;
      if (diffDays <= 30) bucket = 'Next 30 Days';
      else if (diffDays <= 90) bucket = '31-90 Days';
      else if (diffDays <= 180) bucket = '91-180 Days';
      else if (diffDays > 180) bucket = '180+ Days';
      if (bucket) {
        buckets[bucket].amount += amountUsd;
        buckets[bucket].contracts += 1;
      }
    }
    // Format output
    const formatAmount = (amt) => {
      if (amt >= 1e6) return `$${(amt / 1e6).toFixed(1)}M`;
      if (amt >= 1e3) return `$${(amt / 1e3).toFixed(1)}K`;
      return `$${amt.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
    };
    const response = {};
    for (const [key, val] of Object.entries(buckets)) {
      response[key] = {
        amount: formatAmount(val.amount),
        contracts: `${val.contracts} Contracts`,
      };
    }
    res.json(response);
  } catch (err) {
    res.status(500).json({ error: 'Error fetching maturity buckets', details: err.message });
  }
};
// GET /api/forwardDash/rollover-counts
exports.getRolloverCountsByCurrency = async (req, res) => {
  try {
    // Get count of rollovers per currency
    const result = await pool.query(`
      SELECT fb.quote_currency, COUNT(fr.id) AS rollover_count
      FROM forward_bookings fb
      LEFT JOIN forward_rollovers fr
        ON fr.booking_id = fb.system_transaction_id
      GROUP BY fb.quote_currency
    `);
    let total = 0;
    const data = [];
    for (const row of result.rows) {
      const currency = (row.quote_currency || "").toUpperCase();
      const count = Number(row.rollover_count) || 0;
      total += count;
      if (currency) {
        data.push({ label: `${currency} Rollovers:`, value: count.toString() });
      }
    }
    // Add total at the top
    data.unshift({ label: "Total Rollovers:", value: total.toString() });
    res.json(data);
  } catch (err) {
    res
      .status(500)
      .json({ error: "Error fetching rollover counts", details: err.message });
  }
};
// GET /api/forwardDash/bank-trades
exports.getBankTradesData = async (req, res) => {
  try {
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
    const result = await pool.query(
      "SELECT counterparty_dealer, order_type, quote_currency, booking_amount FROM forward_bookings"
    );
    const bankMap = {};
    for (const row of result.rows) {
      const bank = row.counterparty_dealer || "Unknown Bank";
      const trade = `${row.order_type ? row.order_type.trim() : ""} ${
        row.quote_currency ? row.quote_currency.trim() : ""
      }`
        .replace(/\s+/g, " ")
        .trim();
      const amount = typeof row.booking_amount === "number" ? row.booking_amount : Number(row.booking_amount);
      const currency = (row.quote_currency || '').toUpperCase();
      const rate = rates[currency] || 1.0;
      const amountUsd = amount * rate;
      const formattedAmount = isNaN(amountUsd)
        ? "$0"
        : `$${amountUsd.toLocaleString("en-US", { maximumFractionDigits: 0 })}`;
      if (!bankMap[bank]) {
        bankMap[bank] = { bank, trades: [], amounts: [] };
      }
      bankMap[bank].trades.push(trade);
      bankMap[bank].amounts.push(formattedAmount);
    }
    const forwardsData = Object.values(bankMap);
    res.json(forwardsData);
  } catch (err) {
    res
      .status(500)
      .json({ error: "Error fetching bank trades data", details: err.message });
  }
};
// GET /api/forwardDash/total-bankmargin
exports.getTotalBankMarginFromForwardBookings = async (req, res) => {
  try {
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
    const result = await pool.query(
      "SELECT bank_margin, quote_currency FROM forward_bookings"
    );
    let totalBankmargin = 0;
    for (const row of result.rows) {
      const margin = Number(row.bank_margin) || 0;
      const currency = (row.quote_currency || '').toUpperCase();
      const rate = rates[currency] || 1.0;
      totalBankmargin += margin * rate;
    }
    res.json({ totalBankmargin });
  } catch (err) {
    res.status(500).json({
      error: "Error calculating total bank margin",
      details: err.message,
    });
  }
};

const { pool } = require("../db");

// GET /api/forwardDash/total-usd
exports.getTotalUsdSumFromForwardBookings = async (req, res) => {
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
      "SELECT booking_amount, quote_currency FROM forward_bookings"
    );
    let totalUsd = 0;
    for (const row of result.rows) {
      const amount = Number(row.booking_amount) || 0;
      const currency = (row.quote_currency || "").toUpperCase();
      const rate = rates[currency] || 1.0;
      totalUsd += amount * rate;
    }
    res.json({ totalUsd });
  } catch (err) {
    res
      .status(500)
      .json({ error: "Error calculating total USD sum", details: err.message });
  }
};
