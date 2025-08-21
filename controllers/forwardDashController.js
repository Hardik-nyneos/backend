const { pool } = require("../db");
// GET /api/forwardDash/bu-maturity-currency-summary
exports.getBuMaturityCurrencySummaryJoined = async (req, res) => {
  try {
    // --- Get allowed business units for user (descendants logic) ---
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
      return res.status(404).json({ error: "User has no business unit assigned" });
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
      return res.status(404).json({ error: "No accessible business units found" });
    }

    // Only select forward_bookings for allowed entities
    const result = await pool.query(
      "SELECT entity_level_0, delivery_period, quote_currency, order_type, booking_amount FROM forward_bookings WHERE entity_level_0 = ANY($1)",
      [buNames]
    );
    const bucketLabels = {
      month_1: "1 Month",
      month_2: "2 Month",
      month_3: "3 Month",
      month_4: "4 Month",
      month_4_6: "4-6 Month",
      month_6plus: "6 Month +",
    };
    function normalizeDeliveryPeriod(period) {
      if (!period) return "month_1";
      const p = period.toLowerCase().replace(/[^a-z0-9]/g, "");
      if (["1m", "1month", "month1", "m1", "mon1"].includes(p)) return "month_1";
      if (["2m", "2month", "month2", "m2", "mon2"].includes(p)) return "month_2";
      if (["3m", "3month", "month3", "m3", "mon3"].includes(p)) return "month_3";
      if (["4m", "4month", "month4", "m4", "mon4"].includes(p)) return "month_4";
      if (["46m", "4to6month", "month46", "month4to6", "m46", "mon46", "4_6month", "4_6m", "4-6m", "4-6month"].includes(p)) return "month_4_6";
      if (["6mplus", "6monthplus", "month6plus", "6plus", "m6plus", "mon6plus", "6m+", "6month+", "month6+"].includes(p)) return "month_6plus";
      if (p.includes("6")) return "month_6plus";
      if (p.includes("4")) return "month_4";
      if (p.includes("3")) return "month_3";
      if (p.includes("2")) return "month_2";
      return "month_1";
    }
    const summary = {};
    for (const row of result.rows) {
      const bu = row.entity_level_0 || "Unknown BU";
      const bucketKey = normalizeDeliveryPeriod(row.delivery_period);
      const maturity = bucketLabels[bucketKey] || "1 Month";
      const currency = (row.quote_currency || "").toUpperCase();
      const orderType = (row.order_type || "").toLowerCase();
      const amount = Number(row.booking_amount) || 0;
      const key = `${bu}__${maturity}__${currency}`;
      if (!summary[key]) {
        summary[key] = { bu, maturity, currency, forwardBuy: 0, forwardSell: 0 };
      }
      if (orderType === "buy") summary[key].forwardBuy += amount;
      else summary[key].forwardSell += amount;
    }
    res.json(Object.values(summary));
  } catch (err) {
    res.status(500).json({ error: "Error fetching summary", details: err.message });
  }
};
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
      RMB: 0.14,
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
      RMB: 0.14,
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
      RMB: 0.14,
      EUR: 1.09,
      GBP: 1.28,
      JPY: 0.0067,
      SEK: 0.095,
      INR: 0.0117,
    };
    const result = await pool.query(
      "SELECT counterparty, order_type, base_currency, booking_amount FROM forward_bookings"
    );
    const bankMap = {};
    for (const row of result.rows) {
      const bank = row.counterparty || "Unknown Bank";
      const trade = `${row.order_type ? row.order_type.trim() : ""} ${
        row.base_currency ? row.base_currency.trim() : ""
      }`
        .replace(/\s+/g, " ")
        .trim();
      const amount = typeof row.booking_amount === "number" ? row.booking_amount : Number(row.booking_amount);
      const currency = (row.base_currency || '').toUpperCase();
      const rate = rates[currency] || 1.0;
      const amountUsd = amount * rate;
      if (!bankMap[bank]) {
        bankMap[bank] = { bank, trades: {} };
      }
      if (!bankMap[bank].trades[trade]) {
        bankMap[bank].trades[trade] = 0;
      }
      bankMap[bank].trades[trade] += amountUsd;
    }
    // Format output: for each bank, list unique trades and summed formatted amounts
    const forwardsData = Object.values(bankMap).map(b => ({
      bank: b.bank,
      trades: Object.keys(b.trades),
      amounts: Object.values(b.trades).map(amt =>
        amt >= 1e6 ? `$${(amt / 1e6).toFixed(1)}M` :
        amt >= 1e3 ? `$${(amt / 1e3).toFixed(1)}K` :
        `$${amt.toLocaleString('en-US', { maximumFractionDigits: 0 })}`
      )
    }));
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
      RMB: 0.14,
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



// GET /api/forwardDash/total-usd
exports.getTotalUsdSumFromForwardBookings = async (req, res) => {
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
    // Join hedge links with headers to fetch hedged_amount and currency
    const result = await pool.query(`
      SELECT 
        ehl.hedged_amount, 
        eh.currency
      FROM exposure_hedge_links ehl
      INNER JOIN exposure_headers eh 
        ON ehl.exposure_header_id = eh.exposure_header_id
    `);

    let totalUsd = 0;
    for (const row of result.rows) {
      const amount = Number(row.hedged_amount) || 0;
      const currency = (row.currency || "").toUpperCase();
      const rate = rates[currency] || 1.0;
      totalUsd += amount * rate;
    }

    res.json({ totalUsd });
  } catch (err) {
    res.status(500).json({
      error: "Error calculating total USD sum from hedges",
      details: err.message,
    });
  }
};


exports.getOpenAmountToBookingRatio = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        (SELECT COALESCE(SUM(ABS(total_open_amount)), 0) FROM exposure_headers) AS total_open,
        (SELECT COALESCE(SUM(ABS(booking_amount)), 0) FROM forward_bookings) AS total_booking
    `);

    const { total_open, total_booking } = result.rows[0];
    let ratio = 0;

    if (Number(total_booking) > 0) {
      ratio = ((Number(total_open) / Number(total_booking)) * 100).toFixed(3);
    }

    res.json({ ratio });
  } catch (err) {
    res.status(500).json({
      error: "Error calculating open amount to booking ratio",
      details: err.message,
    });
  }
};

// Exchange rates to USD (you can fetch from DB or API instead)
const rates = {
  USD: 1.0,
  EUR: 1.1,
  INR: 0.012,
  GBP: 1.25,
  AUD: 0.68,
  CAD: 0.75,
  CHF: 1.1,
  CNY: 0.14,
  JPY: 0.0068,
  // extend as needed
};

// ================== Avg Exposure Maturity (WAET) ==================
exports.getAvgExposureMaturity = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        total_original_amount AS amount,
        currency,
        document_date,
        ABS(CAST(document_date AS date) - CURRENT_DATE) AS days_to_maturity
      FROM exposure_headers
      WHERE document_date IS NOT NULL
    `);

    let weightedSum = 0;
    let totalAmount = 0;

    result.rows.forEach(row => {
      const rate = rates[row.currency?.toUpperCase()] || 1.0;
      const usdAmount = Math.abs(Number(row.amount)) * rate;
      weightedSum += usdAmount * row.days_to_maturity;
      totalAmount += usdAmount;
    });

    const avgMaturity = totalAmount > 0 ? Math.round(weightedSum / totalAmount) : 0;

    res.json({ avgExposureMaturity: avgMaturity });
  } catch (err) {
    res.status(500).json({
      error: "Error calculating Avg Exposure Maturity",
      details: err.message,
    });
  }
};

// ================== Avg Forward Maturity (WAHT) ==================
exports.getAvgForwardMaturity = async (req, res) => {
  try {
    const result = await pool.query(`
     SELECT 
        booking_amount AS amount,
        base_currency,
        maturity_date,
        ABS(CAST(maturity_date AS date) - CURRENT_DATE) AS days_to_maturity
      FROM forward_bookings
      WHERE maturity_date IS NOT NULL
    `);

    let weightedSum = 0;
    let totalAmount = 0;

    result.rows.forEach(row => {
      const rate = rates[row.currency?.toUpperCase()] || 1.0;
      const usdAmount = Math.abs(Number(row.amount)) * rate;
      weightedSum += usdAmount * row.days_to_maturity;
      totalAmount += usdAmount;
    });

    const avgMaturity = totalAmount > 0 ? Math.round(weightedSum / totalAmount) : 0;

    res.json({ avgForwardMaturity: avgMaturity });
  } catch (err) {
    res.status(500).json({
      error: "Error calculating Avg Forward Maturity",
      details: err.message,
    });
  }
};

exports.getForwardBuySellTotals = async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        booking_amount AS amount,
        quote_currency,
        order_type
      FROM forward_bookings
      WHERE booking_amount IS NOT NULL
    `);

    let buyTotal = 0;
    let sellTotal = 0;

    result.rows.forEach(row => {
      const rate = rates[row.currency?.toUpperCase()] || 1.0;
      const usdAmount = Math.abs(Number(row.amount)) * rate;

      if (row.order_type?.toUpperCase() === "BUY") {
        buyTotal += usdAmount;
      } else if (row.order_type?.toUpperCase() === "SELL") {
        sellTotal += usdAmount;
      }
    });

    res.json({
      buyForwardsUSD: buyTotal.toFixed(2),
      sellForwardsUSD: sellTotal.toFixed(2),
    });
  } catch (err) {
    res.status(500).json({
      error: "Error calculating Buy/Sell Forwards totals",
      details: err.message,
    });
  }
};


exports.getUserCurrency = async (req, res) => {
  try {
    
    const globalSession = require("../globalSession");
    const session = globalSession.UserSessions[0];
    if (!session) {
      return res.status(404).json({ error: "No active session found" });
    }

    const userId = session.userId;

    // Step 1: Get user's business unit name
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

    // Step 2 & 3: Get default currency from masterentity
    const entityResult = await pool.query(
      "SELECT default_currency FROM masterentity WHERE entity_name = $1",
      [userBu]
    );

    if (!entityResult.rows.length) {
      return res.status(404).json({ error: "No entity found for given business unit" });
    }

    const defaultCurrency = entityResult.rows[0].default_currency;

    // Step 4: Return default currency
    return res.json({ defaultCurrency });

  } catch (err) {
    console.error("Error fetching user currency:", err);
    return res.status(500).json({ error: "Internal server error" });
  }
};



