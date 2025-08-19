const express = require("express");
const router = express.Router();
const forwardDashController = require("../controllers/forwardDashController");

// GET /api/forwardDash/bank-trades
router.get("/bank-trades", forwardDashController.getBankTradesData);

// GET /api/forwardDash/total-usd
router.get(
  "/total-usd",
  forwardDashController.getTotalUsdSumFromForwardBookings
);

router.get("/hedge-ratio",forwardDashController.getOpenAmountToBookingRatio);
router.get("/waet",forwardDashController.getAvgExposureMaturity);
router.get("/waht",forwardDashController.getAvgForwardMaturity);

// GET /api/forwardDash/total-bankmargin
router.get(
  "/total-bankmargin",
  forwardDashController.getTotalBankMarginFromForwardBookings
);
router.get("/buysell",forwardDashController.getForwardBuySellTotals);
router.get(
  "/rollover-counts",
  forwardDashController.getRolloverCountsByCurrency
);

router.get("/maturity-buckets", forwardDashController.getMaturityBuckets);
router.get(
  "/recent-trades-dashboard",
  forwardDashController.getRecentTradesDashboard
);
router.get("/active-forwards", forwardDashController.getActiveForwardsCount);

router.get(
  "/bu-maturity-currency-summary",
  forwardDashController.getBuMaturityCurrencySummaryJoined
);

module.exports = router;
