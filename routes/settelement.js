// POST /settlement/forwards-by-entity-currency

const express = require('express');
const router = express.Router();
const settelementController = require('../controllers/settelementController');

// POST /settlement/filter-forward-bookings
router.post('/filter-forward-bookings', settelementController.filterForwardBookingsForSettlement);
router.post('/forwards-by-entity-currency', settelementController.getForwardBookingsByEntityAndCurrency);
router.get("/bookingList", settelementController.getForwardBookingList);
router.post(
  "/exposuresByBookingIds",
  settelementController.getExposuresByBookingIds
);
router.post('/create-cancellations', settelementController.createForwardCancellations);

module.exports = router;
