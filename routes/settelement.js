// POST /settlement/forwards-by-entity-currency

const express = require('express');
const router = express.Router();
const settelementController = require('../controllers/settelementController');

// POST /settlement/filter-forward-bookings
router.post('/filter-forward-bookings', settelementController.filterForwardBookingsForSettlement);
router.post('/forwards-by-entity-currency', settelementController.getForwardBookingsByEntityAndCurrency);
module.exports = router;
