const express = require('express');
const router = express.Router();
const settelementController = require('../controllers/settelementController');

// POST /settlement/filter-forward-bookings
router.post('/filter-forward-bookings', settelementController.filterForwardBookingsForSettlement);

module.exports = router;
