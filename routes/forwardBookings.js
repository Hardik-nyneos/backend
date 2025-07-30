const express = require('express');
const router = express.Router();
const forwardController = require('../controllers/forwardController');

// ...existing routes...

// Manual entry for forward bookings
router.post('/forward-bookings/manual-entry', forwardController.addForwardBookingManualEntry);

module.exports = router;
