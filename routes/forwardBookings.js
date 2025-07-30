// Manual entry for forward confirmations

const express = require('express');
const router = express.Router();
const forwardController = require('../controllers/forwardController');

// Manual entry for forward bookings
router.post('/forward-bookings/manual-entry', forwardController.addForwardBookingManualEntry);

// Multi-file upload for forward bookings (CSV/Excel)
router.post('/forward-bookings/upload-multi', forwardController.upload.array('files'), forwardController.uploadForwardBookingsMulti);
router.post('/forward-confirmations/manual-entry', forwardController.addForwardConfirmationManualEntry);

// Multi-file upload for forward confirmations (CSV/Excel)
router.post('/forward-confirmations/upload-multi', forwardController.upload.array('files'), forwardController.uploadForwardConfirmationsMulti);
module.exports = router;
