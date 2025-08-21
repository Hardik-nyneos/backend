// Link exposure to hedge booking

// Manual entry for forward confirmations

const express = require('express');
const router = express.Router();
const forwardController = require('../controllers/forwardController');

router.get(
  "/linked-summary-by-category",
  forwardController.getLinkedSummaryByCategory
);
router.get('/exposure/summary', forwardController.getExposureSummary);
// Manual entry for forward bookings
router.post('/forward-bookings/manual-entry', forwardController.addForwardBookingManualEntry);
// Get all forward_bookings relevant to user's accessible entities
router.get('/forward-bookings/forwardDetails', forwardController.getEntityRelevantForwardBookings);
router.post('/exposure-hedge-links/link', forwardController.linkExposureHedge);

const uploadFields = forwardController.upload.fields([
  { name: "forward_axis", maxCount: 10 },
  { name: "forward_hdfc", maxCount: 10 },
  { name: "forward_icici", maxCount: 10 },
  { name: "forward_kotak", maxCount: 10 },
]);
router.post(
  "/bank-forward-bookings/upload-multi",
  uploadFields,
  forwardController.uploadBankForwardBookingsMulti
);
// Multi-file upload for forward bookings (CSV/Excel)
router.post('/forward-bookings/upload-multi', forwardController.upload.array('files'), forwardController.uploadForwardBookingsMulti);
router.post('/forward-confirmations/manual-entry', forwardController.addForwardConfirmationManualEntry);

// Multi-file upload for forward confirmations (CSV/Excel)
router.post('/forward-confirmations/upload-multi', forwardController.upload.array('files'), forwardController.uploadForwardConfirmationsMulti);
// Change processing_status to Approved or Rejected for a forward booking
router.post('/forward-bookings/update-processing-status', forwardController.updateForwardBookingProcessingStatus);
// Update any fields of forward_bookings using system_transaction_id
router.post("/:system_transaction_id/update",forwardController.updateForwardBookingFields);
// Bulk approve/reject forward bookings
router.post('/forward-bookings/bulk-update-processing-status', forwardController.bulkUpdateForwardBookingProcessingStatus);

router.post("/bulk-delete", forwardController.bulkDeleteForwardBookings);
module.exports = router;
