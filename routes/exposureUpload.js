const path = require("path");
const fs = require("fs");
const express = require("express");
const router = express.Router();
const multer = require("multer");

const exposureUploadController = require("../controllers/exposureUploadController");

// Multer config for multi-file upload (must match controller)
const upload = multer({ dest: path.join(__dirname, "../uploads") });
const uploadFields = upload.fields([
  { name: "input_letters_of_credit", maxCount: 10 },
  { name: "input_purchase_orders", maxCount: 10 },
  { name: "input_sales_orders", maxCount: 10 },
]);

router.get("/userVars", exposureUploadController.getUserVars);
router.get("/renderVars", exposureUploadController.getRenderVars);
router.get(
  "/pendingrenderVars",
  exposureUploadController.getPendingApprovalVars
);
router.get("/userJourney", exposureUploadController.getUserJourney);
router.post("/bulkApprove", exposureUploadController.approveMultipleExposures);
router.post("/bulkReject", exposureUploadController.rejectMultipleExposures);
router.post("/deleteExposure", exposureUploadController.deleteExposure);
router.get(
  "/netanalysis",
  exposureUploadController.getBuMaturityCurrencySummary
);
router.get(
  "/netanalysis-joined",
  exposureUploadController.getBuMaturityCurrencySummaryJoined
);
router.get("/top-currencies", exposureUploadController.getTopCurrencies);
router.get(
  "/top-currencies-headers",
  exposureUploadController.getTopCurrenciesFromHeaders
);
router.get("/USDsum", exposureUploadController.getPoAmountUsdSum);
router.get(
  "/USDsum-headers",
  exposureUploadController.getTotalOpenAmountUsdSumFromHeaders
);
router.get("/payables", exposureUploadController.getPayablesByCurrency);
router.get(
  "/payables-headers",
  exposureUploadController.getPayablesByCurrencyFromHeaders
);
router.get("/receivables", exposureUploadController.getReceivablesByCurrency);
router.get(
  "/receivables-headers",
  exposureUploadController.getReceivablesByCurrencyFromHeaders
);
router.get(
  "/getpoAmountByCurrency",
  exposureUploadController.getAmountByCurrency
);
router.get(
  "/getAmountByCurrency-headers",
  exposureUploadController.getAmountByCurrencyFromHeaders
);
router.get(
  "/buintexp",
  exposureUploadController.getBusinessUnitCurrencySummary
);
router.get(
  "/business-unit-currency-summary-headers",
  exposureUploadController.getBusinessUnitCurrencySummaryFromHeaders
);
router.get(
  "/matexpirysummary",
  exposureUploadController.getMaturityExpirySummary
);
router.get(
  "/maturity-expiry-summary-headers",
  exposureUploadController.getMaturityExpirySummaryFromHeaders
);
router.get(
  "/matexpirycount7days",
  exposureUploadController.getMaturityExpiryCount7Days
);
router.get(
  "/maturity-expiry-count-7days-headers",
  exposureUploadController.getMaturityExpiryCount7DaysFromHeaders
);
//----------------------------------------------------------------------------------------------------------

router.get(
  "/headers-lineitems",
  exposureUploadController.getExposureHeadersLineItems
);

router.get(
  "/pending-headers-lineitems",
  exposureUploadController.getPendingApprovalHeadersLineItems
);

router.post(
  "/approve-multiple-headers",
  exposureUploadController.approveMultipleExposureHeaders
);
router.post(
  "/delete-exposure-headers",
  exposureUploadController.deleteExposureHeaders
);
router.post(
  "/reject-multiple-headers",
  exposureUploadController.rejectMultipleExposureHeaders
);
// Multi-file batch upload endpoint
router.post(
  "/batch-upload",
  uploadFields,
  exposureUploadController.batchUploadStagingData
);

//----------------------------------------------------------------------------------------------------
router.post(
  "/upload-csv",
  upload.single("file"),
  exposureUploadController.uploadExposuresFromCSV
);

module.exports = router;
