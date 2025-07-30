const express = require("express");
const router = express.Router();
const exposureController = require("../controllers/exposureController");

router.post("/upload-csv", exposureController.uploadCsv);
router.get("/exposure-bucketing", exposureController.getExposureBucketing);
router.post(
  "/exposure-bucketing/save",
  exposureController.saveExposureBucketing
);
router.get("/hedging-proposal", exposureController.getHedgingProposal);
router.put("/hedging-proposal", exposureController.saveHedgingProposal);

module.exports = router;
