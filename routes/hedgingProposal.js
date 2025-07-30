const express = require("express");
const router = express.Router();
const hedgingProposalController = require("../controllers/hedgingProposalController");

router.get("/aggregate", hedgingProposalController.getHedgingProposalsAggregated);

module.exports = router;