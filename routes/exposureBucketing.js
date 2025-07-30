const express = require("express");
const router = express.Router();

const exposureBucketingController = require("../controllers/exposureBucketingController");


router.get("/userVars", exposureBucketingController.getUserVars);
router.get("/renderVars", exposureBucketingController.getRenderVars);
// router.get("/pendingrenderVars", exposureUploadController.getPendingApprovalVars);
router.get("/userJourney", exposureBucketingController.getUserJourney);
router.post("/bulkApprove", exposureBucketingController.approveBucketing);
router.post("/bulkReject", exposureBucketingController.rejectMultipleExposures);
router.post("/:id/edit", exposureBucketingController.getupdate);

module.exports = router;