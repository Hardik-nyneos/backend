const express = require("express");
const router = express.Router();

const exposureBucketingController = require("../controllers/exposureBucketingController");

router.get("/userVars", exposureBucketingController.getUserVars);
router.get("/renderVars", exposureBucketingController.getRenderVars);
// router.get("/pendingrenderVars", exposureUploadController.getPendingApprovalVars);
router.get("/userJourney", exposureBucketingController.getUserJourney);
router.post("/bulkApprove", exposureBucketingController.approveBucketing);
router.post("/bulkReject", exposureBucketingController.rejectMultipleExposures);
router.get(
  "/joined-exposures",
  exposureBucketingController.getExposureHeadersLineItemsBucketing
);
// Approve/reject exposure_bucketing status (status only, no delete logic)
router.post("/approve", exposureBucketingController.approveBucketingStatus);
router.post("/reject", exposureBucketingController.rejectBucketingStatus);

// Update joined exposures (header, line item, bucketing)
router.post(
  "/:exposure_header_id/update",
  exposureBucketingController.updateExposureHeadersLineItemsBucketing
);
router.post("/:id/edit", exposureBucketingController.getupdate);

module.exports = router;
