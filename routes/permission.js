const express = require("express");
const router = express.Router();
const permissionController = require("../controllers/permissionController");
const sessionChecker = require("../middleware/sessionChecker");

// Apply sessionChecker to all permission routes
router.use(sessionChecker);

router.post("/assign", permissionController.upsertRolePermissions);
router.post("/hasaccess", permissionController.WithHasAccess);
router.post("/permissionjson", permissionController.getRolePermissionsJson);

router.post(
  "/update-role-permissions-status-by-name",
  permissionController.updateRolePermissionsStatusByName
);
router.get("/roles-status", permissionController.getRolesStatus);

module.exports = router;
