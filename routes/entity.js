const express = require("express");
const router = express.Router();
const entityController = require("../controllers/entityController");

router.post("/create", entityController.createEntity);

router.get("/names", entityController.getAllEntityNames);

router.get("/findParentAtLevel/:level", entityController.findParentAtLevel);

router.post("/sync-relationships", entityController.syncRelationships); // fixed path
router.get("/hierarchy", entityController.getEntityHierarchy);


router.get("/getRenderVars", entityController.getRenderVarsEntity);
router.get(
  "/getRenderVarsHierarchical",
  entityController.getRenderVarsHierarchical
);

// Entity status update endpoints
router.post("/delete/:id", entityController.deleteEntity);
router.post("/approve/:id", entityController.approveEntity);
router.post("/reject-bulk", entityController.rejectEntitiesBulk);
router.post("/update/:id", entityController.updateEntity);

module.exports = router;
