const express = require('express');
const router = express.Router();
const roleController = require('../controllers/roleController');
const sessionChecker = require('../middleware/sessionChecker');

// Apply sessionChecker to all role routes
// router.use(sessionChecker);

router.post('/create', roleController.createRole);
router.get('/', roleController.getRoles);
router.get('/roles', roleController.getJustRoles);
router.post('/:id/approve', roleController.approveRole);
router.post("/bulk-approve", roleController.approveMultipleRoles);
router.post("/bulk-reject", roleController.rejectMultipleRoles);
router.post('/:id/update', roleController.updateRole);
router.post('/:id/reject', roleController.rejectRole);
router.post('/:id/delete', roleController.deleteRole);
router.get('/page-data', roleController.getRolesPageData);
router.get('/awaitingdata', roleController.getPendingRoles);



module.exports = router;
