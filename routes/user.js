const express = require('express');
const router = express.Router();
const userController = require('../controllers/userController');
const sessionChecker = require('../middleware/sessionChecker');

// Apply sessionChecker to all user routes
// router.use(sessionChecker);

router.post('/create', userController.createUser);
router.get('/', userController.getUsers);
router.get('/approvedusers', userController.getApprovedUsers);
router.get('/awaitingdata', userController.getAwaitingData);
router.post('/:id/approve', userController.approveUser);
router.post('/:id/delete', userController.deleteUser);
router.post('/:id/update', userController.updateUser);

// router.post('/bulk-approve', userController.approveMultipleUsers);
router.get('/:id', userController.getUserById);

router.delete('/:id', userController.deleteUser);
router.post('/:id/approve', userController.approveUser);
router.post('/:id/reject', userController.rejectUser);

router.post('/bulk-approve', userController.approveMultipleUsers);
router.post('/bulk-reject', userController.rejectMultipleUsers);

// router.get('/approvedusers', userController.getApprovedUsers);
// router.get('/awaitingdata', userController.getAwaitingData);
module.exports = router;
