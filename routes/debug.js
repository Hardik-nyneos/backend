const express = require('express');
const router = express.Router();
const debugController = require('../controllers/debugController');
const sessionChecker = require('../middleware/sessionChecker');

// Apply sessionChecker to all debug routes
router.use(sessionChecker);

router.get('/show-all-tables', debugController.showAllTables);
router.get('/db/tables', debugController.showTableStructure);

module.exports = router;
