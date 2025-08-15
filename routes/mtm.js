const express = require("express");
const router = express.Router();
const multer = require("multer");
const path = require("path");
const upload = multer({ dest: path.join(__dirname, "../uploads") });
const mtmController = require("../controllers/mtmController");

router.post("/upload", upload.array("files"), mtmController.uploadMTMFiles);
router.get("/", mtmController.getMTMData);

module.exports = router;
