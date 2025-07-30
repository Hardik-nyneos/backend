const express = require("express");
const router = express.Router();
const {
  loginUser,
  logoutUser,
  getSidebarPermissions,
} = require("../controllers/authController");

router.post("/login", loginUser);
router.post("/logout", logoutUser);
router.post("/sidebar", getSidebarPermissions);

module.exports = router;
