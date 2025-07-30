require("dotenv").config();
const express = require("express");
const cors = require("cors");
const logger = require("./middleware/logger");
// const session = require('express-session');
// const pgSession = require('connect-pg-simple')(session);
const { pool } = require("./db.js");
const userRoutes = require("./routes/user");
const roleRoutes = require("./routes/role");
const permissionRoutes = require("./routes/permission");
const debugRoutes = require("./routes/debug");
const authRoutes = require("./routes/auth");
const rolesRouter = require("./routes/role");
const entityRoutes = require("./routes/entity");
const exposureRoutes = require("./routes/exposure");
const exposureUploadRoutes = require("./routes/exposureUpload");
const exposureBucketingRoutes = require("./routes/exposureBucketing");
// const globalSession = require('./globalSession.js');
const sessionChecker = require("./middleware/sessionChecker");
const hedgingProposalRoutes = require("./routes/hedgingProposal");

const app = express();
const port = process.env.PORT || 3143;

app.use(cors());
app.use(express.json());
app.use(logger);

app.get("/", sessionChecker, (req, res) => {
  res.send("Server is running");
});

app.use("/api/users", userRoutes);
app.use("/roles", rolesRouter);
app.use("/api/roles", roleRoutes);
app.use("/api/permissions", permissionRoutes);
app.use("/api/debug", debugRoutes);
app.use("/api/auth", authRoutes);
app.use("/api/entity", entityRoutes);
// app.use("/api/exposure", exposureRoutes);
app.use("/api/exposureUpload", exposureUploadRoutes);
app.use("/api/exposureBucketing", exposureBucketingRoutes);
app.use("/api/hedgingProposal", hedgingProposalRoutes);

app.get("/api/version", (req, res) => {
  res.json({ version: globalSession.Versions[0] });
});

app.use((err, req, res, next) => {
  console.error("[ERROR]", err.stack || err.message);
  res.status(500).json({ success: false, error: "Internal server error" });
});

app.listen(port, () => {
  console.log(`🚀 Backend service running on port ${port}`);
});

// const { getSession } = require('../globalSession');

// In your main server file
const globalSession = require("./globalSession");

// Helper to append a new session (call this in your login logic)
globalSession.appendSession = function (sessionObj) {
  globalSession.UserSessions.push(sessionObj);
};

// Helper to get all sessions for a userId
globalSession.getSessionsByUserId = function (userId) {
  return globalSession.UserSessions.filter((s) => s.userId === userId);
};

// Endpoint: get all sessions for a userId
app.get("/api/getsessions/:userId", (req, res) => {
  try {
    const userId = parseInt(req.params.userId);
    if (isNaN(userId)) {
      return res
        .status(400)
        .json({ success: false, error: "Invalid user ID format" });
    }
    const sessions = globalSession.getSessionsByUserId(userId);
    if (!sessions.length) {
      return res
        .status(404)
        .json({ success: false, error: "No sessions found for this user" });
    }
    res.json({ success: true, sessions });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Update getuserdetails to return all sessions for a userId
app.get("/api/getuserdetails/:userId", (req, res) => {
  try {
    const userId = parseInt(req.params.userId);
    if (isNaN(userId)) {
      return res
        .status(400)
        .json({ success: false, error: "Invalid user ID format" });
    }
    const sessions = globalSession.getSessionsByUserId(userId);
    if (!sessions.length) {
      return res
        .status(404)
        .json({
          success: false,
          error: "No active session found for this user",
        });
    }
    res.json({ success: true, sessions });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});


