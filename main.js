require("dotenv").config();
const express = require("express");
const cors = require("cors");
const logger = require("./middleware/logger");
const { pool } = require("./db.js");
const userRoutes = require("./routes/user");
const roleRoutes = require("./routes/role");
const permissionRoutes = require("./routes/permission");
const debugRoutes = require("./routes/debug");
const authRoutes = require("./routes/auth");
const rolesRouter = require("./routes/role");
const entityRoutes = require("./routes/entity");
const exposureUploadRoutes = require("./routes/exposureUpload");
const exposureBucketingRoutes = require("./routes/exposureBucketing");
const sessionChecker = require("./middleware/sessionChecker");
const hedgingProposalRoutes = require("./routes/hedgingProposal");
const forwards = require("./routes/forwardBookings");
const forwardDashRoutes = require("./routes/forwardDash");
const settelementRoutes = require("./routes/settelement");
const globalSession = require("./globalSession");
const mtmRoutes = require("./routes/mtm");

const app = express();
const port = process.env.PORT || 3143;

// ✅ Configure allowed origins for CORS
app.use(cors({
  origin: "*"
}));

// ✅ Explicit Referrer Policy for all responses
app.use((req, res, next) => {
  res.setHeader("Referrer-Policy", "no-referrer-when-downgrade"); 
  next();
});

app.use(express.json());
app.use(logger);

app.get("/", sessionChecker, (req, res) => {
  res.send("Server is running");
});

app.use("/api/users", userRoutes);
app.use("/api/forwards", forwards);
app.use("/roles", rolesRouter);
app.use("/api/roles", roleRoutes);
app.use("/api/permissions", permissionRoutes);
app.use("/api/debug", debugRoutes);
app.use("/api/auth", authRoutes);
app.use("/api/entity", entityRoutes);
app.use("/api/exposureUpload", exposureUploadRoutes);
app.use("/api/exposureBucketing", exposureBucketingRoutes);
app.use("/api/hedgingProposal", hedgingProposalRoutes);
app.use("/api/forwardDash", forwardDashRoutes);
app.use("/api/settlement", settelementRoutes);
app.use("/api/mtm", mtmRoutes);

app.get("/api/version", (req, res) => {
  res.json({ version: globalSession.Versions[0] });
});

app.use((err, req, res, next) => {
  console.error("[ERROR]", err.stack || err.message);
  res.status(500).json({ success: false, error: "Internal server error" });
});

// ===== Global Session Helpers =====
globalSession.appendSession = function (sessionObj) {
  globalSession.UserSessions.push(sessionObj);
};

globalSession.getSessionsByUserId = function (userId) {
  return globalSession.UserSessions.filter((s) => s.userId === userId);
};

// ===== Session Endpoints =====
app.get("/api/getsessions/:userId", (req, res) => {
  try {
    const userId = parseInt(req.params.userId);
    if (isNaN(userId)) {
      return res.status(400).json({ success: false, error: "Invalid user ID format" });
    }
    const sessions = globalSession.getSessionsByUserId(userId);
    if (!sessions.length) {
      return res.status(404).json({ success: false, error: "No sessions found for this user" });
    }
    res.json({ success: true, sessions });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/api/getuserdetails/:userId", (req, res) => {
  try {
    const userId = parseInt(req.params.userId);
    if (isNaN(userId)) {
      return res.status(400).json({ success: false, error: "Invalid user ID format" });
    }
    const sessions = globalSession.getSessionsByUserId(userId);
    if (!sessions.length) {
      return res.status(404).json({ success: false, error: "No active session found for this user" });
    }
    res.json({ success: true, sessions });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.listen(port, () => {
  console.log(`🚀 Backend service running on port ${port}`);
});
