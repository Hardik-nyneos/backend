const globalSession = require("../globalSession");

function sessionChecker(req, res, next) {
  const userId = globalSession.UserSessions.find((u) => u.isLoggedIn)?.userId;
  if (!userId) {
    return res.status(401).json({ error: "No session or user ID provided" });
  }

  const sessionUser = globalSession.UserSessions.find(
    (u) => u.userId === userId
  );
  if (!sessionUser) {
    return res.status(401).json({ error: "Invalid or expired session" });
  }
  req.user = sessionUser;
  next();
}

module.exports = sessionChecker;
