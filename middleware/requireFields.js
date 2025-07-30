module.exports = function requireFields(fields) {
  return (req, res, next) => {
    const missing = fields.filter(f => !(f in req.body));
    if (missing.length > 0) {
      return res.status(400).json({ success: false, error: `Missing required fields: ${missing.join(', ')}` });
    }
    next();
  };
};
