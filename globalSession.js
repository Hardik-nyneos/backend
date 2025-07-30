const sessions = new Map(); // Using Map instead of array for better performance

const globalSession = {
  Versions: ['0.0.6'],
  DepVersions: ['0.0.1'],
  WhiteLabelNames: ['Cashinvoice'],
  
  get UserSessions() {
    return Array.from(sessions.values());
  },
  
  addSession(sessionData) {
    sessions.set(sessionData.userId, sessionData);
  },
  
  getSession(userId) {
    return sessions.get(userId);
  },
  
  clearSession(userId) {
    sessions.delete(userId);
    return this.UserSessions;
  },
  
  _getSessionMap() {
    return sessions;
  }
};

module.exports = globalSession;
