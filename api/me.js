// GET /api/me — who the current session belongs to, and what it can see.
// The pages use this to decide which nav links and controls to render.

const { requireSession, scopeGyms, loadUsers } = require("../lib/auth");
const { GYM_LABELS } = require("../lib/gyms");

module.exports = async function handler(req, res){
  res.setHeader("Cache-Control", "no-store");

  const session = requireSession(req, res);
  if (!session) return;

  let isBootstrap = false;
  try {
    isBootstrap = (await loadUsers()).length === 0;
  } catch { /* KV down — treat as configured and let other routes report it */ }

  res.status(200).json({
    username: session.username,
    role: session.role,
    gyms: scopeGyms(session, GYM_LABELS),
    allGyms: session.role === "admin" ? GYM_LABELS : undefined,
    // True while no real accounts exist, so the admin UI can prompt for setup.
    bootstrap: isBootstrap
  });
};
