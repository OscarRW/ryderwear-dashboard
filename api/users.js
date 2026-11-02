// Admin-only login management.
//
//   GET    /api/users            list accounts (never returns password hashes)
//   POST   /api/users            create or update an account
//   DELETE /api/users?id=<id>    remove an account
//
// Guards against locking everyone out: the last remaining admin can't be
// deleted or demoted.

const crypto = require("crypto");
const {
  requireSession, loadUsers, saveUsers, publicUser, hashPassword, parseBody
} = require("../lib/auth");
const { GYM_LABELS } = require("../lib/gyms");

const ROLES = new Set(["admin", "gym"]);
const USERNAME_RE = /^[a-z0-9][a-z0-9._-]{1,31}$/;
const MIN_PASSWORD_LENGTH = 8;

// "*" (every gym) or a list of labels that exist on the roster.
function normaliseGyms(role, gyms){
  if (role === "admin") return "*";
  if (gyms === "*") return "*";
  const list = Array.isArray(gyms) ? gyms : [gyms];
  const clean = [...new Set(list.filter(g => GYM_LABELS.includes(g)))];
  return clean;
}

module.exports = async function handler(req, res){
  res.setHeader("Cache-Control", "no-store");

  const session = requireSession(req, res, { adminOnly: true });
  if (!session) return;

  let users;
  try {
    users = await loadUsers();
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }

  if (req.method === "GET"){
    return res.status(200).json({ users: users.map(publicUser), gyms: GYM_LABELS });
  }

  if (req.method === "POST"){
    const body = parseBody(req);
    const username = String(body.username || "").trim().toLowerCase();
    const role = String(body.role || "").trim();
    const password = body.password;
    const id = body.id ? String(body.id) : null;

    if (!USERNAME_RE.test(username)){
      return res.status(400).json({
        error: "Username must be 2–32 characters: lowercase letters, numbers, dot, dash or underscore"
      });
    }
    if (!ROLES.has(role)){
      return res.status(400).json({ error: "Role must be 'admin' or 'gym'" });
    }

    const existing = id ? users.find(u => u.id === id) : null;
    if (id && !existing) return res.status(404).json({ error: "Account not found" });

    const clash = users.find(u => u.username === username && u.id !== id);
    if (clash) return res.status(409).json({ error: `Username "${username}" is already taken` });

    if (!existing && (typeof password !== "string" || password.length < MIN_PASSWORD_LENGTH)){
      return res.status(400).json({ error: `Password must be at least ${MIN_PASSWORD_LENGTH} characters` });
    }
    if (existing && password !== undefined && password !== "" &&
        (typeof password !== "string" || password.length < MIN_PASSWORD_LENGTH)){
      return res.status(400).json({ error: `Password must be at least ${MIN_PASSWORD_LENGTH} characters` });
    }

    const gyms = normaliseGyms(role, body.gyms);
    if (role === "gym" && Array.isArray(gyms) && gyms.length === 0){
      return res.status(400).json({ error: "Pick at least one gym for a gym account" });
    }

    // Don't let the last admin demote themselves out of existence.
    if (existing && existing.role === "admin" && role !== "admin"){
      const otherAdmins = users.filter(u => u.role === "admin" && u.id !== existing.id);
      if (otherAdmins.length === 0){
        return res.status(400).json({ error: "This is the only admin account — promote another admin first" });
      }
    }

    if (existing){
      existing.username = username;
      existing.role = role;
      existing.gyms = gyms;
      if (password){
        const { salt, hash } = hashPassword(password);
        existing.salt = salt;
        existing.hash = hash;
      }
    } else {
      const { salt, hash } = hashPassword(password);
      users.push({
        id: crypto.randomUUID(),
        username, role, gyms, salt, hash,
        createdAt: Date.now(),
        lastLogin: null
      });
    }

    try {
      await saveUsers(users);
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
    return res.status(200).json({ ok: true, users: users.map(publicUser) });
  }

  if (req.method === "DELETE"){
    const id = String((req.query && req.query.id) || "");
    const target = users.find(u => u.id === id);
    if (!target) return res.status(404).json({ error: "Account not found" });

    if (target.role === "admin" && users.filter(u => u.role === "admin").length === 1){
      return res.status(400).json({ error: "Can't delete the only admin account" });
    }

    const next = users.filter(u => u.id !== id);
    try {
      await saveUsers(next);
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
    return res.status(200).json({ ok: true, users: next.map(publicUser) });
  }

  res.setHeader("Allow", "GET, POST, DELETE");
  res.status(405).json({ error: "method not allowed" });
};
