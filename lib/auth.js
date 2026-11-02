// Shared auth for the Node API routes: KV access, the user store, password
// hashing and the signed session cookie.
//
// Two roles:
//   admin — full access: the $ sales dashboard, target entry, login management
//   gym   — capacity dashboard only, scoped to the gyms on their account, and
//           served percentages with no dollar figures anywhere
//
// A "gym" account scoped to every gym (gyms: "*") is the single shared
// all-gyms login; one scoped to a single label is that gym's own login.
//
// Users live in KV under `authUsers` so admins can add and remove logins
// without a redeploy. Before any user exists, the username `admin` with
// DASHBOARD_PASSWORD gets in, so there is always a way to bootstrap.
//
// Env vars required:
//   AUTH_SECRET          random string, signs the session cookie
//   DASHBOARD_PASSWORD   bootstrap admin password (used until a real admin
//                        account is created in the UI)
//   KV_REST_API_URL / KV_REST_API_TOKEN   (auto-set by Vercel KV)

const crypto = require("crypto");

const KV_USERS = "authUsers";
const SESSION_MAX_AGE_SEC = 30 * 24 * 60 * 60; // 30 days
const BOOTSTRAP_USERNAME = "admin";

// ── Vercel KV (REST, no SDK) ──────────────────────────────────────────────
async function kvGet(key){
  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) return null;
  const r = await fetch(`${url}/get/${encodeURIComponent(key)}`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!r.ok) return null;
  const { result } = await r.json();
  if (!result) return null;
  return typeof result === "string" ? JSON.parse(result) : result;
}

async function kvSet(key, value){
  const url = process.env.KV_REST_API_URL;
  const token = process.env.KV_REST_API_TOKEN;
  if (!url || !token) throw new Error("KV not configured");
  const r = await fetch(`${url}/set/${encodeURIComponent(key)}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify(value)
  });
  if (!r.ok) throw new Error(`KV set ${r.status}: ${await r.text().catch(()=>"")}`);
}

// ── Passwords ─────────────────────────────────────────────────────────────
function hashPassword(password, salt){
  const s = salt || crypto.randomBytes(16).toString("hex");
  const hash = crypto.scryptSync(String(password), s, 32).toString("hex");
  return { salt: s, hash };
}

function passwordMatches(password, salt, expectedHash){
  if (!salt || !expectedHash) return false;
  const { hash } = hashPassword(password, salt);
  const a = Buffer.from(hash);
  const b = Buffer.from(expectedHash);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

// Constant-time compare for the bootstrap password, which is stored in plain
// text in the environment rather than hashed in KV.
function plainMatches(a, b){
  const bufA = Buffer.from(String(a));
  const bufB = Buffer.from(String(b));
  return bufA.length === bufB.length && crypto.timingSafeEqual(bufA, bufB);
}

// ── User store ────────────────────────────────────────────────────────────
async function loadUsers(){
  const stored = await kvGet(KV_USERS);
  if (stored && Array.isArray(stored.users)) return stored.users;
  return [];
}

async function saveUsers(users){
  await kvSet(KV_USERS, { users, updatedAt: Date.now() });
}

function publicUser(u){
  return {
    id: u.id,
    username: u.username,
    role: u.role,
    gyms: u.gyms,
    createdAt: u.createdAt || null,
    lastLogin: u.lastLogin || null
  };
}

// Resolve a username + password to a session subject, or null. Falls back to
// the bootstrap admin only while the user store is empty, so the env password
// stops working the moment real accounts exist.
async function authenticate(username, password){
  const users = await loadUsers();
  const uname = String(username || "").trim().toLowerCase();
  if (!uname || !password) return null;

  if (users.length === 0){
    const boot = process.env.DASHBOARD_PASSWORD;
    if (boot && uname === BOOTSTRAP_USERNAME && plainMatches(password, boot)){
      return { username: BOOTSTRAP_USERNAME, role: "admin", gyms: "*", bootstrap: true };
    }
    return null;
  }

  const user = users.find(u => u.username === uname);
  if (!user) return null;
  if (!passwordMatches(password, user.salt, user.hash)) return null;

  user.lastLogin = Date.now();
  await saveUsers(users).catch(()=>{}); // a failed stamp shouldn't block sign-in
  return { username: user.username, role: user.role, gyms: user.gyms };
}

// ── Session token: base64url(payload).base64url(hmac) ──────────────────────
function b64url(buf){
  return Buffer.from(buf).toString("base64url");
}

function signSession(subject){
  const secret = process.env.AUTH_SECRET;
  if (!secret) throw new Error("AUTH_SECRET not configured");
  const payload = {
    u: subject.username,
    r: subject.role,
    g: subject.gyms,
    exp: Date.now() + SESSION_MAX_AGE_SEC * 1000
  };
  const body = b64url(JSON.stringify(payload));
  const sig = crypto.createHmac("sha256", secret).update(body).digest("base64url");
  return `${body}.${sig}`;
}

function verifySession(token){
  const secret = process.env.AUTH_SECRET;
  if (!secret || !token) return null;
  const parts = String(token).split(".");
  if (parts.length !== 2) return null;
  const [body, sig] = parts;

  const expected = crypto.createHmac("sha256", secret).update(body).digest("base64url");
  const a = Buffer.from(sig);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;

  let payload;
  try { payload = JSON.parse(Buffer.from(body, "base64url").toString("utf8")); }
  catch { return null; }
  if (!payload || typeof payload.exp !== "number" || payload.exp < Date.now()) return null;

  return { username: payload.u, role: payload.r, gyms: payload.g };
}

function sessionCookie(token){
  return `auth=${token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=${SESSION_MAX_AGE_SEC}`;
}

function clearedCookie(){
  return "auth=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0";
}

function readCookie(req, name){
  const raw = req.headers.cookie || "";
  const match = raw.match(new RegExp(`(?:^|;\\s*)${name}=([^;]+)`));
  return match ? match[1] : null;
}

// Reads the session off the request. Returns null when there isn't a valid one.
function getSession(req){
  return verifySession(readCookie(req, "auth"));
}

// Route guard. Responds 401/403 itself and returns null when access is denied,
// so handlers can `const s = requireSession(req, res); if (!s) return;`
function requireSession(req, res, { adminOnly = false } = {}){
  const session = getSession(req);
  if (!session){
    res.status(401).json({ error: "unauthorized" });
    return null;
  }
  if (adminOnly && session.role !== "admin"){
    res.status(403).json({ error: "forbidden" });
    return null;
  }
  return session;
}

// The gyms a session may see. "*" means every gym on the roster.
function scopeGyms(session, allGyms){
  if (!session) return [];
  if (session.role === "admin" || session.gyms === "*") return allGyms.slice();
  const allowed = Array.isArray(session.gyms) ? session.gyms : [session.gyms];
  return allGyms.filter(g => allowed.includes(g));
}

function parseBody(req){
  let body = req.body;
  if (typeof body === "string"){
    try { body = JSON.parse(body); } catch { body = {}; }
  }
  return body && typeof body === "object" ? body : {};
}

module.exports = {
  KV_USERS,
  SESSION_MAX_AGE_SEC,
  BOOTSTRAP_USERNAME,
  kvGet,
  kvSet,
  hashPassword,
  passwordMatches,
  loadUsers,
  saveUsers,
  publicUser,
  authenticate,
  signSession,
  verifySession,
  sessionCookie,
  clearedCookie,
  getSession,
  requireSession,
  scopeGyms,
  parseBody
};
