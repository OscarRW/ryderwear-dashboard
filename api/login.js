// POST /api/login  { username, password } → sets the signed `auth` cookie.
//
// Responds with the landing page for the account's role so the sign-in form
// knows where to send them: admins to the $ sales dashboard, gym accounts to
// the capacity dashboard (which never shows a dollar figure).

const { authenticate, signSession, sessionCookie, parseBody } = require("../lib/auth");

const FAILED_ATTEMPT_DELAY_MS = 250;
const sleep = ms => new Promise(r => setTimeout(r, ms));

const landingFor = role => (role === "admin" ? "/" : "/capacity.html");

module.exports = async function handler(req, res){
  if (req.method !== "POST"){
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "method not allowed" });
  }

  if (!process.env.AUTH_SECRET){
    return res.status(500).json({ error: "auth not configured" });
  }

  const body = parseBody(req);
  const username = body.username;
  const password = body.password;

  if (typeof username !== "string" || !username.trim() ||
      typeof password !== "string" || !password){
    return res.status(400).json({ error: "username and password required" });
  }

  let subject;
  try {
    subject = await authenticate(username, password);
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }

  if (!subject){
    // Blunt the edges of a password-guessing loop without pretending this is
    // a real rate limiter — serverless instances are too short-lived for that.
    await sleep(FAILED_ATTEMPT_DELAY_MS);
    return res.status(401).json({ error: "invalid username or password" });
  }

  res.setHeader("Set-Cookie", sessionCookie(signSession(subject)));
  res.status(200).json({
    ok: true,
    role: subject.role,
    bootstrap: !!subject.bootstrap,
    redirect: landingFor(subject.role)
  });
};
