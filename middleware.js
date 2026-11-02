// Edge auth gate.
//
// Everything is behind the signed `auth` cookie except the sign-in page and
// the cron endpoint (which carries its own CRON_SECRET). On top of that, a
// "gym" account is confined to the capacity dashboard and the percentage-only
// endpoint that feeds it — the $ sales dashboard, the target sheet and the
// login manager are admin-only.
//
// Handlers re-check the session themselves via api/_auth.js; this is the
// outer gate, not the only one.

export const config = {
  matcher: ['/((?!api/login|api/refresh|login\\.html|logo\\.png|favicon\\.ico).*)'],
};

// Paths only an admin may reach. Pages redirect, API routes get a 403.
const ADMIN_ONLY_PREFIXES = [
  '/api/data',
  '/api/cin7',
  '/api/targets',
  '/api/users',
  '/api/types',
  '/api/diagnose-history',
  '/admin.html',
];

const ADMIN_ONLY_EXACT = ['/', '/index.html'];

const GYM_LANDING = '/capacity.html';

function isAdminOnly(pathname){
  if (ADMIN_ONLY_EXACT.includes(pathname)) return true;
  return ADMIN_ONLY_PREFIXES.some(p => pathname === p || pathname.startsWith(p + '/'));
}

export default async function middleware(request){
  const cookie = request.headers.get('cookie') || '';
  const match = cookie.match(/(?:^|;\s*)auth=([^;]+)/);
  const session = match ? await verifyToken(match[1]) : null;
  const url = new URL(request.url);
  const isApi = url.pathname.startsWith('/api/');

  if (!session){
    if (isApi){
      return new Response(JSON.stringify({ error: 'unauthorized' }), {
        status: 401,
        headers: { 'content-type': 'application/json' },
      });
    }
    return Response.redirect(new URL('/login.html', request.url), 302);
  }

  if (session.role !== 'admin' && isAdminOnly(url.pathname)){
    if (isApi){
      return new Response(JSON.stringify({ error: 'forbidden' }), {
        status: 403,
        headers: { 'content-type': 'application/json' },
      });
    }
    return Response.redirect(new URL(GYM_LANDING, request.url), 302);
  }

  return;
}

function base64UrlToBytes(value){
  const padded = value.replace(/-/g, '+').replace(/_/g, '/');
  const binary = atob(padded + '='.repeat((4 - (padded.length % 4)) % 4));
  return Uint8Array.from(binary, c => c.charCodeAt(0));
}

// Mirrors signSession() in api/_auth.js: base64url(payload).base64url(hmac)
async function verifyToken(token){
  const secret = process.env.AUTH_SECRET;
  if (!secret) return null;

  const parts = token.split('.');
  if (parts.length !== 2) return null;
  const [body, sig] = parts;

  try {
    const key = await crypto.subtle.importKey(
      'raw',
      new TextEncoder().encode(secret),
      { name: 'HMAC', hash: 'SHA-256' },
      false,
      ['verify']
    );

    const ok = await crypto.subtle.verify(
      'HMAC',
      key,
      base64UrlToBytes(sig),
      new TextEncoder().encode(body)
    );
    if (!ok) return null;

    const payload = JSON.parse(new TextDecoder().decode(base64UrlToBytes(body)));
    if (!payload || typeof payload.exp !== 'number' || payload.exp < Date.now()) return null;

    return { username: payload.u, role: payload.r, gyms: payload.g };
  } catch {
    return null;
  }
}
