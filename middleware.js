// Edge auth gate and address routing.
//
// Everything is behind the signed `auth` cookie except the sign-in page and
// the cron endpoint (which carries its own CRON_SECRET). On top of that, a
// "gym" account is confined to the capacity dashboard and the percentage-only
// endpoint that feeds it — the $ sales dashboard, the target sheet and the
// login manager are admin-only.
//
// The two dashboards are served as separate addresses off one deployment.
// Set TARGETS_HOST and SALES_HOST to the two hostnames, e.g.
// "targets.ryderwear.com.au" and "sales.ryderwear.com.au". Each address then
// answers only for its own pages and sends the other's traffic across.
//
// A hostname that matches neither — a preview deployment, or either address
// before the domains are configured — serves both dashboards together and
// never redirects across. That keeps previews self-contained instead of
// bouncing testers onto production.
//
// Handlers re-check the session themselves via lib/auth.js; this is the
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

// The sales dashboard. Lives on every hostname except TARGETS_HOST.
const SALES_PATHS = ['/', '/index.html'];

// Target tracking. Lives on TARGETS_HOST when one is configured.
const TARGETS_PATHS = ['/capacity.html', '/admin.html'];

const GYM_LANDING = '/capacity.html';

function isAdminOnly(pathname){
  if (SALES_PATHS.includes(pathname)) return true;
  return ADMIN_ONLY_PREFIXES.some(p => pathname === p || pathname.startsWith(p + '/'));
}

function hostOf(request){
  return (request.headers.get('host') || '').toLowerCase().split(':')[0];
}

function envHost(name){
  return (process.env[name] || '').toLowerCase().trim().split(':')[0];
}

// Which of the two addresses this request arrived on — or neither, for a
// preview deployment or an address that hasn't been configured yet.
function addressRole(request){
  const host = hostOf(request);
  const targets = envHost('TARGETS_HOST');
  const sales = envHost('SALES_HOST');
  if (targets && host === targets) return 'targets';
  if (sales && host === sales) return 'sales';
  return 'unknown';
}

// Where a gym account belongs. Only crosses addresses from the sales one,
// so an unrecognised host keeps traffic where it is.
function gymLanding(request, role){
  const targets = envHost('TARGETS_HOST');
  if (role === 'sales' && targets){
    return new URL(GYM_LANDING, `https://${targets}`);
  }
  return new URL(GYM_LANDING, request.url);
}

export default async function middleware(request){
  const cookie = request.headers.get('cookie') || '';
  const match = cookie.match(/(?:^|;\s*)auth=([^;]+)/);
  const session = match ? await verifyToken(match[1]) : null;
  const url = new URL(request.url);
  const isApi = url.pathname.startsWith('/api/');
  const role = addressRole(request);

  if (!session){
    if (isApi){
      return new Response(JSON.stringify({ error: 'unauthorized' }), {
        status: 401,
        headers: { 'content-type': 'application/json' },
      });
    }
    return Response.redirect(new URL('/login.html', request.url), 302);
  }

  // Address routing. Each hostname answers only for its own dashboard and
  // hands the other's pages over to where they live. APIs are shared by both,
  // so they're left alone, and an unrecognised host routes nothing.
  if (!isApi){
    if (role === 'targets' && SALES_PATHS.includes(url.pathname)){
      return Response.redirect(new URL(GYM_LANDING, request.url), 302);
    }
    if (role === 'sales' && TARGETS_PATHS.includes(url.pathname)){
      const to = new URL(url.pathname, `https://${envHost('TARGETS_HOST')}`);
      to.search = url.search;
      return Response.redirect(to, 302);
    }
  }

  if (session.role !== 'admin' && isAdminOnly(url.pathname)){
    if (isApi){
      return new Response(JSON.stringify({ error: 'forbidden' }), {
        status: 403,
        headers: { 'content-type': 'application/json' },
      });
    }
    return Response.redirect(gymLanding(request, role), 302);
  }

  return;
}

function base64UrlToBytes(value){
  const padded = value.replace(/-/g, '+').replace(/_/g, '/');
  const binary = atob(padded + '='.repeat((4 - (padded.length % 4)) % 4));
  return Uint8Array.from(binary, c => c.charCodeAt(0));
}

// Mirrors signSession() in lib/auth.js: base64url(payload).base64url(hmac)
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
