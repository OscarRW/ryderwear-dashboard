export const config = {
  matcher: ['/((?!api/login|api/refresh|login\\.html|logo\\.png|favicon\\.ico).*)'],
};

export default async function middleware(request) {
  const cookie = request.headers.get('cookie') || '';
  const match = cookie.match(/(?:^|;\s*)auth=([^;]+)/);

  if (match && (await verifyToken(match[1]))) {
    return;
  }

  const url = new URL(request.url);

  if (url.pathname.startsWith('/api/')) {
    return new Response(JSON.stringify({ error: 'unauthorized' }), {
      status: 401,
      headers: { 'content-type': 'application/json' },
    });
  }

  return Response.redirect(new URL('/login.html', request.url), 302);
}

async function verifyToken(token) {
  const secret = process.env.AUTH_SECRET;
  if (!secret) return false;

  const parts = token.split('.');
  if (parts.length !== 2) return false;

  const [expiryStr, sigB64] = parts;
  const expiry = parseInt(expiryStr, 10);
  if (Number.isNaN(expiry) || expiry < Date.now()) return false;

  try {
    const key = await crypto.subtle.importKey(
      'raw',
      new TextEncoder().encode(secret),
      { name: 'HMAC', hash: 'SHA-256' },
      false,
      ['verify']
    );

    const sigBytes = Uint8Array.from(
      atob(sigB64.replace(/-/g, '+').replace(/_/g, '/')),
      (c) => c.charCodeAt(0)
    );

    return await crypto.subtle.verify(
      'HMAC',
      key,
      sigBytes,
      new TextEncoder().encode(expiryStr)
    );
  } catch {
    return false;
  }
}
