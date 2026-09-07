// Verifies the short-lived token the Next.js dashboard mints for itself at
// GET /api/user/mqtt/socket-token (see mqttcloud.ir/src/app/api/user/mqtt/socket-token/route.ts)
// so a browser can join its own room on this broker's Socket.IO server without
// either process needing to share session storage. Token shape:
//   base64url(JSON.stringify({ uid, exp })) + "." + hmacSha256(payload)
const crypto = require("crypto");

function verifySocketToken(token, secret) {
  if (!token || typeof token !== "string") return null;
  const [payloadB64, sig] = token.split(".");
  if (!payloadB64 || !sig) return null;

  const expected = crypto.createHmac("sha256", secret).update(payloadB64).digest("base64url");
  const a = Buffer.from(sig);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;

  try {
    const payload = JSON.parse(Buffer.from(payloadB64, "base64url").toString("utf8"));
    if (!payload.uid || !payload.exp || payload.exp < Date.now()) return null;
    return payload.uid;
  } catch {
    return null;
  }
}

module.exports = { verifySocketToken };
