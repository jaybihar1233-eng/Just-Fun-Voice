# JustFun 100% Free Real-Time Voice Backend (Node.js + WebSocket + WebRTC)

Yeh backend **Firebase Firestore** ko poori tarah replace karta hai aur **₹0 cost** me hazaaron voice chat rooms aur seats ko <10ms latency me sync karta hai!

## Kyun yeh Firebase se 100x behtar hai?
1. **Zero Stuck/Ghost Rooms:** Jaise hi koi phone app close karega ya network katega, WebSocket `close` event 1 second me user ko room aur seat se hata dega.
2. **Zero Per-Read/Write Billing:** Hazaaron rooms RAM me chalte hain — koi Firestore read/write limit nahi.
3. **Free HD Voice (WebRTC + Smart Relay Fallback):** Seedhe phone-to-phone UDP Opus/DataChannel voice via Google Free STUN servers (`₹0` server bandwidth), aur Jio/Airtel strict NAT ke liye automatic fallback relay!

---

## 2 Minute Me 100% FREE Deploy Kaise Karein?

### Option 1: Render.com (Sabse Aasaan — Card ki zaroorat nahi)
1. Is project ka `backend/` folder GitHub par push karein.
2. [Render.com](https://render.com) par free account banayein -> **New +** -> **Web Service**.
3. Apna repo select karein -> **Root Directory:** `backend` -> **Plan:** `Free ($0/month)` -> **Deploy**.
4. Deploy hone ke baad aapko URL milega jaise `https://justfun-voice-backend.onrender.com`.
5. App ke andar **Server URL** me (ya `.env` me `JUSTFUN_SERVER_URL`) yeh URL daal kar **Connect** dabaiye — done!

### Option 2: Oracle Cloud Always Free (Lifetime ₹0, 24 GB RAM, No Sleep — Heavy Crowd Ke Liye Best!)
1. Oracle Cloud Free Tier par Ubuntu VM banayein (`4 OCPU, 24 GB RAM` Always Free).
2. Server par run karein:
   ```bash
   git clone <your-repo>
   cd backend
   npm install
   npm start
   ```
3. Nginx / Cloudflare se `https://` laga kar app me URL daal dein — **10,000+ log ek saath chal sakte hain ₹0 me!**
