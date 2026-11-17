const express = require('express');
const http = require('http');
const cors = require('cors');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');
let profileAndMedalsModule;
try {
  profileAndMedalsModule = require('./modules/profileAndMedalsModule');
} catch (_) {
  try {
    profileAndMedalsModule = require('./backend/modules/profileAndMedalsModule');
  } catch (__) {
    profileAndMedalsModule = null;
  }
}
let firestoreStore;
try {
  firestoreStore = require('./firestoreStore');
} catch (err) {
  try {
    firestoreStore = require('./backend/firestoreStore');
  } catch (_) {
    console.warn('[Server] firestoreStore.js missing in deploy directory — running in standby fallback mode.');
    firestoreStore = {
      bindStateRefs: () => {},
      hydrateFromCloud: async () => {},
      flushDirtyState: async () => {},
      markUserDirty: () => {},
      markRoomDirty: () => {},
      markDeviceDirty: () => {},
      markDmDirty: () => {},
      markGlobalDirty: () => {},
      signCloudinaryUpload: () => { throw new Error('firestoreStore.js missing'); },
      uploadMediaToCloudinary: async () => { throw new Error('firestoreStore.js missing'); },
      saveGiftToFirestoreCollection: async () => { throw new Error('firestoreStore.js missing'); },
      deleteGiftFromFirestoreCollection: async () => {},
      getStatus: () => ({
        enabled: false,
        mode: 'standby_missing_module',
        error: 'firestoreStore.js file not uploaded alongside server.js yet',
      }),
    };
  }
}

function getLocalCloudinaryConfig() {
  const cleanVal = (v) =>
    String(v || '')
      .trim()
      .replace(/^['"]+|['"]+$/g, '')
      .trim();

  let cloudName = cleanVal(process.env.CLOUDINARY_CLOUD_NAME || process.env.CLOUD_NAME);
  let apiKey = cleanVal(process.env.CLOUDINARY_API_KEY || process.env.CLOUDINARY_KEY || process.env.API_KEY);
  let apiSecret = cleanVal(
    process.env.CLOUDINARY_API_SECRET || process.env.CLOUDINARY_SECRET || process.env.API_SECRET
  );

  let rawUrl = cleanVal(process.env.CLOUDINARY_URL);
  if (rawUrl.toUpperCase().startsWith('CLOUDINARY_URL=')) {
    rawUrl = cleanVal(rawUrl.slice('CLOUDINARY_URL='.length));
  }
  if (!rawUrl && cloudName.startsWith('cloudinary://')) {
    rawUrl = cloudName;
    cloudName = '';
  }
  if (rawUrl.startsWith('cloudinary://')) {
    try {
      const parsed = new URL(rawUrl);
      if (!cloudName && parsed.hostname) cloudName = decodeURIComponent(parsed.hostname);
      if (!apiKey && parsed.username) apiKey = decodeURIComponent(parsed.username);
      if (!apiSecret && parsed.password) apiSecret = decodeURIComponent(parsed.password);
    } catch (_) {}
  }
  const baseFolder =
    cleanVal(process.env.CLOUDINARY_UPLOAD_FOLDER || 'justfun/gifts').replace(/^\/+|\/+$/g, '') ||
    'justfun/gifts';
  return {
    configured: Boolean(cloudName && apiKey && apiSecret),
    cloudName,
    apiKey,
    apiSecret,
    baseFolder,
  };
}

function signCloudinaryUploadLocal({ kind = 'video' } = {}) {
  if (firestoreStore && typeof firestoreStore.signCloudinaryUpload === 'function') {
    return firestoreStore.signCloudinaryUpload({ kind });
  }
  const cfg = getLocalCloudinaryConfig();
  if (!cfg.configured) {
    throw new Error(
      'Cloudinary is not configured on the server. Please set CLOUDINARY_URL (or CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET) in environment variables.'
    );
  }
  const isVideo = String(kind).toLowerCase() === 'video';
  const resourceType = isVideo ? 'video' : 'image';
  const folder = isVideo ? `${cfg.baseFolder}/videos` : `${cfg.baseFolder}/icons`;
  const publicId = crypto.randomUUID();
  const timestamp = Math.floor(Date.now() / 1000);
  const paramsToSign = `folder=${folder}&public_id=${publicId}&timestamp=${timestamp}`;
  const signature = crypto
    .createHash('sha1')
    .update(paramsToSign + cfg.apiSecret)
    .digest('hex');
  return {
    cloudName: cfg.cloudName,
    apiKey: cfg.apiKey,
    timestamp,
    signature,
    folder,
    publicId,
    resourceType,
    uploadUrl: `https://api.cloudinary.com/v1_1/${encodeURIComponent(cfg.cloudName)}/${resourceType}/upload`,
  };
}

async function uploadMediaToCloudinaryLocal({ buffer, kind = 'video', contentType }) {
  if (firestoreStore && typeof firestoreStore.uploadMediaToCloudinary === 'function') {
    return firestoreStore.uploadMediaToCloudinary({ buffer, kind, contentType });
  }
  const sign = signCloudinaryUploadLocal({ kind });
  const form = new FormData();
  const ext = sign.resourceType === 'video' ? 'mp4' : 'png';
  const mime = contentType || (sign.resourceType === 'video' ? 'video/mp4' : 'image/png');
  form.append('file', new Blob([buffer], { type: mime }), `${sign.publicId}.${ext}`);
  form.append('api_key', sign.apiKey);
  form.append('timestamp', String(sign.timestamp));
  form.append('signature', sign.signature);
  form.append('folder', sign.folder);
  form.append('public_id', sign.publicId);

  const response = await fetch(sign.uploadUrl, { method: 'POST', body: form });
  const data = await response.json().catch(() => ({}));
  if (!response.ok || data.error) {
    throw new Error(data?.error?.message || `Cloudinary upload failed (HTTP ${response.status})`);
  }
  return {
    downloadUrl: data.secure_url || data.url,
    publicId: data.public_id,
    resourceType: sign.resourceType,
  };
}
const PORT = process.env.PORT || 8080;

const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));

// ============================================================================
// HIGH-SCALE IN-MEMORY STATE (O(1) lookup, supports 10,000+ rooms concurrently)
// ============================================================================
// rooms: Map<roomId, RoomState>
const rooms = new Map();
// users: Map<userId, UserProfile>
const users = new Map();
// homeSubscribers: Set<WebSocket> (clients currently watching Popular / Mine tabs)
const homeSubscribers = new Set();
// clientMeta: WeakMap<WebSocket, { userId, roomId, missedPings }>
const clientMeta = new WeakMap();
// feedbacks: Array<FeedbackTicket>
const feedbacks = [];
// pendingDisconnectTimers: Map<`${roomId}:${userId}`, Timeout>
const pendingDisconnectTimers = new Map();

// ============================================================================
// SOCIAL GRAPH, VISITORS, GLOBAL SEARCH & 1-ON-1 PERSONAL MESSAGES (DM)
// ============================================================================
// userSockets: Map<userId, Set<WebSocket>>
const userSockets = new Map();
// userFollowers: Map<userId, Set<followerUserId>>
const userFollowers = new Map();
// userFollowing: Map<userId, Set<targetUserId>>
const userFollowing = new Map();
// userVisitors: Map<userId, Array<{ userId, nickname, avatarUri, gender, level, visitedAt }>>
const userVisitors = new Map();
// dmThreads: Map<threadKey, Array<DmMessage>> where threadKey = [u1, u2].sort().join('_')
const dmThreads = new Map();
// dmUnreadCounts: Map<`${viewerUserId}:${peerUserId}`, number>
const dmUnreadCounts = new Map();
// blockedUsers: Map<userId, Map<blockedUserId, { userId, nickname, avatarUri, blockedAtMs }>>
const blockedUsers = new Map();
// userDmPrivacy: Map<userId, 'ALL' | 'FOLLOWING'>
const userDmPrivacy = new Map();
// userFollowAlerts: Map<userId, Array<{ fromUserId, fromName, fromAvatarUri, timestampMs, isRead }>>
const userFollowAlerts = new Map();
// userSystemNotices: Map<userId, Array<{ id, title, content, subTitle, actionText, createdAtMs, isRead }>>
const userSystemNotices = new Map();
let MAX_UNFOLLOWED_DM_MESSAGES = 5;
// reports: Array<ReportRecord>
const reports = [];
// Admin Ban & Dynamic Catalog State
const ADMIN_SECRET = process.env.ADMIN_SECRET || 'justfun-admin-2026';
const bannedUserIds = new Map(); // Map<userId, { reason, bannedAtMs }>
const bannedDeviceKeys = new Map(); // Map<deviceKey, { reason, bannedAtMs }>
const LEGACY_BUILTIN_GIFT_IDS = new Set([
  'g1', 'g2', 'g3', 'g4', 'g5', 'g6', 'g7', 'g8', 'g9',
  'g_rose_3d', 'g_kiss', 'g_sports_car', 'g_golden_crown', 'g_private_jet',
  'g_vip_halo', 'g_cyber_light', 'g_neon_galaxy', 'g_vip_fly',
]);
const DYNAMIC_GIFTS_CATALOG = new Map();
const DYNAMIC_MEDALS_CATALOG = new Map();
let giftsCustomizedByAdmin = true;
let storeCustomizedByAdmin = false;
const DYNAMIC_EVENTS_CATALOG = new Map([
  ['ev_anniv', { id: 'ev_anniv', title: '🎆 3rd Anniversary Party', subtitle: "Let's enjoy the 3rd anniversary fireworks show & live party together!", bannerUrl: '', createdAtMs: Date.now() }],
  ['ev_daily', { id: 'ev_daily', title: '🎁 Daily Sign-In & Lucky Box', subtitle: 'Log in every day and chat in rooms to collect free coins!', bannerUrl: '', createdAtMs: Date.now() }],
]);

function formatNoticeDate(ms = Date.now()) {
  const d = new Date(ms);
  return `${d.getDate()}/${d.getMonth() + 1} ${d.getFullYear()} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}

function getOrInitSystemNotices(uid) {
  const key = String(uid || '');
  if (!key) return [];
  let list = userSystemNotices.get(key);
  if (!list) {
    const u = users.get(key) || {};
    const name = u.nickname || `User ${key}`;
    const now = Date.now();
    list = [
      {
        id: `sys_welcome_${key}`,
        title: 'Welcome to Just Fun Live!',
        subTitle: formatNoticeDate(now),
        content: `Hello ${name}! Welcome to Just Fun Voice Party. Follow friends mutually to unlock unlimited real-time 1-on-1 messages, voice clips, photos, and gifts!`,
        actionText: 'Explore Rooms',
        createdAtMs: now,
        isRead: false,
      },
    ];
    userSystemNotices.set(key, list);
  }
  return list;
}

function pushSystemNotificationToUser(uid, title, content, actionText = 'Go to check') {
  const key = String(uid || '');
  if (!key) return;
  const list = getOrInitSystemNotices(key);
  const now = Date.now();
  const item = {
    id: `notice_${now}_${Math.random().toString(36).slice(2, 6)}`,
    title,
    subTitle: formatNoticeDate(now),
    content,
    actionText,
    createdAtMs: now,
    isRead: false,
  };
  list.unshift(item);
  if (list.length > 50) list.pop();
  sendToUser(key, {
    type: 'SYSTEM_NOTIFICATION',
    notice: item,
  });
}

// ============================================================================
// ZERO-LOAD ANTI-CHEAT GREEDY BABY ENGINE (SYNCED ACROSS ALL CHAT ROOMS)
// ============================================================================
const GREEDY_EPOCH_MS = 1730000000000;
const GREEDY_ROUND_MS = 40000;
const GREEDY_BET_CUTOFF_MS = 29500;
const GREEDY_SLOT_MULTIPLIERS = [5, 5, 5, 5, 10, 15, 25, 45];
const GREEDY_ALLOWED_CHIPS = new Set([100, 1000, 10000, 50000, 100000, 200000, 500000, 1000000, 5000000]);
const GREEDY_INITIAL_COINS = 0;
const GREEDY_MAX_BALANCE = 500000000;
const GREEDY_MAX_BETS_PER_ROUND = 400;
const WALLET_RECHARGE_PACKS = {
  p1: { coins: 800000, priceInr: 100 },
  p2: { coins: 3549000, priceInr: 500 },
  p3: { coins: 7099000, priceInr: 1000 },
  p4: { coins: 17290000, priceInr: 2500 },
};
const WALLET_EXCHANGE_DIAMOND_COST = {
  330: 1000,
  3300: 10000,
  33000: 100000,
  330000: 1000000,
  3300000: 10000000,
};
let DAILY_CHECKIN_REWARD_COINS = 10;
const WALLET_RECHARGE_COOLDOWN_MS = 4000;
const GREEDY_HMAC_SECRET = process.env.GREEDY_HMAC_SECRET || crypto.randomBytes(32).toString('hex');

const greedySubscribers = new Set(); // Only sockets actively viewing Greedy Baby
const greedyRoundBets = new Map();   // Map<roundNumber, Map<userId, { userId, nickname, avatarUri, slotBets, updatedAt }>>
const greedyRoundSlotTotals = new Map(); // Map<roundNumber, number[8]> O(1) running slot totals
const greedyRecentChips = new Map(); // Map<roundNumber, Array<{ slotId, chipIndex, userId }>>
const greedySettlements = new Map(); // Map<roundNumber, CachedSettlement>
const greedyUserBalances = new Map(); // Map<userId, number> (Server-authoritative unified Wallet + Game coins)
const userDiamonds = new Map();       // Map<userId, number> (Server-authoritative Diamonds earned from gifts)
const walletTransactions = new Map(); // Map<userId, Array<{ id, type, title, amountText, date, createdAtMs, isAddition }>>
const walletDailyCheckIns = new Map(); // Map<userId, string(YYYY-MM-DD)>
const deviceDailyCheckIns = new Map(); // Map<deviceKey, string(YYYY-MM-DD)>
const walletProcessedTxIds = new Map(); // Map<txId, { userId, amount, remainingGiftCoins, createdAtMs }>
const walletLastRechargeAt = new Map(); // Map<userId, number>
const greedyRoundBetCounts = new Map(); // Map<roundNumber, Map<userId, number>>
const greedyLastBetAtMs = new Map(); // Map<userId, number>
const greedyBetRecords = new Map(); // Map<userId, Array<{ roundNumber, settleTimeMs, betsBySlot, winningFoodId, totalCost, totalWin, afterBalance }>>
const GREEDY_RECORD_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const GREEDY_RECORD_MAX_COUNT = 200;

function getPrunedGreedyBetRecords(uid, nowMs = Date.now()) {
  if (!uid) return [];
  const raw = greedyBetRecords.get(String(uid)) || [];
  const cutoff = nowMs - GREEDY_RECORD_RETENTION_MS;
  const filtered = raw
    .filter((rec) => rec && Number(rec.settleTimeMs || 0) >= cutoff && Number(rec.totalCost || 0) > 0)
    .sort((a, b) => Number(b.settleTimeMs || 0) - Number(a.settleTimeMs || 0))
    .slice(0, GREEDY_RECORD_MAX_COUNT);
  if (filtered.length !== raw.length) {
    greedyBetRecords.set(String(uid), filtered);
  }
  return filtered;
}

function appendGreedyBetRecord(uid, record) {
  const key = String(uid || '');
  if (!key || !record || Number(record.totalCost || 0) <= 0) return;
  const nowMs = Date.now();
  const existing = getPrunedGreedyBetRecords(key, nowMs);
  if (existing.some((r) => Number(r.roundNumber) === Number(record.roundNumber))) {
    return;
  }
  const next = [record, ...existing].slice(0, GREEDY_RECORD_MAX_COUNT);
  greedyBetRecords.set(key, next);
}
let greedyCrowdBatch = null; // { roundNumber, events: Array<{ userId, slotIds, chipIndex }>, slotTotals, singleSourceWs }
let greedyCrowdFlushTimer = null;

// ============================================================================
// REAL-TIME ANTI-CHEAT MASTI STORE & MY ITEMS ("MINE") ENGINE
// ============================================================================
const STORE_DAY_MS = 24 * 60 * 60 * 1000;
const STORE_DURATION_MULT = {
  7: 1.0,
  15: 1.8,
  30: 3.0,
};
const STORE_CATALOG = {
  // VEHICLE
  v_phantom_car: { id: 'v_phantom_car', name: 'Phantom Supercar', subtitle: 'Royal V12 Roar & Golden Drift Entrance', category: 'VEHICLE', basePrice7Days: 15000, rarityStars: 3, hasAnimationPlay: true, active: true },
  v_silver_roadster: { id: 'v_silver_roadster', name: 'Silver Roadster', subtitle: 'Lightning Speedster Room Entry Effect', category: 'VEHICLE', basePrice7Days: 10000, rarityStars: 2, hasAnimationPlay: true, active: true },
  v_royal_crown_ride: { id: 'v_royal_crown_ride', name: 'Imperial Crown Chariot', subtitle: "King's Golden Crown Room Entrance", category: 'VEHICLE', basePrice7Days: 25000, rarityStars: 3, hasAnimationPlay: true, active: true },
  v_starlight_cruiser: { id: 'v_starlight_cruiser', name: 'Starlight Cruiser', subtitle: 'Neon Galaxy Trail Room Entrance', category: 'VEHICLE', basePrice7Days: 12000, rarityStars: 2, hasAnimationPlay: true, active: true },
  v_supersonic_jet: { id: 'v_supersonic_jet', name: 'Golden Sky Jet', subtitle: 'VIP Supersonic Jet Fly-In Effect', category: 'VEHICLE', basePrice7Days: 30000, rarityStars: 3, hasAnimationPlay: true, active: true },
  v_cupid_heart: { id: 'v_cupid_heart', name: 'Lover Heart Float', subtitle: 'Romantic Pink Petals Entrance', category: 'VEHICLE', basePrice7Days: 8000, rarityStars: 1, hasAnimationPlay: true, active: true },
  // FRAME
  f_3rd_anniversary: { id: 'f_3rd_anniversary', name: '3rd Anniversary Frame', subtitle: 'Exclusive 3-Star Golden Wing & Cake Frame', category: 'FRAME', basePrice7Days: 12000, rarityStars: 3, hasAnimationPlay: false, active: true },
  f_charm_crescent: { id: 'f_charm_crescent', name: 'Charm Star Frame', subtitle: 'Golden Crescent Moon & Starlight Rim', category: 'FRAME', basePrice7Days: 6000, rarityStars: 1, hasAnimationPlay: false, active: true },
  f_cp_lover: { id: 'f_cp_lover', name: 'CP Lover Lv.1 Frame', subtitle: 'Rose Gold Couple & Emerald Gem Frame', category: 'FRAME', basePrice7Days: 9000, rarityStars: 2, hasAnimationPlay: false, active: true },
  f_ornate_gold: { id: 'f_ornate_gold', name: 'Royal Ornate Gold', subtitle: 'Supreme Noble Gold Filigree Frame', category: 'FRAME', basePrice7Days: 18000, rarityStars: 3, hasAnimationPlay: false, active: true },
  // BUBBLE
  b_purple_panther: { id: 'b_purple_panther', name: 'Purple Panther Bubble', subtitle: 'Fierce Neon Panther Room & Chat Frame', category: 'BUBBLE', basePrice7Days: 8000, rarityStars: 3, hasAnimationPlay: false, sampleBubbleText: 'Welcome to Just Fun Room! 🎤', active: true },
  b_starlight_galaxy: { id: 'b_starlight_galaxy', name: 'Starlight Galaxy Bubble', subtitle: 'Dreamy Purple Starry Sky Chat Box', category: 'BUBBLE', basePrice7Days: 5000, rarityStars: 2, hasAnimationPlay: false, sampleBubbleText: 'Hello friends! Join the mic 🎤✨', active: true },
  b_golden_combo: { id: 'b_golden_combo', name: 'Imperial Gold Bubble', subtitle: 'Golden Rich Supporter Chat Highlight', category: 'BUBBLE', basePrice7Days: 10000, rarityStars: 3, hasAnimationPlay: false, sampleBubbleText: 'VIP Boss is in the house 👑🔥', active: true },
  b_cp_romance: { id: 'b_cp_romance', name: 'CP Sweetheart Bubble', subtitle: 'Pink & Gold Couple Chat Style', category: 'BUBBLE', basePrice7Days: 6500, rarityStars: 2, hasAnimationPlay: false, sampleBubbleText: 'Best Friends Forever ❤️✨', active: true },
  // MIC_WAVE
  w_emerald_pulse: { id: 'w_emerald_pulse', name: 'Emerald Neon Wave', subtitle: 'Glowing Mint-Emerald Sound Rings on Mic', category: 'MIC_WAVE', basePrice7Days: 6000, rarityStars: 2, hasAnimationPlay: true, micWaveColorHex: 0xFF19CC9D, active: true },
  w_golden_crown_wave: { id: 'w_golden_crown_wave', name: 'Royal Gold Wave', subtitle: 'Imperial Golden Sound Pulse on Room Seat', category: 'MIC_WAVE', basePrice7Days: 12000, rarityStars: 3, hasAnimationPlay: true, micWaveColorHex: 0xFFFFB300, active: true },
  w_cyber_violet_wave: { id: 'w_cyber_violet_wave', name: 'Cyber Violet Wave', subtitle: 'Electric Purple DJ Sound Rings on Mic', category: 'MIC_WAVE', basePrice7Days: 8500, rarityStars: 2, hasAnimationPlay: true, micWaveColorHex: 0xFFAA00FF, active: true },
  w_rose_lover_wave: { id: 'w_rose_lover_wave', name: 'Romantic Rose Wave', subtitle: 'Sweet Pink Heartbeat Mic Aura', category: 'MIC_WAVE', basePrice7Days: 7500, rarityStars: 2, hasAnimationPlay: true, micWaveColorHex: 0xFFFF4081, active: true },
  // SPECIAL_ID
  sid_888888: { id: 'sid_888888', name: 'SID: 888888', subtitle: 'Supreme Six-8s Wealth VIP ID Badge', category: 'SPECIAL_ID', basePrice7Days: 50000, rarityStars: 3, hasAnimationPlay: false, specialIdNumber: '888888', active: true },
  sid_999999: { id: 'sid_999999', name: 'SID: 999999', subtitle: 'Emperor 999999 Eternal Honour ID', category: 'SPECIAL_ID', basePrice7Days: 50000, rarityStars: 3, hasAnimationPlay: false, specialIdNumber: '999999', active: true },
  sid_777777: { id: 'sid_777777', name: 'SID: 777777', subtitle: 'Lucky Jackpot 777777 Golden ID', category: 'SPECIAL_ID', basePrice7Days: 35000, rarityStars: 3, hasAnimationPlay: false, specialIdNumber: '777777', active: true },
  sid_5201314: { id: 'sid_5201314', name: 'ID: 5201314', subtitle: 'Forever Love Special Charm ID', category: 'SPECIAL_ID', basePrice7Days: 20000, rarityStars: 2, hasAnimationPlay: false, specialIdNumber: '5201314', active: true },
  sid_100001: { id: 'sid_100001', name: 'ID: 100001', subtitle: 'No.1 Top Host Pioneer Charm ID', category: 'SPECIAL_ID', basePrice7Days: 15000, rarityStars: 2, hasAnimationPlay: false, specialIdNumber: '100001', active: true },
  sid_112233: { id: 'sid_112233', name: 'ID: 112233', subtitle: 'Double Step Lucky Number ID', category: 'SPECIAL_ID', basePrice7Days: 10000, rarityStars: 1, hasAnimationPlay: false, specialIdNumber: '112233', active: true },
};
const userStoreInventories = new Map(); // Map<userId, { items: Map<itemId, ...>, equipped: { VEHICLE, FRAME, BUBBLE, MIC_WAVE, SPECIAL_ID } }>
const storeProcessedOrderIds = new Map(); // Map<orderId, number>
const storeLastActionAt = new Map(); // Map<userId, number>
const STORE_ACTION_COOLDOWN_MS = 1200;

function getOrCreateStoreInventory(uid) {
  let inv = userStoreInventories.get(uid);
  if (!inv) {
    inv = {
      items: new Map(),
      equipped: {
        VEHICLE: null,
        FRAME: null,
        BUBBLE: null,
        MIC_WAVE: null,
        SPECIAL_ID: null,
      },
    };
    userStoreInventories.set(uid, inv);
  }
  return inv;
}

function pruneAndSyncUserStore(uid, nowMs = Date.now()) {
  const inv = getOrCreateStoreInventory(uid);
  for (const [itemId, entry] of inv.items.entries()) {
    if (!entry || Number(entry.expiresAtMs || 0) <= nowMs || !STORE_CATALOG[itemId]) {
      inv.items.delete(itemId);
    }
  }
  for (const cat of Object.keys(inv.equipped)) {
    const eqId = inv.equipped[cat];
    if (eqId && !inv.items.has(eqId)) {
      inv.equipped[cat] = null;
    }
  }
  const prof = users.get(uid);
  if (prof) {
    const eqVehicleId = inv.equipped.VEHICLE || '';
    const eqVehicleSpec = eqVehicleId ? STORE_CATALOG[eqVehicleId] : null;
    const eqSidId = inv.equipped.SPECIAL_ID || '';
    const eqSidSpec = eqSidId ? STORE_CATALOG[eqSidId] : null;
    prof.equippedFrameId = inv.equipped.FRAME || '';
    prof.equippedVehicleId = eqVehicleId;
    prof.equippedVehicleName = eqVehicleSpec ? eqVehicleSpec.name : '';
    prof.equippedBubbleId = inv.equipped.BUBBLE || '';
    prof.equippedMicWaveId = inv.equipped.MIC_WAVE || '';
    prof.equippedSpecialId = eqSidSpec ? eqSidSpec.specialIdNumber : '';
    users.set(uid, prof);
  }
  return inv;
}

function buildStoreStatePayload(uid, integrity = null) {
  const nowMs = Date.now();
  const inv = pruneAndSyncUserStore(uid, nowMs);
  const ownedItems = Array.from(inv.items.values())
    .sort((a, b) => Number(b.purchasedAtMs || 0) - Number(a.purchasedAtMs || 0))
    .map((entry) => ({
      itemId: entry.itemId,
      category: entry.category,
      expiresAtMs: Number(entry.expiresAtMs || 0),
      totalDaysPurchased: Number(entry.totalDaysPurchased || 7),
      purchasedAtMs: Number(entry.purchasedAtMs || nowMs),
      isEquipped: inv.equipped[entry.category] === entry.itemId,
      giftedById: entry.giftedById || '',
      giftedByName: entry.giftedByName || '',
    }));
  const effIntegrity = integrity || users.get(uid)?.lastIntegrity || null;
  const isCloned = Boolean(effIntegrity && effIntegrity.isClonedEnvironment);
  return {
    type: 'STORE_STATE_SYNC',
    userId: uid,
    serverNowMs: nowMs,
    ownedItems,
    equipped: { ...inv.equipped },
    securityVerified: !isCloned,
    securityReason: isCloned ? (effIntegrity?.reasons?.[0] || 'CLONE_DETECTED') : '',
  };
}

function pushStoreSyncToUser(uid) {
  if (!uid || !userSockets.has(uid)) return;
  sendToUser(uid, buildStoreStatePayload(uid));
}

function pruneStoreOrderCache(nowMs = Date.now()) {
  if (storeProcessedOrderIds.size < 4000) return;
  for (const [key, ts] of storeProcessedOrderIds.entries()) {
    if (nowMs - Number(ts || 0) > 3600000) {
      storeProcessedOrderIds.delete(key);
    }
  }
}
const GREEDY_JACKPOT_SHARE_PCTS = [50, 22, 10, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1];
const GREEDY_IST_OFFSET_MS = 19800000;

function getGreedyDayIndexIst(nowMs = Date.now()) {
  return Math.floor((nowMs + GREEDY_IST_OFFSET_MS) / 86400000);
}

function getGreedyNextResetAtMs(dayIdx = getGreedyDayIndexIst()) {
  return (dayIdx + 1) * 86400000 - GREEDY_IST_OFFSET_MS;
}

let greedyDayIndex = getGreedyDayIndexIst();
let greedyTodayJackpotPool = 0;
let greedyYesterdayJackpotPool = 0;
let greedyTodayLeaderboard = new Map(); // Map<userId, { userId, userName, avatarUri, coinsPlay, winCoins }>
let greedyYesterdayLeaderboard = new Map();

function getGreedyEntryCoinsPlay(e) {
  return Math.max(0, Number((e && (e.coinsPlay ?? e.winCoins)) || 0));
}

function distributeGreedyJackpotRewards(lbMap, poolCoins) {
  const pool = Math.floor(Number(poolCoins || 0));
  if (pool <= 0 || !lbMap || lbMap.size === 0) return;
  const sorted = Array.from(lbMap.values())
    .filter((e) => getGreedyEntryCoinsPlay(e) > 0)
    .sort((a, b) => getGreedyEntryCoinsPlay(b) - getGreedyEntryCoinsPlay(a))
    .slice(0, 15);
  sorted.forEach((entry, idx) => {
    const pct = GREEDY_JACKPOT_SHARE_PCTS[idx] || 0;
    const reward = Math.floor((pool * pct) / 100);
    if (reward > 0 && entry.userId) {
      const prevBal = getOrInitGreedyBalance(entry.userId);
      greedyUserBalances.set(entry.userId, prevBal + reward);
      recordWalletTx(
        entry.userId,
        'GREEDY_JACKPOT',
        `Ranking Jackpot #${idx + 1} (${pct}%)`,
        `+${reward} Gold Coins`,
        true,
        `greedy_jp_${greedyDayIndex}_${idx + 1}_${entry.userId}`
      );
      pushWalletSyncToUser(entry.userId);
      firestoreStore.markUserDirty(entry.userId);
    }
  });
}

function ensureGreedyDayFresh() {
  const curDay = getGreedyDayIndexIst();
  if (curDay !== greedyDayIndex) {
    distributeGreedyJackpotRewards(greedyTodayLeaderboard, greedyTodayJackpotPool);
    if (curDay === greedyDayIndex + 1) {
      greedyYesterdayLeaderboard = greedyTodayLeaderboard;
      greedyYesterdayJackpotPool = greedyTodayJackpotPool;
    } else {
      greedyYesterdayLeaderboard = new Map();
      greedyYesterdayJackpotPool = 0;
    }
    greedyTodayLeaderboard = new Map();
    greedyTodayJackpotPool = 0;
    greedyDayIndex = curDay;
    firestoreStore.markGlobalDirty();
  }
}

function getOrInitGreedyBalance(userId) {
  if (!userId) return GREEDY_INITIAL_COINS;
  if (!greedyUserBalances.has(userId)) {
    greedyUserBalances.set(userId, GREEDY_INITIAL_COINS);
    return GREEDY_INITIAL_COINS;
  }
  return greedyUserBalances.get(userId);
}

function getOrInitDiamondsBalance(userId) {
  if (!userId) return 0;
  if (!userDiamonds.has(userId)) {
    userDiamonds.set(userId, 0);
    return 0;
  }
  return userDiamonds.get(userId);
}

function recordWalletTx(uid, type, title, amountText, isAddition, customId = null) {
  if (!uid) return;
  const list = walletTransactions.get(uid) || [];
  const tx = {
    id: customId || `wtx_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
    type,
    title: String(title || 'Wallet Transaction').slice(0, 96),
    amountText: String(amountText || ''),
    date: 'Just now',
    createdAtMs: Date.now(),
    isAddition: Boolean(isAddition),
  };
  list.unshift(tx);
  if (list.length > 60) {
    list.length = 60;
  }
  walletTransactions.set(uid, list);
}

function getTodayDateKeyIst() {
  const istMs = Date.now() + 19800000;
  return new Date(istMs).toISOString().slice(0, 10);
}

function buildWalletStatePayload(uid, integrity = null) {
  const coinsBalance = getOrInitGreedyBalance(uid);
  const diamondsBalance = getOrInitDiamondsBalance(uid);
  const txs = walletTransactions.get(uid) || [];
  const todayKey = getTodayDateKeyIst();
  const dailyCheckInDone = walletDailyCheckIns.get(uid) === todayKey;
  const effIntegrity = integrity || users.get(uid)?.lastIntegrity || null;
  const isCloned = Boolean(effIntegrity && effIntegrity.isClonedEnvironment);
  return {
    type: 'WALLET_STATE_SYNC',
    userId: uid,
    coinsBalance,
    serverBalance: coinsBalance,
    diamondsBalance,
    dailyCheckInDone,
    securityVerified: !isCloned,
    securityReason: isCloned ? (effIntegrity?.reasons?.[0] || 'CLONE_DETECTED') : '',
    serverTimeMs: Date.now(),
    transactions: txs,
  };
}

function pushWalletSyncToUser(uid) {
  if (!uid || !userSockets.has(uid)) return;
  const payload = buildWalletStatePayload(uid);
  sendToUser(uid, payload);
  sendToUser(uid, { type: 'WALLET_BALANCE_SYNC', serverBalance: payload.coinsBalance });
}

const OFFICIAL_PACKAGE_NAME = 'com.aistudio.yovoiceparty.app';

function validateIntegrityForWallet(uid, integrity) {
  if (!integrity) {
    return { ok: true };
  }
  if (integrity.isClonedEnvironment || Number(integrity.userProfileSlot || 0) !== 0 || integrity.uidMatchesPackage === false || integrity.isKernelPathClean === false) {
    return {
      ok: false,
      error: `Clone / Dual-App detected (${(integrity.reasons || []).join(', ') || 'CLONE'}). Wallet actions are disabled.`,
    };
  }
  if (integrity.packageName && integrity.packageName !== OFFICIAL_PACKAGE_NAME) {
    return {
      ok: false,
      error: 'Tampered app package detected. Please install the official JustFun app.',
    };
  }
  return { ok: true };
}

function pruneWalletTxCache(now = Date.now()) {
  if (walletProcessedTxIds.size < 4000) return;
  for (const [key, entry] of walletProcessedTxIds.entries()) {
    if (now - Number(entry.createdAtMs || 0) > 3600000) {
      walletProcessedTxIds.delete(key);
    }
  }
}

function getGreedyCurrentRoundInfo(nowMs = Date.now()) {
  const safeDiff = Math.max(0, nowMs - GREEDY_EPOCH_MS);
  const roundNumber = Math.floor(safeDiff / GREEDY_ROUND_MS);
  const elapsedMs = safeDiff % GREEDY_ROUND_MS;
  return { nowMs, roundNumber, elapsedMs };
}

function getGreedyWinningOutcomeForRound(roundNumber) {
  const hmac = crypto
    .createHmac('sha256', GREEDY_HMAC_SECRET)
    .update(`greedy_v2:${Math.floor(roundNumber)}`)
    .digest();
  const r = hmac.readUInt32BE(0) % 100;
  if (r < 17) return 0;
  if (r < 34) return 1;
  if (r < 51) return 2;
  if (r < 68) return 3;
  if (r < 78) return 4;
  if (r < 85) return 5;
  if (r < 90) return 6;
  if (r < 93) return 7;
  if (r < 97) return 8; // Fruit Salad (0,1,2,3)
  return 9;             // Pizza Platter (4,5,6,7)
}

function getGreedyResultHistory(latestCompletedRound) {
  const arr = [];
  for (let i = 0; i < 8; i++) {
    const r = latestCompletedRound - i;
    if (r >= 0) {
      arr.push(getGreedyWinningOutcomeForRound(r));
    }
  }
  return arr;
}

function getGreedyWinningSlots(outcomeId) {
  if (outcomeId === 8) return [0, 1, 2, 3];
  if (outcomeId === 9) return [4, 5, 6, 7];
  if (outcomeId >= 0 && outcomeId <= 7) return [outcomeId];
  return [0];
}

function pruneOldGreedyRounds(currentRound) {
  for (const r of greedyRoundBets.keys()) {
    if (r < currentRound - 2) greedyRoundBets.delete(r);
  }
  for (const r of greedyRoundSlotTotals.keys()) {
    if (r < currentRound - 2) greedyRoundSlotTotals.delete(r);
  }
  for (const r of greedyRoundBetCounts.keys()) {
    if (r < currentRound - 2) greedyRoundBetCounts.delete(r);
  }
  for (const r of greedyRecentChips.keys()) {
    if (r < currentRound - 1) greedyRecentChips.delete(r);
  }
  for (const r of greedySettlements.keys()) {
    if (r < currentRound - 4) greedySettlements.delete(r);
  }
}

function computeGreedySlotTotals(roundNumber) {
  const cached = greedyRoundSlotTotals.get(roundNumber);
  if (cached) return [...cached];
  const totals = [0, 0, 0, 0, 0, 0, 0, 0];
  const bettors = greedyRoundBets.get(roundNumber);
  if (bettors) {
    for (const b of bettors.values()) {
      for (let s = 0; s < 8; s++) {
        totals[s] += Number(b.slotBets[s] || 0);
      }
    }
  }
  greedyRoundSlotTotals.set(roundNumber, totals);
  return [...totals];
}

function formatGreedyLeaderboard(map, limit = 15) {
  return Array.from(map.values())
    .filter((e) => getGreedyEntryCoinsPlay(e) > 0)
    .sort((a, b) => getGreedyEntryCoinsPlay(b) - getGreedyEntryCoinsPlay(a))
    .slice(0, limit)
    .map((e, idx) => {
      const userProf = users.get(e.userId) || {};
      const coinsPlay = getGreedyEntryCoinsPlay(e);
      return {
        rank: idx + 1,
        userId: e.userId,
        userName: userProf.nickname || e.userName || `User ${e.userId}`,
        avatarUri: resolveLiveUserAvatar(e.userId, e.avatarUri || ''),
        coinsPlay,
        winCoins: coinsPlay,
      };
    });
}

function computeMyGreedyRank(map, uid) {
  if (!uid) return { rank: 0, coinsPlay: 0 };
  const sorted = Array.from(map.values())
    .filter((e) => getGreedyEntryCoinsPlay(e) > 0)
    .sort((a, b) => getGreedyEntryCoinsPlay(b) - getGreedyEntryCoinsPlay(a));
  const idx = sorted.findIndex((e) => e.userId === uid);
  if (idx < 0) {
    return { rank: 0, coinsPlay: 0 };
  }
  return {
    rank: idx + 1,
    coinsPlay: getGreedyEntryCoinsPlay(sorted[idx]),
  };
}

function buildGreedyRankState(uid) {
  ensureGreedyDayFresh();
  return {
    todayJackpotPool: Math.floor(Number(greedyTodayJackpotPool || 0)),
    yesterdayJackpotPool: Math.floor(Number(greedyYesterdayJackpotPool || 0)),
    nextResetAtMs: getGreedyNextResetAtMs(greedyDayIndex),
    serverNowMs: Date.now(),
    todayRankList: formatGreedyLeaderboard(greedyTodayLeaderboard, 15),
    yesterdayRankList: formatGreedyLeaderboard(greedyYesterdayLeaderboard, 15),
    myTodayRank: computeMyGreedyRank(greedyTodayLeaderboard, uid),
    myYesterdayRank: computeMyGreedyRank(greedyYesterdayLeaderboard, uid),
  };
}

function computeAndCacheGreedySettlement(roundNumber) {
  if (greedySettlements.has(roundNumber)) {
    return greedySettlements.get(roundNumber);
  }
  ensureGreedyDayFresh();
  const winningFoodId = getGreedyWinningOutcomeForRound(roundNumber);
  const winningSlots = getGreedyWinningSlots(winningFoodId);
  const bettors = greedyRoundBets.get(roundNumber);
  const allWinners = [];
  const userWinAmountsById = {};
  const userBetTotalsById = {};
  let totalRoundWager = 0;
  let anyPersistentChange = false;

  if (bettors) {
    for (const b of bettors.values()) {
      let betTotal = 0;
      for (let s = 0; s < 8; s++) {
        betTotal += Number(b.slotBets[s] || 0);
      }
      userBetTotalsById[b.userId] = betTotal;
      totalRoundWager += betTotal;
      if (betTotal > 0) {
        firestoreStore.markUserDirty(b.userId);
        anyPersistentChange = true;
      }

      let winAmount = 0;
      for (const sId of winningSlots) {
        const betAmt = Number(b.slotBets[sId] || 0);
        if (betAmt > 0) {
          winAmount += betAmt * (GREEDY_SLOT_MULTIPLIERS[sId] || 5);
        }
      }
      userWinAmountsById[b.userId] = winAmount;

      if (winAmount > 0) {
        const prevBal = getOrInitGreedyBalance(b.userId);
        greedyUserBalances.set(b.userId, prevBal + winAmount);

        const userProf = users.get(b.userId) || {};
        const name = b.nickname || userProf.nickname || `User ${b.userId}`;
        const avatar = resolveLiveUserAvatar(b.userId, b.avatarUri || '');
        allWinners.push({
          userId: b.userId,
          userName: name,
          avatarUri: avatar,
          winAmount,
        });
      }

      if (betTotal > 0) {
        const cleanBetsBySlot = {};
        for (let s = 0; s < 8; s++) {
          const amt = Number(b.slotBets[s] || 0);
          if (amt > 0) cleanBetsBySlot[s] = amt;
        }
        const settleTimeMs = GREEDY_EPOCH_MS + (Math.floor(roundNumber) * GREEDY_ROUND_MS) + 35000;
        appendGreedyBetRecord(b.userId, {
          roundNumber: Math.floor(roundNumber),
          settleTimeMs,
          betsBySlot: cleanBetsBySlot,
          winningFoodId,
          totalCost: betTotal,
          totalWin: winAmount,
          afterBalance: getOrInitGreedyBalance(b.userId),
        });
      }
    }
  }

  if (totalRoundWager > 0) {
    // 5% of total coins wagered in each round grows the live Global Ranking Jackpot Pool
    const poolIncrement = Math.max(10, Math.floor(totalRoundWager * 0.05));
    greedyTodayJackpotPool = Math.floor(Number(greedyTodayJackpotPool || 0)) + poolIncrement;
    anyPersistentChange = true;
  }

  if (anyPersistentChange) {
    firestoreStore.markGlobalDirty();
  }

  allWinners.sort((a, b) => b.winAmount - a.winAmount);
  const topWinners = allWinners.slice(0, 3).map((w, idx) => ({
    rank: idx + 1,
    userId: w.userId,
    userName: w.userName,
    avatarUri: w.avatarUri || '',
    winAmount: w.winAmount,
  }));

  const cached = {
    roundNumber,
    winningFoodId,
    topWinners,
    userWinAmountsById,
    userBetTotalsById,
    todayJackpotPool: Math.floor(Number(greedyTodayJackpotPool || 0)),
    yesterdayJackpotPool: Math.floor(Number(greedyYesterdayJackpotPool || 0)),
    nextResetAtMs: getGreedyNextResetAtMs(greedyDayIndex),
    todayRankList: formatGreedyLeaderboard(greedyTodayLeaderboard, 15),
    yesterdayRankList: formatGreedyLeaderboard(greedyYesterdayLeaderboard, 15),
    completedHistory: getGreedyResultHistory(roundNumber),
  };
  greedySettlements.set(roundNumber, cached);
  return cached;
}

function buildGreedyPerUserOutcomePayload(type, roundNumber, userId, clientSentElapsedMs = 0) {
  const cached = computeAndCacheGreedySettlement(roundNumber);
  const payload = {
    type,
    roundNumber: cached.roundNumber,
    winningFoodId: cached.winningFoodId,
    topWinners: cached.topWinners,
    userBetTotal: Number(cached.userBetTotalsById[userId] || 0),
    userWinAmount: Number(cached.userWinAmountsById[userId] || 0),
    serverBalance: userId ? getOrInitGreedyBalance(userId) : GREEDY_INITIAL_COINS,
    completedHistory: cached.completedHistory,
    serverNowMs: Date.now(),
    clientSentElapsedMs: Number(clientSentElapsedMs || 0),
  };
  // Omit bulky leaderboards during 30s DRAW_REVEAL; send capped Today leaderboard & Jackpot Pool on SETTLEMENT
  if (type === 'GREEDY_SETTLEMENT') {
    payload.todayRankList = cached.todayRankList;
    payload.todayJackpotPool = cached.todayJackpotPool;
    payload.yesterdayJackpotPool = cached.yesterdayJackpotPool;
    payload.nextResetAtMs = cached.nextResetAtMs;
    payload.myTodayRank = computeMyGreedyRank(greedyTodayLeaderboard, userId);
    payload.myYesterdayRank = computeMyGreedyRank(greedyYesterdayLeaderboard, userId);
  }
  return payload;
}

function broadcastToGreedySubscribers(payloadObj, excludeWs = null) {
  const raw = JSON.stringify(payloadObj);
  for (const clientWs of greedySubscribers) {
    if (excludeWs && clientWs === excludeWs) continue;
    if (clientWs && clientWs.readyState === WebSocket.OPEN) {
      try {
        clientWs.send(raw);
      } catch (_) {}
    } else {
      greedySubscribers.delete(clientWs);
    }
  }
}

function flushGreedyCrowdBetBatch() {
  if (greedyCrowdFlushTimer) {
    clearTimeout(greedyCrowdFlushTimer);
    greedyCrowdFlushTimer = null;
  }
  const batch = greedyCrowdBatch;
  greedyCrowdBatch = null;
  if (!batch || greedySubscribers.size === 0 || batch.events.length === 0) return;
  broadcastToGreedySubscribers(
    {
      type: 'GREEDY_BET_BATCH',
      serverNowMs: Date.now(),
      roundNumber: batch.roundNumber,
      events: batch.events,
      slotTotals: batch.slotTotals,
    },
    batch.singleSourceWs || null
  );
}

function queueGreedyCrowdBetEvent(roundNumber, userId, slotIds, chipIndex, slotTotals, sourceWs) {
  if (greedySubscribers.size === 0) return;
  if (greedyCrowdBatch && greedyCrowdBatch.roundNumber !== roundNumber) {
    flushGreedyCrowdBetBatch();
  }
  if (!greedyCrowdBatch) {
    greedyCrowdBatch = {
      roundNumber,
      events: [],
      slotTotals,
      singleSourceWs: sourceWs || null,
    };
  } else {
    if (greedyCrowdBatch.singleSourceWs !== sourceWs) {
      greedyCrowdBatch.singleSourceWs = null;
    }
    greedyCrowdBatch.slotTotals = slotTotals;
  }
  if (greedyCrowdBatch.events.length < 14) {
    greedyCrowdBatch.events.push({
      userId,
      slotIds,
      chipIndex,
    });
  }
  if (!greedyCrowdFlushTimer) {
    greedyCrowdFlushTimer = setTimeout(flushGreedyCrowdBetBatch, 120);
  }
}

function addUserSocket(userId, ws) {
  if (!userId || !ws) return;
  let set = userSockets.get(userId);
  if (!set) {
    set = new Set();
    userSockets.set(userId, set);
  }
  set.add(ws);
}

function removeUserSocket(userId, ws) {
  if (!userId || !ws) return;
  const set = userSockets.get(userId);
  if (!set) return;
  set.delete(ws);
  if (set.size === 0) {
    userSockets.delete(userId);
  }
}

function sendToUser(userId, payloadObj) {
  if (!userId) return;
  const set = userSockets.get(userId);
  if (!set) return;
  const raw = JSON.stringify(payloadObj);
  for (const clientWs of set) {
    if (clientWs && clientWs.readyState === WebSocket.OPEN) {
      try {
        clientWs.send(raw);
      } catch (_) {}
    } else {
      set.delete(clientWs);
    }
  }
}

function getOrCreateSet(map, key) {
  let s = map.get(key);
  if (!s) {
    s = new Set();
    map.set(key, s);
  }
  return s;
}

function getThreadKey(a, b) {
  return [String(a), String(b)].sort().join('_');
}

function findUserActiveRoomId(userId) {
  if (!userId) return '';
  const uid = String(userId);
  for (const room of rooms.values()) {
    if (room.activeClients.has(uid)) return room.id;
  }
  return '';
}

function buildUserSummary(u, viewerId = '') {
  if (!u || !u.userId) return null;
  const uid = String(u.userId);
  const followersSet = userFollowers.get(uid);
  const followingSet = userFollowing.get(uid);
  const visitorsArr = userVisitors.get(uid);
  const viewerFollowingSet = viewerId ? userFollowing.get(String(viewerId)) : null;
  const viewerBlockedMap = viewerId ? blockedUsers.get(String(viewerId)) : null;

  return {
    userId: uid,
    user8DigitId: u.user8DigitId || uid,
    nickname: u.nickname || 'Just Fun User',
    avatarUri: u.avatarUri || '',
    gender: u.gender || 'Boy',
    bio: u.bio || "He/she hasn't set a bio yet.",
    birthday: u.birthday || '2004-01-01',
    country: u.country || 'India',
    level: Number(u.level || 0),
    totalCharmPoints: Number(u.totalCharmPoints || 0),
    followersCount: followersSet ? followersSet.size : 0,
    followingCount: followingSet ? followingSet.size : 0,
    visitorsCount: visitorsArr ? visitorsArr.length : 0,
    isFollowedByMe: viewerFollowingSet ? viewerFollowingSet.has(uid) : false,
    isBlockedByMe: viewerBlockedMap ? viewerBlockedMap.has(uid) : false,
    inRoomId: findUserActiveRoomId(uid),
    equippedFrameId: u.equippedFrameId || '',
    equippedVehicleId: u.equippedVehicleId || '',
    equippedVehicleName: u.equippedVehicleName || '',
    equippedBubbleId: u.equippedBubbleId || '',
    equippedMicWaveId: u.equippedMicWaveId || '',
    equippedSpecialId: u.equippedSpecialId || '',
    bgUrl: u.bgUrl || '',
    unlockedMedalIds: Array.isArray(u.unlockedMedalIds) ? u.unlockedMedalIds : [],
    equippedMedalIds: Array.isArray(u.equippedMedalIds) ? u.equippedMedalIds.slice(0, 10) : [],
    medalProgress: u.medalProgress && typeof u.medalProgress === 'object' ? u.medalProgress : {},
    receivedGifts: Array.isArray(u.receivedGifts) ? u.receivedGifts : [],
    photos: Array.isArray(u.photos) ? u.photos.slice(0, 4) : [],
  };
}

function buildSocialSnapshotForUser(userId) {
  const uid = String(userId || '');
  const u = users.get(uid) || { userId: uid, user8DigitId: uid };
  const followerIds = Array.from(userFollowers.get(uid) || []);
  const followingIds = Array.from(userFollowing.get(uid) || []);

  const followers = followerIds
    .map((fid) => buildUserSummary(users.get(fid) || { userId: fid, user8DigitId: fid }, uid))
    .filter(Boolean);

  const following = followingIds
    .map((tid) => buildUserSummary(users.get(tid) || { userId: tid, user8DigitId: tid }, uid))
    .filter(Boolean);

  const visitors = (userVisitors.get(uid) || []).slice(0, 50).map((v) => {
    const profile = users.get(v.userId) || {};
    const myFollowSet = userFollowing.get(uid);
    return {
      userId: v.userId,
      user8DigitId: v.userId,
      nickname: profile.nickname || v.nickname || 'User',
      avatarUri: profile.avatarUri || v.avatarUri || '',
      gender: profile.gender || v.gender || 'Boy',
      bio: profile.bio || '',
      level: Number(profile.level || v.level || 0),
      totalCharmPoints: Number(profile.totalCharmPoints || 0),
      followersCount: (userFollowers.get(v.userId) || new Set()).size,
      followingCount: (userFollowing.get(v.userId) || new Set()).size,
      visitorsCount: (userVisitors.get(v.userId) || []).length,
      isFollowedByMe: myFollowSet ? myFollowSet.has(v.userId) : false,
      visitedAt: v.visitedAt,
    };
  });

  // Collect DM threads involving this user
  const myThreads = [];
  let totalUnreadDm = 0;
  for (const [threadKey, messages] of dmThreads.entries()) {
    const parts = threadKey.split('_');
    if (parts.length !== 2) continue;
    if (parts[0] !== uid && parts[1] !== uid) continue;
    const peerId = parts[0] === uid ? parts[1] : parts[0];
    const visibleMessages = messages.filter(
      (m) => !(Array.isArray(m.deletedBy) && m.deletedBy.includes(uid))
    );
    if (visibleMessages.length === 0 && messages.length > 0) continue;
    const peerProfile =
      peerId === '10000'
        ? { userId: '10000', nickname: 'Customer service', avatarUri: '' }
        : users.get(peerId) || {};
    const unreadKey = `${uid}:${peerId}`;
    const unreadCount = dmUnreadCounts.get(unreadKey) || 0;
    totalUnreadDm += unreadCount;
    const lastMsg = visibleMessages[visibleMessages.length - 1];
    let previewText = '';
    if (lastMsg) {
      if (lastMsg.isRecalled) {
        previewText = 'Message recalled';
      } else if (lastMsg.msgType === 'VOICE') {
        previewText = `[Voice ${lastMsg.voiceDurationSec || 1}s]`;
      } else if (lastMsg.msgType === 'IMAGE') {
        previewText = '[Photo]';
      } else if (lastMsg.msgType === 'GIFT') {
        previewText = `[Gift: ${lastMsg.giftName || 'Gift'}]`;
      } else {
        previewText = lastMsg.text || '';
      }
    }
    const iFollowPeer = peerId === '10000' ? true : Boolean(userFollowing.get(uid)?.has(peerId));
    const peerFollowsMe = peerId === '10000' ? true : Boolean(userFollowing.get(peerId)?.has(uid));
    const isMutualFollow = iFollowPeer && peerFollowsMe;
    const mySentCount = messages.filter((m) => m.fromUserId === uid && !m.isRecalled).length;
    const isPeerOnline =
      peerId === '10000' || Boolean(userSockets.has(peerId) && userSockets.get(peerId).size > 0);
    myThreads.push({
      peerUserId: peerId,
      peerName: peerProfile.nickname || lastMsg?.fromName || lastMsg?.toName || `User ${peerId}`,
      peerAvatarUri: peerProfile.avatarUri || '',
      unreadCount,
      lastMessageText: previewText,
      lastMessageAtMs: lastMsg ? lastMsg.createdAtMs : 0,
      isPeerOnline,
      isFollowedByMe: iFollowPeer,
      isFollowingMe: peerFollowsMe,
      isMutualFollow,
      mySentCount,
      messages: visibleMessages.slice(-80),
    });
  }
  myThreads.sort((a, b) => (b.lastMessageAtMs || 0) - (a.lastMessageAtMs || 0));

  const myBlockedMap = blockedUsers.get(uid);
  const blockedList = myBlockedMap ? Array.from(myBlockedMap.values()) : [];
  const alertsList = (userFollowAlerts.get(uid) || []).slice(0, 30);
  const sysNotices = getOrInitSystemNotices(uid).slice(0, 30);

  return {
    type: 'SOCIAL_STATE_SYNC',
    userId: uid,
    followersCount: followers.length,
    followingCount: following.length,
    visitorsCount: visitors.length,
    totalCharmPoints: Number(u.totalCharmPoints || 0),
    followerIds,
    followingIds,
    followers,
    following,
    visitors,
    dmThreads: myThreads,
    totalUnreadDm,
    blockedUsers: blockedList,
    dmPrivacy: userDmPrivacy.get(uid) || 'ALL',
    followAlerts: alertsList,
    systemNotifications: sysNotices,
  };
}

function pushSocialSyncToUser(userId) {
  if (!userId || !userSockets.has(userId)) return;
  sendToUser(userId, buildSocialSnapshotForUser(userId));
}

function recordProfileVisit(viewerId, targetId) {
  if (!viewerId || !targetId || String(viewerId) === String(targetId)) return;
  const tid = String(targetId);
  const vid = String(viewerId);
  const viewerProfile = users.get(vid) || {};
  let list = userVisitors.get(tid);
  if (!list) {
    list = [];
    userVisitors.set(tid, list);
  }
  const filtered = list.filter((v) => v.userId !== vid);
  filtered.unshift({
    userId: vid,
    nickname: viewerProfile.nickname || `User ${vid}`,
    avatarUri: viewerProfile.avatarUri || '',
    gender: viewerProfile.gender || 'Boy',
    level: Number(viewerProfile.level || 0),
    visitedAt: Date.now(),
  });
  userVisitors.set(tid, filtered.slice(0, 60));
}

function searchRoomsAndUsers(rawQuery, viewerId = '') {
  const q = String(rawQuery || '').trim().toLowerCase();
  const matchedRoomsMap = new Map();
  const matchedUsers = [];

  for (const room of rooms.values()) {
    const idMatch = room.id.toLowerCase().includes(q);
    const titleMatch = (room.title || '').toLowerCase().includes(q);
    const hostMatch = (room.hostName || '').toLowerCase().includes(q);
    if (!q || idMatch || titleMatch || hostMatch) {
      const hostUser = users.get(room.hostId || room.id) || users.get(room.id);
      const validRoomDp = room.hostAvatarUri && !String(room.hostAvatarUri).startsWith('res://')
        ? room.hostAvatarUri
        : '';
      const validHostDp = hostUser?.avatarUri && !String(hostUser.avatarUri).startsWith('res://')
        ? hostUser.avatarUri
        : '';
      matchedRoomsMap.set(room.id, {
        id: room.id,
        title: room.title || hostUser?.nickname || 'Voice Room',
        announcement: room.announcement || 'Welcome!',
        categoryTag: room.categoryTag || 'Popular',
        countryName: room.countryName || 'India',
        onlineCount: room.activeClients.size,
        hostId: room.hostId || room.id,
        hostName: room.hostName || hostUser?.nickname || 'Host',
        hostAvatarUri: validRoomDp || validHostDp || '',
        isLive: room.activeClients.size > 0,
        isLockedRoom: Boolean(room.roomPassword),
      });
    }
  }

  for (const u of users.values()) {
    const uid = String(u.userId || '');
    const id8 = String(u.user8DigitId || uid);
    const name = String(u.nickname || '');
    if (!q || uid.toLowerCase().includes(q) || id8.toLowerCase().includes(q) || name.toLowerCase().includes(q)) {
      const summary = buildUserSummary(u, viewerId);
      if (summary) matchedUsers.push(summary);

      // Ensure every matching user also has a searchable Room entry in the Room tab (like Masti Search!)
      if (!matchedRoomsMap.has(uid)) {
        const existingRoom = rooms.get(uid);
        const validRoomDp = existingRoom?.hostAvatarUri && !String(existingRoom.hostAvatarUri).startsWith('res://')
          ? existingRoom.hostAvatarUri
          : '';
        const validUserDp = u.avatarUri && !String(u.avatarUri).startsWith('res://')
          ? u.avatarUri
          : '';
        matchedRoomsMap.set(uid, {
          id: uid,
          title: existingRoom?.title || u.nickname || 'Voice Room',
          announcement: existingRoom?.announcement || u.bio || 'Welcome to my room!',
          categoryTag: existingRoom?.categoryTag || 'Popular',
          countryName: u.country || 'India',
          onlineCount: existingRoom ? existingRoom.activeClients.size : 0,
          hostId: uid,
          hostName: u.nickname || 'Host',
          hostAvatarUri: validRoomDp || validUserDp || '',
          isLive: existingRoom ? existingRoom.activeClients.size > 0 : false,
          isLockedRoom: Boolean(existingRoom?.roomPassword),
        });
      }
    }
  }

  const matchedRooms = Array.from(matchedRoomsMap.values())
    .sort((a, b) => (b.onlineCount - a.onlineCount))
    .slice(0, 40);

  return {
    rooms: matchedRooms,
    users: matchedUsers.slice(0, 40),
  };
}

function createDefaultSeats() {
  return Array.from({ length: 8 }, (_, index) => ({
    seatIndex: index,
    isLocked: index === 0 || index === 3 || index === 4 || index === 5 || index === 7,
    isMuted: false,
    isForceMuted: false,
    isSpeaking: false,
    occupiedUserId: null,
    occupiedNickname: null,
    occupiedAvatarUri: null,
    occupiedLevel: 23,
    occupiedGender: 'male',
    occupiedIsAdmin: false,
  }));
}

// ============================================================================
// AUTHORITATIVE REAL-TIME USER LEVEL & EXP ENGINE (`Lv.0`..`Lv.50`)
// EXP Sources (ONLY):
//   1. Gift Send (`SEND_GIFT_TX`): 5 Coins spent = 1 EXP (min 1)
//   2. Rocket Launch (`ROCKET_LAUNCHED`): Top 1-3 (min 50,000 coins sent) win 1x Coin Reward as EXP
//   3. Room Stay (`ROOM_STAY_CLAIM`): 10 mins = 1,000 EXP
//      - Own Room Max: 10,000 EXP/day
//      - Other Room Max: 20,000 EXP/day
// ============================================================================
const LEVEL_CUMULATIVE_EXP = {
  0: 0,
  1: 15000,
  2: 45000,
  3: 90000,
  4: 150000,
  5: 240000,
  6: 360000,
  7: 510000,
  8: 690000,
  9: 900000,
  10: 1200000,
  11: 1560000,
  12: 1980000,
  13: 2460000,
  14: 3000000,
  15: 3600000,
  16: 4260000,
  17: 4980000,
  18: 5760000,
  19: 6600000,
  20: 7500000,
  21: 8460000,
  22: 9480000,
  23: 10560000,
  24: 11700000,
  25: 12900000,
  26: 13800000,
  27: 14706846,
  28: 16091100,
  29: 19500000,
  30: 24000000,
  31: 36000000,
  32: 51000000,
  33: 72000000,
  34: 96000000,
  35: 126000000,
  36: 165000000,
  37: 210000000,
  38: 264000000,
  39: 330000000,
  40: 405000000,
  41: 486000000,
  42: 576000000,
  43: 672000000,
  44: 774000000,
  45: 870000000,
  46: 951200712,
  47: 990770400,
  48: 1050000000,
  49: 1125000000,
  50: 1200000000,
};

const LEVEL_COIN_MILESTONES = [
  { level: 4, coins: 9000 },
  { level: 6, coins: 12000 },
  { level: 8, coins: 30000 },
  { level: 10, coins: 60000 },
  { level: 12, coins: 90000 },
  { level: 15, coins: 180000 },
  { level: 18, coins: 240000 },
  { level: 20, coins: 300000 },
  { level: 22, coins: 480000 },
  { level: 25, coins: 780000 },
  { level: 27, coins: 900000 },
  { level: 30, coins: 3000000 },
  { level: 33, coins: 6000000 },
  { level: 36, coins: 9000000 },
  { level: 40, coins: 10000000 },
  { level: 43, coins: 11000000 },
  { level: 46, coins: 12000000 },
  { level: 50, coins: 13000000 },
];

const ROOM_STAY_INTERVAL_MS = 10 * 60 * 1000;
const ROOM_STAY_MIN_ELAPSED_MS = 580 * 1000; // 20s clock tolerance on 10m
const ROOM_STAY_EXP_PER_STEP = 1000;
const ROOM_STAY_MAX_OWN_DAILY = 10000;
const ROOM_STAY_MAX_OTHER_DAILY = 20000;

// Map<userId, { exp, level, creditedCoinLevels: Set<number>, dayKey: number, dailyOwnRoomStayExp: number, dailyOtherRoomStayExp: number }>
const userLevels = new Map();
// Map<userId, { roomId, joinedAtMs, lastClaimAtMs }>
const userRoomStayTracking = new Map();

function getCurrentDayKey() {
  return Math.floor(Date.now() / 86400000);
}

function computeLevelFromExp(exp) {
  const safeExp = Math.max(0, Math.floor(Number(exp || 0)));
  let lv = 0;
  for (let i = 1; i <= 50; i++) {
    if (safeExp >= LEVEL_CUMULATIVE_EXP[i]) {
      lv = i;
    } else {
      break;
    }
  }
  const nextLv = Math.min(50, lv + 1);
  const nextExp = lv >= 50
    ? Math.max(LEVEL_CUMULATIVE_EXP[50], safeExp)
    : LEVEL_CUMULATIVE_EXP[nextLv];
  return { level: lv, exp: safeExp, nextLevelExp: nextExp };
}

function getOrCreateUserLevelState(uid) {
  const today = getCurrentDayKey();
  let st = userLevels.get(uid);
  if (!st) {
    st = {
      exp: 0,
      level: 0,
      creditedCoinLevels: new Set(),
      dayKey: today,
      dailyOwnRoomStayExp: 0,
      dailyOtherRoomStayExp: 0,
    };
    userLevels.set(uid, st);
  }
  if (st.dayKey !== today) {
    st.dayKey = today;
    st.dailyOwnRoomStayExp = 0;
    st.dailyOtherRoomStayExp = 0;
  }
  return st;
}

function buildLevelStatePayload(uid, extra = {}) {
  const st = getOrCreateUserLevelState(uid);
  const computed = computeLevelFromExp(st.exp);
  return {
    type: 'LEVEL_STATE_SYNC',
    userId: uid,
    currentLevel: computed.level,
    currentExp: computed.exp,
    nextLevelExp: computed.nextLevelExp,
    creditedCoinLevels: Array.from(st.creditedCoinLevels).sort((a, b) => a - b),
    dayKey: st.dayKey,
    dailyOwnRoomStayExpEarned: st.dailyOwnRoomStayExp,
    dailyOtherRoomStayExpEarned: st.dailyOtherRoomStayExp,
    ...extra,
  };
}

function broadcastUserLevelToRooms(uid, newLevel) {
  for (const room of rooms.values()) {
    let changed = false;
    if (room.activeClients.has(uid)) {
      changed = true;
    }
    if (Array.isArray(room.seats)) {
      room.seats = room.seats.map((seat) => {
        if (seat && seat.occupiedUserId === uid && Number(seat.occupiedLevel || 0) !== newLevel) {
          changed = true;
          return { ...seat, occupiedLevel: newLevel };
        }
        return seat;
      });
    }
    if (changed) {
      broadcastToRoom(room, buildRoomSnapshot(room));
    }
  }
}

function grantUserExp(uid, expGain, reason = 'EXP') {
  if (!uid || !Number.isFinite(expGain) || expGain <= 0) return null;
  const st = getOrCreateUserLevelState(uid);
  const prevLevel = st.level;
  st.exp = Math.max(0, Math.floor(st.exp + expGain));
  const computed = computeLevelFromExp(st.exp);
  st.level = computed.level;

  let milestoneCoinsAdded = 0;
  const unlockedMilestones = [];
  for (const m of LEVEL_COIN_MILESTONES) {
    if (st.level >= m.level && !st.creditedCoinLevels.has(m.level)) {
      st.creditedCoinLevels.add(m.level);
      milestoneCoinsAdded += m.coins;
      unlockedMilestones.push(m.level);
    }
  }

  if (milestoneCoinsAdded > 0) {
    const curBal = getOrInitGreedyBalance(uid);
    const newBal = Math.min(GREEDY_MAX_BALANCE, curBal + milestoneCoinsAdded);
    greedyUserBalances.set(uid, newBal);
    recordWalletTx(
      uid,
      'COIN_USAGE',
      `Level Up Reward (Lv.${unlockedMilestones[unlockedMilestones.length - 1] ?? st.level})`,
      `+${milestoneCoinsAdded.toLocaleString('en-US')} Coins`,
      true
    );
    pushWalletSyncToUser(uid);
  }

  const prof = users.get(uid);
  if (prof) {
    prof.level = st.level;
    users.set(uid, prof);
  }

  if (st.level !== prevLevel) {
    broadcastUserLevelToRooms(uid, st.level);
  }

  sendToUser(
    uid,
    buildLevelStatePayload(uid, {
      lastExpGain: Math.floor(expGain),
      lastExpReason: reason,
      leveledUpFrom: st.level > prevLevel ? prevLevel : -1,
      milestoneCoinsAdded,
      unlockedMilestones,
    })
  );
  firestoreStore.markUserDirty(uid);

  return {
    newLevel: st.level,
    newExp: st.exp,
    leveledUp: st.level > prevLevel,
    milestoneCoinsAdded,
  };
}

// ============================================================================
// REAL-TIME ANTI-CHEAT & NON-CLONE INVITE REWARDS ENGINE
// ============================================================================
let INVITE_FRIEND_WELCOME_COINS = 250000;
let INVITE_DAY1_COINS = 150000;
let INVITE_DAY3_COINS = 100000;
const INVITE_DAY3_DELAY_MS = 48 * 60 * 60 * 1000;
const INVITE_RECHARGE_THRESHOLD = 500000;
const INVITE_RECHARGE_BONUS_COINS = 250000;
const INVITE_GIFT_REBATE_RATE = 0.05;
const INVITE_COMMISSION_WINDOW_MS = 90 * 24 * 60 * 60 * 1000;

const userInviteLedgers = new Map();     // Map<userId, InviteLedger>
const deviceInviteClaims = new Map();    // Map<fingerprintKey, { firstUserId, boundInviterId, boundAtMs }>
const userKnownDevices = new Map();      // Map<userId, Set<fingerprintKey>>
const userHardwareSignatures = new Map();// Map<userId, Set<hardwareSignature>>
const userLastIps = new Map();           // Map<userId, Set<ip>>
const deviceUserAccounts = new Map();    // Map<fingerprintKey, Set<userId>>
const inviteBindRateLimits = new Map();  // Map<userId, Array<timestampMs>>
const inviteTickerHistory = [];          // Array<{ id, userName, coins, reason, createdAtMs }>
const INVITE_SECURITY_FILE = path.join(__dirname, '.invite_security_ledger.json');

function saveInviteSecurityLedgerToDisk(affectedUserIds = [], affectedDeviceKeys = []) {
  try {
    affectedUserIds.forEach((u) => firestoreStore.markUserDirty(u));
    affectedDeviceKeys.forEach((k) => firestoreStore.markDeviceDirty(k));
    if (affectedUserIds.length === 0 && affectedDeviceKeys.length === 0) {
      firestoreStore.markGlobalDirty();
    }
  } catch (err) {
    console.warn('Failed to mark invite security ledger dirty:', err.message);
  }
}

function loadInviteSecurityLedgerFromDisk() {
  try {
    if (!fs.existsSync(INVITE_SECURITY_FILE)) return;
    const raw = fs.readFileSync(INVITE_SECURITY_FILE, 'utf8');
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (parsed.claims && typeof parsed.claims === 'object') {
      Object.entries(parsed.claims).forEach(([k, v]) => deviceInviteClaims.set(k, v));
    }
    if (parsed.knownDevices && typeof parsed.knownDevices === 'object') {
      Object.entries(parsed.knownDevices).forEach(([uid, arr]) => {
        if (Array.isArray(arr)) userKnownDevices.set(uid, new Set(arr));
      });
    }
    if (parsed.hwSigs && typeof parsed.hwSigs === 'object') {
      Object.entries(parsed.hwSigs).forEach(([uid, arr]) => {
        if (Array.isArray(arr)) userHardwareSignatures.set(uid, new Set(arr));
      });
    }
    if (parsed.userIps && typeof parsed.userIps === 'object') {
      Object.entries(parsed.userIps).forEach(([uid, arr]) => {
        if (Array.isArray(arr)) userLastIps.set(uid, new Set(arr));
      });
    }
    if (parsed.ledgers && typeof parsed.ledgers === 'object') {
      Object.entries(parsed.ledgers).forEach(([uid, l]) => {
        if (!l || typeof l !== 'object') return;
        userInviteLedgers.set(uid, {
          ...l,
          invitedFriends: new Map(Array.isArray(l.invitedFriends) ? l.invitedFriends : []),
        });
      });
    }
  } catch (err) {
    console.warn('Failed to load invite security ledger:', err.message);
  }
}

loadInviteSecurityLedgerFromDisk();

// ============================================================================
// STRICT 3-ID PER PHONE HARDWARE LOCK (SERVER-AUTHORITATIVE + DISK PERSISTED)
// ============================================================================
let MAX_ACCOUNTS_PER_DEVICE = 3;
const DEVICE_LEDGER_FILE = path.join(__dirname, '.device_account_ledger.json');
// deviceAccountSlots: Map<deviceKey, Map<user8DigitId, AccountSlotRecord>>
const deviceAccountSlots = new Map();

function loadDeviceAccountLedgerFromDisk() {
  try {
    if (!fs.existsSync(DEVICE_LEDGER_FILE)) return;
    const raw = fs.readFileSync(DEVICE_LEDGER_FILE, 'utf8');
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return;
    Object.entries(parsed).forEach(([devKey, slotList]) => {
      if (!Array.isArray(slotList)) return;
      const slotMap = new Map();
      slotList.forEach((slot) => {
        const id8 = String(slot.user8DigitId || slot.userId || '').trim();
        if (id8.length === 8) {
          slotMap.set(id8, {
            user8DigitId: id8,
            accountUid: String(slot.accountUid || id8),
            nickname: String(slot.nickname || ''),
            avatarUri: String(slot.avatarUri || ''),
            gender: String(slot.gender || 'Boy'),
            bio: String(slot.bio || 'Welcome to JustFun'),
            country: String(slot.country || 'India'),
            level: Number(slot.level || 0),
            profileCompleted: Boolean(slot.profileCompleted ?? true),
            createdAtMs: Number(slot.createdAtMs || Date.now()),
          });
        }
      });
      if (slotMap.size > 0) {
        deviceAccountSlots.set(devKey, slotMap);
      }
    });
  } catch (err) {
    console.warn('Failed to load device account ledger:', err.message);
  }
}

function saveDeviceAccountLedgerToDisk(affectedDeviceKeys = []) {
  try {
    if (Array.isArray(affectedDeviceKeys) && affectedDeviceKeys.length > 0) {
      affectedDeviceKeys.forEach((k) => firestoreStore.markDeviceDirty(k));
    } else {
      for (const dk of deviceAccountSlots.keys()) {
        firestoreStore.markDeviceDirty(dk);
      }
    }
  } catch (err) {
    console.warn('Failed to mark device account ledger dirty:', err.message);
  }
}

loadDeviceAccountLedgerFromDisk();

function getRegisteredAccountsForDeviceKeys(keys) {
  if (!Array.isArray(keys) || keys.length === 0) return [];
  const mergedBy8DigitId = new Map();
  keys.forEach((k) => {
    const slotMap = deviceAccountSlots.get(k);
    if (!slotMap) return;
    slotMap.forEach((slot, id8) => {
      const prev = mergedBy8DigitId.get(id8);
      if (!prev || (slot.nickname && !prev.nickname)) {
        mergedBy8DigitId.set(id8, { ...slot });
      }
    });
  });
  return Array.from(mergedBy8DigitId.values())
    .sort((a, b) => Number(a.createdAtMs || 0) - Number(b.createdAtMs || 0))
    .slice(0, MAX_ACCOUNTS_PER_DEVICE);
}

function upsertAccountSlotAcrossDeviceKeys(keys, slotRecord) {
  if (!Array.isArray(keys) || keys.length === 0 || !slotRecord || !slotRecord.user8DigitId) return;
  const id8 = String(slotRecord.user8DigitId);
  keys.forEach((k) => {
    let slotMap = deviceAccountSlots.get(k);
    if (!slotMap) {
      slotMap = new Map();
      deviceAccountSlots.set(k, slotMap);
    }
    const existing = slotMap.get(id8);
    if (existing || slotMap.size < MAX_ACCOUNTS_PER_DEVICE) {
      slotMap.set(id8, {
        ...(existing || {}),
        ...slotRecord,
        user8DigitId: id8,
        createdAtMs: Number(existing?.createdAtMs || slotRecord.createdAtMs || Date.now()),
      });
    }
  });
}

function evaluateServerDeviceAccountSlot(uid, accountUid, profileObj, integrity) {
  const cleanId8 = String(uid || '').trim();
  const cleanAccUid = String(accountUid || cleanId8).trim();
  if (!integrity || typeof integrity !== 'object') {
    return { allowed: true, accounts: [] };
  }
  const keys = getDeviceKeysFromIntegrity(integrity);
  const existingAccounts = getRegisteredAccountsForDeviceKeys(keys);

  if (integrity.isClonedEnvironment || Number(integrity.userProfileSlot || 0) !== 0 || integrity.uidMatchesPackage === false || integrity.isKernelPathClean === false || (integrity.packageName && integrity.packageName !== OFFICIAL_PACKAGE_NAME)) {
    return {
      allowed: false,
      reason: 'CLONE_DETECTED',
      allowedAccounts: existingAccounts,
      error: 'Clone / Dual-App / Parallel Space me nayi ID banana allowed nahi hai. Kripya main phone app use karein.',
    };
  }

  if (keys.length === 0) {
    return { allowed: true, accounts: existingAccounts };
  }

  const matched = existingAccounts.find(
    (acc) => acc.user8DigitId === cleanId8 || (cleanAccUid && acc.accountUid === cleanAccUid)
  );

  if (matched) {
    const updatedSlot = {
      ...matched,
      user8DigitId: matched.user8DigitId,
      accountUid: cleanAccUid || matched.accountUid,
      nickname: String(profileObj?.nickname || matched.nickname || ''),
      avatarUri: String(profileObj?.avatarUri || matched.avatarUri || ''),
      gender: String(profileObj?.gender || matched.gender || 'Boy'),
      bio: String(profileObj?.bio || matched.bio || 'Welcome to JustFun'),
      country: String(profileObj?.country || matched.country || 'India'),
      level: Math.max(Number(matched.level || 0), Number(profileObj?.level || 0)),
      profileCompleted: Boolean(
        matched.profileCompleted ||
          (profileObj?.nickname && profileObj.nickname !== 'जितेश ❤️')
      ),
    };
    upsertAccountSlotAcrossDeviceKeys(keys, updatedSlot);
    saveDeviceAccountLedgerToDisk(keys);
    return {
      allowed: true,
      accounts: getRegisteredAccountsForDeviceKeys(keys),
    };
  }

  if (existingAccounts.length >= MAX_ACCOUNTS_PER_DEVICE) {
    return {
      allowed: false,
      reason: 'LIMIT_REACHED',
      allowedAccounts: existingAccounts,
      error: `Ek phone me maximum ${MAX_ACCOUNTS_PER_DEVICE} IDs hi ban sakti hain (${existingAccounts.length}/${MAX_ACCOUNTS_PER_DEVICE} IDs ban chuki hain). Nayi 4th ID is phone par nahi ban sakti. Kripya apni pehle se bani 3 IDs me se kisi ek se login karein.`,
    };
  }

  if (cleanId8.length === 8) {
    const newSlot = {
      user8DigitId: cleanId8,
      accountUid: cleanAccUid || cleanId8,
      nickname: String(profileObj?.nickname || ''),
      avatarUri: String(profileObj?.avatarUri || ''),
      gender: String(profileObj?.gender || 'Boy'),
      bio: String(profileObj?.bio || 'Welcome to JustFun'),
      country: String(profileObj?.country || 'India'),
      level: Number(profileObj?.level || 0),
      profileCompleted: Boolean(profileObj?.nickname && profileObj.nickname !== 'जितेश ❤️'),
      createdAtMs: Date.now(),
    };
    upsertAccountSlotAcrossDeviceKeys(keys, newSlot);
    saveDeviceAccountLedgerToDisk(keys);
  }

  return {
    allowed: true,
    accounts: getRegisteredAccountsForDeviceKeys(keys),
  };
}

function getOrCreateInviteLedger(uid) {
  if (!uid) return null;
  let ledger = userInviteLedgers.get(uid);
  if (!ledger) {
    ledger = {
      boundInviterId: null,
      boundAtMs: 0,
      day3Credited: false,
      friendRechargedTotal: 0,
      rechargeBonusCredited: false,
      giftRebateTotal: 0,
      withdrawableCoins: 0,
      totalCoinsRewarded: 0,
      invitedFriends: new Map(), // Map<friendId, Record>
    };
    userInviteLedgers.set(uid, ledger);
  }
  return ledger;
}

function getDeviceKeysFromIntegrity(integrity) {
  if (!integrity || typeof integrity !== 'object') return [];
  const keys = [];
  const fp = String(integrity.deviceFingerprint || '').trim();
  const drm = String(integrity.mediaDrmHash || '').trim();
  if (fp) keys.push(`fp:${fp}`);
  if (drm) keys.push(`drm:${drm}`);
  return keys;
}

function recordUserDeviceIntegrity(uid, integrity, clientIp = '') {
  if (!uid || !integrity || typeof integrity !== 'object') return;
  const keys = getDeviceKeysFromIntegrity(integrity);

  if (keys.length > 0) {
    let uDevs = userKnownDevices.get(uid);
    if (!uDevs) {
      uDevs = new Set();
      userKnownDevices.set(uid, uDevs);
    }
    keys.forEach((k) => {
      uDevs.add(k);
      let dUsers = deviceUserAccounts.get(k);
      if (!dUsers) {
        dUsers = new Set();
        deviceUserAccounts.set(k, dUsers);
      }
      dUsers.add(uid);
    });
  }

  const hwSig = String(integrity.hardwareSignature || '').trim();
  if (hwSig) {
    let hwSet = userHardwareSignatures.get(uid);
    if (!hwSet) {
      hwSet = new Set();
      userHardwareSignatures.set(uid, hwSet);
    }
    hwSet.add(hwSig);
  }

  const cleanIp = String(clientIp || '').trim();
  if (cleanIp) {
    let ipSet = userLastIps.get(uid);
    if (!ipSet) {
      ipSet = new Set();
      userLastIps.set(uid, ipSet);
    }
    ipSet.add(cleanIp);
  }

  const existing = users.get(uid);
  if (existing) {
    existing.lastIntegrity = {
      isClonedEnvironment: Boolean(integrity.isClonedEnvironment),
      isEmulator: Boolean(integrity.isEmulator),
      reasons: Array.isArray(integrity.reasons) ? integrity.reasons.slice(0, 6) : [],
      updatedAtMs: Date.now(),
    };
    users.set(uid, existing);
  }
}

function isDeviceAlreadyClaimedForKeys(keys) {
  for (const k of keys) {
    if (deviceInviteClaims.has(k)) return true;
  }
  return false;
}

function shareSameDevice(uidA, uidB, currentKeys = [], integrity = null, clientIp = '') {
  const devsA = new Set([...(userKnownDevices.get(uidA) || []), ...currentKeys]);
  const devsB = userKnownDevices.get(uidB);
  if (devsB && devsA.size > 0) {
    for (const k of devsA) {
      if (devsB.has(k)) return true;
    }
  }
  // Catch Dual-App / Cloner where ANDROID_ID changed & MediaDrm failed, by matching same hardwareSignature + same clientIp
  const hwA = new Set(userHardwareSignatures.get(uidA) || []);
  const curHw = String(integrity?.hardwareSignature || '').trim();
  if (curHw) hwA.add(curHw);
  const hwB = userHardwareSignatures.get(uidB);

  const ipsA = new Set(userLastIps.get(uidA) || []);
  const cleanIp = String(clientIp || '').trim();
  if (cleanIp) ipsA.add(cleanIp);
  const ipsB = userLastIps.get(uidB);

  if (hwB && ipsB && hwA.size > 0 && ipsA.size > 0) {
    let hwOverlap = false;
    for (const h of hwA) {
      if (hwB.has(h)) { hwOverlap = true; break; }
    }
    if (hwOverlap) {
      for (const ip of ipsA) {
        if (ipsB.has(ip)) return true;
      }
    }
  }
  return false;
}

function validateIntegrityForInvite(integrity) {
  if (!integrity || typeof integrity !== 'object') {
    return { ok: false, error: 'Device integrity check missing. Please restart the app.' };
  }
  if (integrity.packageName && integrity.packageName !== OFFICIAL_PACKAGE_NAME) {
    return {
      ok: false,
      error: 'Tampered/Cloned APK package detected. Please install the official JustFun app.',
    };
  }
  if (integrity.isClonedEnvironment || Number(integrity.userProfileSlot || 0) !== 0 || integrity.uidMatchesPackage === false || integrity.isKernelPathClean === false) {
    const reasonCode = Array.isArray(integrity.reasons) && integrity.reasons.length > 0
      ? integrity.reasons[0]
      : 'CLONE_DETECTED';
    return {
      ok: false,
      error: `Clone / Dual-App detected (${reasonCode}). Invite rewards are only allowed on the official non-cloned app.`,
    };
  }
  if (integrity.isEmulator) {
    return {
      ok: false,
      error: 'Emulators are blocked from invite rewards by anti-cheat policy.',
    };
  }
  const keys = getDeviceKeysFromIntegrity(integrity);
  if (keys.length === 0) {
    return {
      ok: false,
      error: 'Hardware device verification failed. Please retry on a physical device.',
    };
  }
  return { ok: true, keys };
}

function pushInviteTicker(userName, coins, reason) {
  if (!userName || !Number.isFinite(coins) || coins <= 0) return;
  const entry = {
    id: `itk_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
    userName: String(userName).slice(0, 32),
    coins: Math.floor(coins),
    reason: String(reason || 'INVITE'),
    createdAtMs: Date.now(),
  };
  inviteTickerHistory.unshift(entry);
  if (inviteTickerHistory.length > 30) {
    inviteTickerHistory.splice(30);
  }
  const payload = JSON.stringify({
    type: 'INVITE_TICKER_UPDATE',
    entry,
    tickers: inviteTickerHistory.slice(0, 15),
  });
  for (const sockets of userSockets.values()) {
    for (const client of sockets) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(payload);
      }
    }
  }
}

function buildInviteStatePayload(uid, integrity = null) {
  const ledger = getOrCreateInviteLedger(uid);
  const keys = getDeviceKeysFromIntegrity(integrity);
  const deviceClaimed = keys.length > 0 ? isDeviceAlreadyClaimedForKeys(keys) : false;
  const invitedList = Array.from(ledger.invitedFriends.values())
    .sort((a, b) => Number(b.boundAtMs || 0) - Number(a.boundAtMs || 0));

  const boundInviterProfile = ledger.boundInviterId ? users.get(ledger.boundInviterId) : null;

  return {
    type: 'INVITE_STATE_SYNC',
    userId: uid,
    myInviteCode: uid,
    coinsToWithdraw: Math.max(0, Math.floor(ledger.withdrawableCoins)),
    numberOfInvitations: invitedList.length,
    totalCoinsRewarded: Math.max(0, Math.floor(ledger.totalCoinsRewarded)),
    isCodeBound: Boolean(ledger.boundInviterId),
    boundInviterId: ledger.boundInviterId || '',
    boundInviterName: boundInviterProfile?.nickname || '',
    isDeviceAlreadyClaimed: deviceClaimed || Boolean(ledger.boundInviterId),
    invitedFriends: invitedList,
    tickers: inviteTickerHistory.slice(0, 15),
  };
}

function pushInviteSyncToUser(uid, integrity = null) {
  if (!uid || !userSockets.has(uid)) return;
  sendToUser(uid, buildInviteStatePayload(uid, integrity));
}

function evaluateInviteDay3Retention(uid, integrity = null) {
  if (!uid) return;
  const ledger = userInviteLedgers.get(uid);
  if (!ledger || !ledger.boundInviterId || ledger.day3Credited) return;
  if (Date.now() - Number(ledger.boundAtMs || 0) < INVITE_DAY3_DELAY_MS) return;

  if (integrity && (integrity.isClonedEnvironment || integrity.isEmulator)) return;

  ledger.day3Credited = true;
  const inviterId = ledger.boundInviterId;
  const inviterLedger = getOrCreateInviteLedger(inviterId);
  if (!inviterLedger) return;

  const rec = inviterLedger.invitedFriends.get(uid);
  if (rec && !rec.day3Credited) {
    rec.day3Credited = true;
    rec.coinsRewarded = Number(rec.coinsRewarded || 0) + INVITE_DAY3_COINS;
    inviterLedger.invitedFriends.set(uid, rec);
  }
  inviterLedger.withdrawableCoins += INVITE_DAY3_COINS;
  inviterLedger.totalCoinsRewarded += INVITE_DAY3_COINS;

  const inviterName = users.get(inviterId)?.nickname || `User ${inviterId}`;
  pushInviteTicker(inviterName, INVITE_DAY3_COINS, 'DAY3_LOGIN');
  firestoreStore.markUserDirty(uid);
  firestoreStore.markUserDirty(inviterId);
  pushInviteSyncToUser(inviterId);
  pushInviteSyncToUser(uid, integrity);
}

function creditInviteRechargeProgress(uid, packCoins) {
  if (!uid || !Number.isFinite(packCoins) || packCoins <= 0) return;
  const ledger = userInviteLedgers.get(uid);
  if (!ledger || !ledger.boundInviterId) return;

  const u = users.get(uid);
  if (u?.lastIntegrity?.isClonedEnvironment || u?.lastIntegrity?.isEmulator) return;

  ledger.friendRechargedTotal = Number(ledger.friendRechargedTotal || 0) + Math.floor(packCoins);
  const inviterId = ledger.boundInviterId;
  const inviterLedger = getOrCreateInviteLedger(inviterId);
  const rec = inviterLedger.invitedFriends.get(uid);
  if (rec) {
    rec.rechargeCoins = ledger.friendRechargedTotal;
  }

  if (!ledger.rechargeBonusCredited && ledger.friendRechargedTotal >= INVITE_RECHARGE_THRESHOLD) {
    ledger.rechargeBonusCredited = true;
    if (rec) {
      rec.hasRecharged = true;
      rec.coinsRewarded = Number(rec.coinsRewarded || 0) + INVITE_RECHARGE_BONUS_COINS;
    }
    inviterLedger.withdrawableCoins += INVITE_RECHARGE_BONUS_COINS;
    inviterLedger.totalCoinsRewarded += INVITE_RECHARGE_BONUS_COINS;
    const inviterName = users.get(inviterId)?.nickname || `User ${inviterId}`;
    pushInviteTicker(inviterName, INVITE_RECHARGE_BONUS_COINS, 'FRIEND_RECHARGE');
  }

  if (rec) {
    inviterLedger.invitedFriends.set(uid, rec);
  }
  firestoreStore.markUserDirty(uid);
  firestoreStore.markUserDirty(inviterId);
  pushInviteSyncToUser(inviterId);
}

function creditInviteGiftCommission(fromUserId, coinsSpent) {
  if (!fromUserId || !Number.isFinite(coinsSpent) || coinsSpent <= 0) return;
  const ledger = userInviteLedgers.get(fromUserId);
  if (!ledger || !ledger.boundInviterId) return;
  if (Date.now() - Number(ledger.boundAtMs || 0) > INVITE_COMMISSION_WINDOW_MS) return;

  const rebate = Math.floor(coinsSpent * INVITE_GIFT_REBATE_RATE);
  if (rebate <= 0) return;

  const inviterId = ledger.boundInviterId;
  const inviterLedger = getOrCreateInviteLedger(inviterId);
  ledger.giftRebateTotal = Number(ledger.giftRebateTotal || 0) + rebate;

  const rec = inviterLedger.invitedFriends.get(fromUserId);
  if (rec) {
    rec.hasSentGifts = true;
    rec.giftCoinsReward = Number(rec.giftCoinsReward || 0) + rebate;
    rec.coinsRewarded = Number(rec.coinsRewarded || 0) + rebate;
    inviterLedger.invitedFriends.set(fromUserId, rec);
  }
  inviterLedger.withdrawableCoins += rebate;
  inviterLedger.totalCoinsRewarded += rebate;
  firestoreStore.markUserDirty(fromUserId);
  firestoreStore.markUserDirty(inviterId);
  pushInviteSyncToUser(inviterId);
}

const ROCKET_TARGETS = {
  1: 2500000,
  2: 5000000,
  3: 10000000,
};

const ROCKET_MIN_TOP_COINS = 50000;
const IST_OFFSET_MS = (5 * 60 + 30) * 60 * 1000;

function getNextDailyResetMs() {
  const dayMs = 24 * 60 * 60 * 1000;
  return (Math.floor((Date.now() + IST_OFFSET_MS) / dayMs) + 1) * dayMs - IST_OFFSET_MS;
}

function createInitialRocketState() {
  return {
    level: 1,
    currentExp: 0,
    allLaunched: false,
    resetAtMs: getNextDailyResetMs(),
    contributors: new Map(),
    history: [],
  };
}

function ensureRocketDailyFresh(room) {
  if (!room) return;
  if (!room.rocketState) {
    room.rocketState = createInitialRocketState();
    return;
  }
  if (Date.now() >= Number(room.rocketState.resetAtMs || 0)) {
    room.rocketState = createInitialRocketState();
    firestoreStore.markRoomDirty(room.id);
  }
}

function resolveLiveUserAvatar(uid, fallbackAvatar = '') {
  const uProf = users.get(uid);
  const liveAv = uProf?.avatarUri && !String(uProf.avatarUri).startsWith('res://') ? String(uProf.avatarUri) : '';
  const fb = fallbackAvatar && !String(fallbackAvatar).startsWith('res://') ? String(fallbackAvatar) : '';
  return liveAv || fb || uProf?.avatarUri || fallbackAvatar || '';
}

function buildRocketStatePayload(room) {
  ensureRocketDailyFresh(room);
  const rs = room.rocketState;
  const lvl = Math.min(3, Math.max(1, Number(rs.level || 1)));
  const contribs = Array.from(rs.contributors.values())
    .filter((c) => Number(c.coins || 0) >= ROCKET_MIN_TOP_COINS)
    .sort((a, b) => Number(b.coins || 0) - Number(a.coins || 0))
    .slice(0, 20)
    .map((c) => {
      const uProf = users.get(c.userId);
      return {
        ...c,
        nickname: uProf?.nickname || c.nickname || `User ${c.userId}`,
        avatarUri: resolveLiveUserAvatar(c.userId, c.avatarUri),
      };
    });
  const history = (Array.isArray(rs.history) ? rs.history.slice(0, 20) : []).map((h) => {
    const tProf = h.topGifterId ? users.get(h.topGifterId) : null;
    return {
      ...h,
      topGifterName: tProf?.nickname || h.topGifterName || 'Top Gifter',
      topGifterAvatarUri: h.topGifterId ? resolveLiveUserAvatar(h.topGifterId, h.topGifterAvatarUri) : (h.topGifterAvatarUri || ''),
    };
  });
  return {
    roomId: room.id,
    level: lvl,
    currentExp: Number(rs.currentExp || 0),
    targetExp: ROCKET_TARGETS[lvl] || ROCKET_TARGETS[1],
    allLaunched: Boolean(rs.allLaunched),
    resetAtMs: Number(rs.resetAtMs || getNextDailyResetMs()),
    contributors: contribs,
    history,
  };
}

function computeRocketLaunchRewards(level, sortedContribs, room) {
  const topRewards = level === 1
    ? [50000, 25000, 15000]
    : level === 2
      ? [120000, 60000, 30000]
      : [300000, 150000, 80000];
  const baseLucky = level === 1 ? 8000 : level === 2 ? 18000 : 35000;

  const rewardsByUser = {};
  const expByUser = {};
  const winners = [];

  const qualifiedTop = Array.isArray(sortedContribs)
    ? sortedContribs.filter((c) => Number(c?.coins || 0) >= ROCKET_MIN_TOP_COINS)
    : [];

  for (let i = 0; i < Math.min(3, qualifiedTop.length); i++) {
    const c = qualifiedTop[i];
    if (!c || !c.userId) continue;
    const amt = topRewards[i];
    const expWon = amt; // 1x Coin Reward as EXP (no 15x extra)
    const uProf = users.get(c.userId);
    rewardsByUser[c.userId] = amt;
    expByUser[c.userId] = expWon;
    winners.push({
      userId: c.userId,
      nickname: uProf?.nickname || c.nickname || `User ${c.userId}`,
      avatarUri: resolveLiveUserAvatar(c.userId, c.avatarUri),
      coinsWon: amt,
      expWon,
      isTopContributor: true,
      rank: i + 1,
    });
  }

  for (const uid of room.activeClients.keys()) {
    if (rewardsByUser[uid] !== undefined) continue;
    const uProf = users.get(uid) || {};
    const stepBonus = (Math.floor(Math.random() * 4) + 1) * Math.floor(baseLucky / 4);
    const amt = baseLucky + stepBonus;
    rewardsByUser[uid] = amt;
    winners.push({
      userId: uid,
      nickname: uProf.nickname || (uid === room.hostId ? room.hostName : `User ${uid}`),
      avatarUri: uProf.avatarUri || (uid === room.hostId ? room.hostAvatarUri : ''),
      coinsWon: amt,
      expWon: 0,
      isTopContributor: false,
      rank: winners.length + 1,
    });
  }

  const totalPoolCoins = Object.values(rewardsByUser).reduce((acc, v) => acc + Number(v || 0), 0);
  return { rewardsByUser, expByUser, winners, totalPoolCoins };
}

function getOrCreateRoom(roomId, init = {}) {
  let room = rooms.get(roomId);
  if (!room) {
    room = {
      id: roomId,
      title: init.title || 'Voice Room',
      announcement: init.announcement || 'Welcome to my room, let\'s chat together !',
      hostId: init.hostId || roomId,
      hostName: init.hostName || 'Host',
      hostAvatarUri: init.hostAvatarUri || '',
      isHostOnOtherSeat: false,
      isHostMuted: false,
      themeDrawableResId: init.themeDrawableResId || 0,
      isAdminChatOnly: false,
      chatClearedAt: 0,
      roomPassword: init.roomPassword || '',
      roomTotalCharm: 0,
      roomContributions: {
        daily: new Map(),
        weekly: new Map(),
        monthly: new Map(),
      },
      rocketState: createInitialRocketState(),
      categoryTag: init.categoryTag || 'Hot Party',
      countryName: init.countryName || 'India',
      seats: createDefaultSeats(),
      adminUsers: [],
      bannedChatUserIds: [],
      bannedUsers: [],
      messages: [],
      activeClients: new Map(), // Map<userId, WebSocket>
      updatedAt: Date.now(),
    };
    rooms.set(roomId, room);
  }
  return room;
}

function getLiveRoomsSummary() {
  const list = [];
  for (const room of rooms.values()) {
    const activeCount = room.activeClients.size;
    if (activeCount > 0) {
      const hostUser = users.get(room.hostId || room.id) || users.get(room.id);
      const validRoomDp = room.hostAvatarUri && !String(room.hostAvatarUri).startsWith('res://')
        ? room.hostAvatarUri
        : '';
      const validHostDp = hostUser?.avatarUri && !String(hostUser.avatarUri).startsWith('res://')
        ? hostUser.avatarUri
        : '';
      list.push({
        id: room.id,
        title: room.title,
        announcement: room.announcement,
        categoryTag: room.categoryTag,
        countryName: room.countryName,
        onlineCount: activeCount,
        hostId: room.hostId,
        hostName: room.hostName || hostUser?.nickname || 'Host',
        hostAvatarUri: validRoomDp || validHostDp || '',
        isLive: true,
        isLockedRoom: Boolean(room.roomPassword),
        activeUserIds: Array.from(room.activeClients.keys()),
        activeUsers: buildRoomActiveUsers(room).slice(0, 4),
      });
    }
  }
  list.sort((a, b) => b.onlineCount - a.onlineCount);
  return list;
}

function formatContributionList(map) {
  if (!map) return [];
  return Array.from(map.values())
    .sort((a, b) => (b.coins || 0) - (a.coins || 0))
    .slice(0, 50);
}

function buildContributionsPayload(room) {
  return {
    type: 'ROOM_CONTRIBUTIONS_UPDATE',
    roomId: room.id,
    roomTotalCharm: Number(room.roomTotalCharm || 0),
    daily: formatContributionList(room.roomContributions?.daily),
    weekly: formatContributionList(room.roomContributions?.weekly),
    monthly: formatContributionList(room.roomContributions?.monthly),
  };
}

function buildRoomActiveUsers(room) {
  const list = [];
  for (const uid of room.activeClients.keys()) {
    const u = users.get(uid);
    list.push({
      userId: uid,
      nickname: u?.nickname || (uid === room.hostId ? room.hostName : `User ${uid}`),
      avatarUri: u?.avatarUri || (uid === room.hostId ? room.hostAvatarUri : ''),
      level: Number(userLevels.get(uid)?.level ?? u?.level ?? 0),
      gender: u?.gender || 'Boy',
      bio: u?.bio || "He/she hasn't set a bio yet.",
    });
  }
  return list;
}

function buildRoomSnapshot(room) {
  return {
    type: 'ROOM_STATE',
    room: {
      roomId: room.id,
      title: room.title,
      announcement: room.announcement,
      hostId: room.hostId,
      hostName: room.hostName,
      hostAvatarUri: room.hostAvatarUri,
      isHostOnOtherSeat: room.isHostOnOtherSeat,
      isHostMuted: room.isHostMuted,
      themeDrawableResId: room.themeDrawableResId,
      isAdminChatOnly: room.isAdminChatOnly,
      chatClearedAt: room.chatClearedAt,
      isLockedRoom: Boolean(room.roomPassword),
      roomTotalCharm: Number(room.roomTotalCharm || 0),
      onlineCount: room.activeClients.size,
      isLive: room.activeClients.size > 0,
      activeUserIds: Array.from(room.activeClients.keys()),
      activeUsers: buildRoomActiveUsers(room),
      adminUsers: room.adminUsers,
      bannedChatUserIds: room.bannedChatUserIds,
      bannedUsers: room.bannedUsers,
      seats: room.seats,
      messages: room.messages.slice(-60),
      rocketState: buildRocketStatePayload(room),
    },
  };
}

function safeSend(ws, payloadObj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try {
      ws.send(JSON.stringify(payloadObj));
    } catch (_) {}
  }
}

function broadcastToRoom(room, payloadObj, excludeUserId = null) {
  const raw = JSON.stringify(payloadObj);
  for (const [uid, clientWs] of room.activeClients.entries()) {
    if (excludeUserId && uid === excludeUserId) continue;
    if (clientWs && clientWs.readyState === WebSocket.OPEN) {
      try {
        clientWs.send(raw);
      } catch (_) {}
    }
  }
}

let homeLiveRoomsTimer = null;
function broadcastLiveRoomsToHome() {
  if (homeLiveRoomsTimer) return;
  homeLiveRoomsTimer = setTimeout(() => {
    homeLiveRoomsTimer = null;
    if (homeSubscribers.size === 0) return;
    const payload = JSON.stringify({
      type: 'LIVE_ROOMS_UPDATE',
      serverNowMs: Date.now(),
      rooms: getLiveRoomsSummary(),
    });
    for (const clientWs of homeSubscribers) {
      if (clientWs.readyState === WebSocket.OPEN) {
        try {
          clientWs.send(payload);
        } catch (_) {}
      } else {
        homeSubscribers.delete(clientWs);
      }
    }
  }, 180);
}

function removeUserFromRoom(ws, roomId, userId) {
  if (!roomId || !userId) return;
  const room = rooms.get(roomId);
  if (!room) return;

  const existingWs = room.activeClients.get(userId);
  if (existingWs && existingWs !== ws && existingWs.readyState === WebSocket.OPEN) {
    return;
  }

  room.activeClients.delete(userId);
  if (userId === room.hostId) {
    room.isHostOnOtherSeat = false;
    room.isHostMuted = false;
  }

  let seatChanged = false;
  room.seats = room.seats.map((seat) => {
    if (seat.occupiedUserId === userId) {
      seatChanged = true;
      return {
        ...seat,
        occupiedUserId: null,
        occupiedNickname: null,
        occupiedAvatarUri: null,
        isSpeaking: false,
        isMuted: Boolean(seat.isForceMuted),
      };
    }
    return seat;
  });

  if (room.activeClients.size > 0) {
    broadcastToRoom(room, buildRoomSnapshot(room));
    broadcastToRoom(room, {
      type: 'WEBRTC_SIGNAL',
      roomId: room.id,
      fromPeerId: userId,
      toPeerId: 'ALL',
      signalType: 'PEER_LEFT',
    });
  } else if (seatChanged) {
    room.updatedAt = Date.now();
  }

  broadcastLiveRoomsToHome();
}

// HTTP Health & Metrics Routes
app.get('/', (req, res) => {
  const liveRooms = getLiveRoomsSummary();
  res.json({
    service: 'JustFun Real-Time Voice & Social Backend',
    status: 'ONLINE',
    liveRoomsCount: liveRooms.length,
    totalSavedRooms: rooms.size,
    registeredUsers: users.size,
    uptimeSeconds: Math.floor(process.uptime()),
    firestore: firestoreStore.getStatus(),
  });
});

app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

app.get('/api/rooms/live', (req, res) => {
  res.json({ rooms: getLiveRoomsSummary() });
});

app.get('/api/users', (req, res) => {
  res.json({ users: Array.from(users.values()).slice(0, 50) });
});

function buildDynamicCatalogPayload() {
  return {
    type: 'DYNAMIC_CATALOG_SYNC',
    gifts: Array.from(DYNAMIC_GIFTS_CATALOG.values()),
    storeCatalog: Object.values(STORE_CATALOG),
    customMedals: Array.from(DYNAMIC_MEDALS_CATALOG.values()),
    events: Array.from(DYNAMIC_EVENTS_CATALOG.values()),
    rechargePacks: WALLET_RECHARGE_PACKS,
    rules: {
      maxUnfollowedDmMessages: MAX_UNFOLLOWED_DM_MESSAGES,
      maxAccountsPerDevice: MAX_ACCOUNTS_PER_DEVICE,
      inviteDay1Coins: INVITE_DAY1_COINS,
      inviteDay3Coins: INVITE_DAY3_COINS,
      inviteFriendWelcomeCoins: INVITE_FRIEND_WELCOME_COINS,
      dailyCheckInCoins: DAILY_CHECKIN_REWARD_COINS,
    },
    updatedAtMs: Date.now(),
  };
}

function broadcastDynamicCatalog() {
  const payload = JSON.stringify(buildDynamicCatalogPayload());
  for (const ws of wss.clients) {
    if (ws.readyState === WebSocket.OPEN) {
      try {
        ws.send(payload);
      } catch (_) {}
    }
  }
}

function isUserOrDeviceBanned(uid, integrity = null) {
  const cleanId = String(uid || '').trim();
  if (cleanId && bannedUserIds.has(cleanId)) {
    const info = bannedUserIds.get(cleanId);
    return { banned: true, reason: info.reason || 'Your account ID has been banned by Admin.' };
  }
  const keys = [
    ...(userKnownDevices.get(cleanId) || []),
    ...getDeviceKeysFromIntegrity(integrity),
  ];
  for (const k of keys) {
    if (bannedDeviceKeys.has(k)) {
      const info = bannedDeviceKeys.get(k);
      return { banned: true, reason: info.reason || 'This phone device has been hardware-banned by Admin.' };
    }
  }
  return { banned: false, reason: '' };
}

function kickAndNotifyBannedUser(uid, reason) {
  const cleanId = String(uid || '').trim();
  if (!cleanId) return;
  for (const r of rooms.values()) {
    if (r.activeClients && r.activeClients.has(cleanId)) {
      const sock = r.activeClients.get(cleanId);
      removeUserFromRoom(sock, r.id, cleanId);
    }
  }
  sendToUser(cleanId, {
    type: 'ACCOUNT_BANNED',
    userId: cleanId,
    reason: reason || 'Your account has been suspended by Admin.',
    bannedAtMs: Date.now(),
  });
}

// Public API for app dynamic catalog
app.get('/api/config/catalog', (req, res) => {
  res.json(buildDynamicCatalogPayload());
});

const EMBEDDED_ADMIN_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>JustFun — Master Admin Control Panel</title>
  <style>
    :root {
      --bg: #0b0f17;
      --card: #151c28;
      --card-2: #1c2536;
      --border: #28344b;
      --accent: #19cc9d;
      --accent-2: #4e6fff;
      --danger: #ff4d4f;
      --warn: #ffb74d;
      --text: #f3f6fa;
      --muted: #94a3b8;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body { background: var(--bg); color: var(--text); min-height: 100vh; display: flex; flex-direction: column; }
    header {
      background: var(--card);
      border-bottom: 1px solid var(--border);
      padding: 14px 22px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 12px;
      position: sticky;
      top: 0;
      z-index: 20;
    }
    .brand { display: flex; align-items: center; gap: 10px; font-size: 18px; font-weight: 800; color: var(--accent); }
    .auth-bar { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    input, select, textarea {
      background: var(--bg);
      border: 1px solid var(--border);
      color: var(--text);
      padding: 9px 12px;
      border-radius: 8px;
      font-size: 13.5px;
      outline: none;
    }
    input:focus, select:focus, textarea:focus { border-color: var(--accent); }
    button {
      background: var(--accent);
      color: #062119;
      border: none;
      padding: 9px 15px;
      border-radius: 8px;
      font-weight: 700;
      font-size: 13px;
      cursor: pointer;
      transition: opacity 0.15s;
    }
    button:hover { opacity: 0.9; }
    button.secondary { background: var(--card-2); color: var(--text); border: 1px solid var(--border); }
    button.danger { background: var(--danger); color: #fff; }
    button.warn { background: var(--warn); color: #1a1200; }
    .layout { display: flex; flex: 1; min-height: 0; }
    nav {
      width: 230px;
      background: var(--card);
      border-right: 1px solid var(--border);
      padding: 14px 10px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .nav-btn {
      text-align: left;
      background: transparent;
      color: var(--muted);
      padding: 11px 14px;
      border-radius: 8px;
      font-size: 13.5px;
      font-weight: 600;
    }
    .nav-btn.active { background: rgba(25, 204, 157, 0.15); color: var(--accent); border: 1px solid rgba(25, 204, 157, 0.35); }
    main { flex: 1; padding: 22px; overflow-y: auto; max-width: 1400px; }
    .stats-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(175px, 1fr));
      gap: 12px;
      margin-bottom: 20px;
    }
    .stat-card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px 16px;
    }
    .stat-label { color: var(--muted); font-size: 12px; margin-bottom: 6px; }
    .stat-val { font-size: 22px; font-weight: 800; color: var(--accent); }
    .panel { display: none; }
    .panel.active { display: block; }
    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 18px;
      margin-bottom: 18px;
    }
    .card h3 { font-size: 16px; margin-bottom: 12px; color: var(--text); display: flex; align-items: center; justify-content: space-between; }
    .form-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
      gap: 10px;
      margin-bottom: 12px;
    }
    table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 8px; }
    th, td { border-bottom: 1px solid var(--border); padding: 10px 8px; text-align: left; vertical-align: middle; }
    th { color: var(--muted); font-weight: 600; }
    .badge {
      display: inline-block;
      padding: 3px 8px;
      border-radius: 99px;
      font-size: 11px;
      font-weight: 700;
    }
    .badge.ok { background: rgba(25, 204, 157, 0.16); color: var(--accent); }
    .badge.ban { background: rgba(255, 77, 79, 0.2); color: var(--danger); }
    .toast {
      position: fixed;
      bottom: 20px;
      right: 20px;
      background: #111827;
      border: 1px solid var(--accent);
      color: var(--text);
      padding: 12px 18px;
      border-radius: 10px;
      font-size: 13.5px;
      font-weight: 600;
      display: none;
      z-index: 100;
      box-shadow: 0 10px 25px rgba(0,0,0,0.45);
    }
    @media (max-width: 820px) {
      .layout { flex-direction: column; }
      nav { width: 100%; flex-direction: row; overflow-x: auto; border-right: none; border-bottom: 1px solid var(--border); }
      .nav-btn { white-space: nowrap; }
    }
  </style>
</head>
<body>
  <header>
    <div class="brand">⚡ JustFun Master Admin Panel (Real-Time API)</div>
    <div class="auth-bar">
      <input type="password" id="adminKey" placeholder="Admin Secret Key" value="justfun-admin-2026" />
      <button onclick="loadAll()">🔄 Sync Live State</button>
    </div>
  </header>

  <div class="layout">
    <nav>
      <button class="nav-btn active" onclick="switchTab('users', this)">👤 Users, ID & Device Ban</button>
      <button class="nav-btn" onclick="switchTab('wallet', this)">💰 Wallet, Coins & Packs</button>
      <button class="nav-btn" onclick="switchTab('gifts', this)">🎁 Gifts Manager (API)</button>
      <button class="nav-btn" onclick="switchTab('store', this)">🏎️ Store: Frame / Car / Bubble</button>
      <button class="nav-btn" onclick="switchTab('medals', this)">🏅 Medals & Levels</button>
      <button class="nav-btn" onclick="switchTab('events', this)">🎉 Events & Broadcast</button>
      <button class="nav-btn" onclick="switchTab('rules', this)">🛡️ Invite & Chat Rules</button>
      <button class="nav-btn" onclick="switchTab('rooms', this)">🎤 Live Rooms & Reports</button>
    </nav>

    <main>
      <div class="stats-grid" id="statsGrid">
        <div class="stat-card"><div class="stat-label">Registered Users</div><div class="stat-val" id="stUsers">0</div></div>
        <div class="stat-card"><div class="stat-label">Online Sockets</div><div class="stat-val" id="stOnline">0</div></div>
        <div class="stat-card"><div class="stat-label">Live Voice Rooms</div><div class="stat-val" id="stRooms">0</div></div>
        <div class="stat-card"><div class="stat-label">API Gifts Count</div><div class="stat-val" id="stGifts">0</div></div>
        <div class="stat-card"><div class="stat-label">Banned IDs / Devices</div><div class="stat-val" id="stBanned">0 / 0</div></div>
        <div class="stat-card"><div class="stat-label">Unfollowed DM Limit</div><div class="stat-val" id="stDmLimit">5</div></div>
      </div>

      <section class="panel active" id="panel-users">
        <div class="card">
          <h3>🚫 Quick Ban / Unban User ID or Hardware Phone Device</h3>
          <div class="form-grid">
            <input id="banUserId" placeholder="8-Digit User ID (e.g. 12345678)" />
            <input id="banReason" placeholder="Ban Reason (e.g. Fraud / Abuse)" value="Violated community rules" />
            <button class="danger" onclick="banUser(true)">🚫 Ban User ID</button>
            <button class="secondary" onclick="banUser(false)">✅ Unban User ID</button>
          </div>
          <div class="form-grid">
            <input id="banDeviceTarget" placeholder="User ID or Device Key to Hardware-Ban" />
            <button class="danger" onclick="banDevice(true)">📱 Ban Full Phone (Device)</button>
            <button class="secondary" onclick="banDevice(false)">🔓 Unban Phone Device</button>
            <button class="warn" onclick="resetDeviceSlots()">🔄 Reset Phone 3-ID Limit</button>
          </div>
        </div>

        <div class="card">
          <h3>👥 All Users (Real-Time)</h3>
          <div style="overflow-x:auto;">
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Nickname</th>
                  <th>Status</th>
                  <th>Lv / Charm</th>
                  <th>Coins / Diamonds</th>
                  <th>Device Keys</th>
                  <th>Quick Actions</th>
                </tr>
              </thead>
              <tbody id="usersTableBody"></tbody>
            </table>
          </div>
        </div>
      </section>

      <section class="panel" id="panel-wallet">
        <div class="card">
          <h3>💰 Add / Deduct User Coins & Diamonds (Live Sync)</h3>
          <div class="form-grid">
            <input id="walUserId" placeholder="8-Digit User ID" />
            <input id="walCoinsDelta" type="number" placeholder="Coins (+Add or -Deduct)" value="100000" />
            <input id="walDiamondsDelta" type="number" placeholder="Diamonds (+Add or -Deduct)" value="0" />
            <input id="walNote" placeholder="Reason / Title" value="Admin Wallet Adjustment" />
            <button onclick="adjustWallet()">⚡ Apply to Wallet</button>
          </div>
        </div>

        <div class="card">
          <h3>💎 Edit Wallet Recharge Packs (API Synced)</h3>
          <div class="form-grid">
            <input id="packId" placeholder="Pack ID (p1, p2, p3, p4)" value="p1" />
            <input id="packCoins" type="number" placeholder="Coins Amount" value="800000" />
            <input id="packInr" type="number" placeholder="Price (INR ₹)" value="100" />
            <button onclick="saveRechargePack()">💾 Update Recharge Pack</button>
          </div>
          <div id="packsPreview" style="color:var(--muted); font-size:13px; margin-top:8px;"></div>
        </div>
      </section>

      <section class="panel" id="panel-gifts">
        <div class="card" style="border-color: rgba(25,204,157,0.45); background: linear-gradient(145deg, #151c28, #13222d);">
          <h3>🎬 Upload New MP4 Gift (Phone / PC Gallery → Cloudinary & Firestore)</h3>
          <div class="form-grid">
            <input id="mp4GiftName" placeholder="Gift Name (e.g. Royal Dragon)" />
            <input id="mp4GiftPrice" type="number" min="1" placeholder="Price in Coins (Int, e.g. 5000)" />
            <select id="mp4GiftCategory">
              <option value="Gifts">Gifts</option>
              <option value="Customized">Customized</option>
              <option value="Lucky">Lucky</option>
              <option value="Popular">Popular</option>
              <option value="Luxury">Luxury</option>
              <option value="CP">CP</option>
            </select>
            <input id="mp4GiftTag" placeholder="Badge Tag (MP4 / HOT / VIP)" value="MP4" />
          </div>

          <div style="display:flex; flex-wrap:wrap; gap:12px; margin-bottom:14px;">
            <div style="flex:1; min-width:240px; background:var(--bg); border:1px solid var(--border); border-radius:12px; padding:12px;">
              <input type="file" id="mp4VideoFile" accept="video/mp4" style="display:none;" onchange="onMp4VideoSelected(this)" />
              <button type="button" id="btnSelectMp4Video" onclick="document.getElementById('mp4VideoFile').click()" style="width:100%; background:#1f3348; color:#4de8c0; border:1px solid #19cc9d;">
                🎥 Select MP4 Video
              </button>
              <div id="mp4VideoFileName" style="margin-top:8px; font-size:12px; color:var(--muted); word-break:break-all;">
                No MP4 video selected yet (MIME: video/mp4)
              </div>
            </div>

            <div style="flex:1; min-width:240px; background:var(--bg); border:1px solid var(--border); border-radius:12px; padding:12px; display:flex; align-items:center; gap:12px;">
              <img id="mp4ThumbPreview" src="" alt="" style="display:none; width:48px; height:48px; object-fit:cover; border-radius:10px; border:1px solid var(--gold); flex-shrink:0;" />
              <div style="flex:1; min-width:0;">
                <input type="file" id="mp4ThumbFile" accept="image/*" style="display:none;" onchange="onMp4ThumbSelected(this)" />
                <button type="button" id="btnSelectThumbIcon" onclick="document.getElementById('mp4ThumbFile').click()" style="width:100%; background:#2b2618; color:#ffc83d; border:1px solid #ffc83d;">
                  🖼️ Select Thumbnail Icon
                </button>
                <div id="mp4ThumbFileName" style="margin-top:8px; font-size:12px; color:var(--muted); word-break:break-all;">
                  No thumbnail icon selected yet (image/*)
                </div>
              </div>
            </div>
          </div>

          <div id="mp4ProgressWrap" style="display:none; margin-bottom:14px; background:var(--bg); border:1px solid var(--border); border-radius:12px; padding:12px;">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px; font-size:13px;">
              <span id="mp4ProgressStage" style="color:#d6ddf7; font-weight:600;">Preparing upload...</span>
              <span id="mp4ProgressPct" style="color:var(--accent); font-weight:800;">0%</span>
            </div>
            <div style="width:100%; height:10px; background:#1c2536; border-radius:8px; overflow:hidden;">
              <div id="mp4ProgressFill" style="width:0%; height:100%; background:linear-gradient(90deg, #19cc9d, #4e6fff); transition:width 0.15s ease;"></div>
            </div>
          </div>

          <button type="button" id="btnUploadSaveMp4Gift" onclick="uploadAndSaveMp4Gift()" style="width:100%; padding:13px 18px; font-size:15px;">
            ☁️ Upload &amp; Save Gift
          </button>
        </div>

        <div class="card">
          <h3>🎁 Add or Update Gift via API (Loads Directly in App Without APK Update)</h3>
          <div class="form-grid">
            <input id="giftId" placeholder="Gift ID (e.g. g_dragon)" />
            <input id="giftName" placeholder="Gift Name (e.g. Golden Dragon)" />
            <input id="giftPrice" type="number" placeholder="Price in Coins (e.g. 5000)" />
            <select id="giftCategory">
              <option value="Popular">Popular</option>
              <option value="Luxury">Luxury</option>
              <option value="Privilege">Privilege</option>
              <option value="CP">CP</option>
            </select>
            <input id="giftTag" placeholder="Tag (HOT / LUXURY / VIP)" value="HOT" />
            <input id="giftIcon" placeholder="Emoji (🌹) OR Image URL (https://...)" value="🐉" />
            <input id="giftAnimUrl" placeholder="Full-Screen GIF/WebP/PNG URL (Optional)" />
            <input id="giftDuration" type="number" step="0.5" placeholder="Duration Sec (e.g. 4.0)" value="3.5" />
            <button onclick="saveGift()">➕ Save / Update Gift</button>
          </div>
        </div>

        <div class="card">
          <h3>📦 Active API Gifts Catalog</h3>
          <div style="overflow-x:auto;">
            <table>
              <thead>
                <tr><th>ID</th><th>Icon</th><th>Name</th><th>Category / Tag</th><th>Coins</th><th>Animation URL</th><th>Action</th></tr>
              </thead>
              <tbody id="giftsTableBody"></tbody>
            </table>
          </div>
        </div>
      </section>

      <section class="panel" id="panel-store">
        <div class="card">
          <h3>🎁 Grant VIP Frame / Entry Car / Bubble / Special ID to User</h3>
          <div class="form-grid">
            <input id="grantUserId" placeholder="8-Digit User ID" />
            <select id="grantItemId"></select>
            <input id="grantDays" type="number" placeholder="Validity Days (e.g. 7, 15, 30)" value="30" />
            <button onclick="grantStoreItem()">🚀 Grant & Equip Item Live</button>
          </div>
        </div>

        <div class="card">
          <h3>🏎️ Add or Update Store Catalog Item (Frame / Vehicle / Bubble / Mic Wave / Special ID)</h3>
          <div class="form-grid">
            <input id="stItemId" placeholder="Item ID (e.g. f_royal_king)" />
            <input id="stItemName" placeholder="Item Name (e.g. Royal King Frame)" />
            <input id="stItemSub" placeholder="Subtitle / Effect Note" />
            <select id="stItemCat">
              <option value="FRAME">FRAME (Avatar Frame)</option>
              <option value="VEHICLE">VEHICLE (Entry Car)</option>
              <option value="BUBBLE">BUBBLE (Chat Bubble)</option>
              <option value="MIC_WAVE">MIC_WAVE (Mic Wave)</option>
              <option value="SPECIAL_ID">SPECIAL_ID (VIP ID)</option>
            </select>
            <input id="stItemPrice" type="number" placeholder="7-Day Base Price (Coins)" value="12000" />
            <select id="stItemStars">
              <option value="1">1 Star ★</option>
              <option value="2">2 Stars ★★</option>
              <option value="3" selected>3 Stars ★★★</option>
            </select>
            <input id="stItemIconUrl" placeholder="Custom Image/GIF URL (https://...)" />
            <input id="stSpecialIdNum" placeholder="Special ID Number (if SPECIAL_ID)" />
            <button onclick="saveStoreCatalogItem()">💾 Save Store Item</button>
          </div>
          <div style="overflow-x:auto;">
            <table>
              <thead>
                <tr><th>ID</th><th>Name</th><th>Category</th><th>7-Day Price</th><th>Image URL</th><th>Action</th></tr>
              </thead>
              <tbody id="storeTableBody"></tbody>
            </table>
          </div>
        </div>
      </section>

      <section class="panel" id="panel-medals">
        <div class="card">
          <h3>🏅 Set User Level / EXP or Grant Medal to User ID</h3>
          <div class="form-grid">
            <input id="medUserId" placeholder="8-Digit User ID" />
            <input id="medLevel" type="number" placeholder="New Level (e.g. 25, leave blank to skip)" />
            <input id="medIdToGrant" placeholder="Medal ID (e.g. ach_greedy_king, m_superstar)" />
            <button onclick="updateUserMedalAndLevel()">⚡ Update Level / Unlock Medal</button>
          </div>
        </div>

        <div class="card">
          <h3>🎖️ Add or Update Gift-Linked Star Medal (1★ to 4★ Real-Time Sync)</h3>
          <div class="form-grid">
            <input id="newMedalId" placeholder="Medal ID (e.g. g_ferris_star)" />
            <input id="newMedalName" placeholder="Medal Title (e.g. Ferris Wheel Star)" />
            <select id="newMedalCat">
              <option value="GIFT">GIFT (Gift-Linked)</option>
              <option value="ACHIEVEMENT">ACHIEVEMENT (Total Coins)</option>
              <option value="ACTIVITY">ACTIVITY</option>
            </select>
            <select id="newMedalLinkType">
              <option value="SPECIFIC_GIFT">Specific Gift (By Gift ID / Name)</option>
              <option value="ALL_GIFTS_COINS">Total Gift Coins Sent (All Gifts)</option>
            </select>
            <input id="newMedalGiftId" placeholder="Linked Gift ID (e.g. g_ferris) or blank" />
            <input id="newMedalGiftName" placeholder="Linked Gift Name (e.g. Ferris Wheel)" />
            <input id="newMedalStar1" type="number" value="1000000" placeholder="1★ Coins (e.g. 10,00,000)" />
            <input id="newMedalStar2" type="number" value="5000000" placeholder="2★ Coins (e.g. 50,00,000)" />
            <input id="newMedalStar3" type="number" value="10000000" placeholder="3★ Coins (e.g. 1,00,00,000)" />
            <input id="newMedalStar4" type="number" value="50000000" placeholder="4★ Coins (e.g. 5,00,00,000)" />
            <input id="newMedalDesc" placeholder="Description" value="Send linked gift in voice rooms to upgrade 1★ to 4★" />
            <input id="newMedalIconUrl" placeholder="Medal Icon URL (https://...)" />
            <button onclick="saveCustomMedal()">➕ Save / Update Gift-Linked Medal</button>
          </div>
          <div id="customMedalsPreview" style="color:var(--muted); font-size:13px; margin-top:8px;"></div>
        </div>
      </section>

      <section class="panel" id="panel-events">
        <div class="card">
          <h3>📢 Send Real-Time Notification / Customer Service Message</h3>
          <div class="form-grid">
            <input id="bcTargetUserId" placeholder="Target User ID (Leave blank for ALL users)" />
            <input id="bcTitle" placeholder="Notification Title" value="🎉 Official JustFun Reward Event!" />
            <input id="bcContent" placeholder="Message Content" value="Join live voice rooms tonight at 9 PM for double Lucky Box rewards!" />
            <select id="bcChannel">
              <option value="NOTIFICATION">System Notification + Customer Service</option>
              <option value="CUSTOMER_SERVICE">Customer Service Chat Only</option>
            </select>
            <button onclick="sendBroadcast()">📣 Send Live Broadcast</button>
          </div>
        </div>

        <div class="card">
          <h3>🎪 Manage Official Events & Home Banners</h3>
          <div class="form-grid">
            <input id="evTitle" placeholder="Event Title" />
            <input id="evSubtitle" placeholder="Event Description / Rules" />
            <input id="evBannerUrl" placeholder="Banner Image URL (https://...)" />
            <button onclick="addEventBanner()">➕ Publish Event / Banner</button>
          </div>
          <div style="overflow-x:auto;">
            <table>
              <thead><tr><th>ID</th><th>Title</th><th>Description</th><th>Banner URL</th><th>Action</th></tr></thead>
              <tbody id="eventsTableBody"></tbody>
            </table>
          </div>
        </div>
      </section>

      <section class="panel" id="panel-rules">
        <div class="card">
          <h3>🛡️ Global App Rules, Invite Rewards & Anti-Cheat Settings</h3>
          <div class="form-grid">
            <div>
              <label class="stat-label">Max Messages Without Mutual Follow</label>
              <input id="ruleDmLimit" type="number" value="5" style="width:100%" />
            </div>
            <div>
              <label class="stat-label">Max Accounts Per Phone (Device Limit)</label>
              <input id="ruleMaxAccounts" type="number" value="3" style="width:100%" />
            </div>
            <div>
              <label class="stat-label">Invite Day-1 Reward (Coins)</label>
              <input id="ruleInvDay1" type="number" value="50000" style="width:100%" />
            </div>
            <div>
              <label class="stat-label">Invite Day-3 Reward (Coins)</label>
              <input id="ruleInvDay3" type="number" value="150000" style="width:100%" />
            </div>
            <div>
              <label class="stat-label">Invited Friend Welcome Bonus (Coins)</label>
              <input id="ruleInvWelcome" type="number" value="250000" style="width:100%" />
            </div>
            <div>
              <label class="stat-label">Daily Check-In Reward (Coins)</label>
              <input id="ruleCheckInCoins" type="number" value="10" style="width:100%" />
            </div>
          </div>
          <button onclick="saveGlobalRules()">💾 Save & Broadcast Rules Live</button>
        </div>
      </section>

      <section class="panel" id="panel-rooms">
        <div class="card">
          <h3>🎤 Active Voice Party Rooms</h3>
          <div style="overflow-x:auto;">
            <table>
              <thead><tr><th>Room ID</th><th>Title</th><th>Host</th><th>Online</th><th>Official</th><th>Actions</th></tr></thead>
              <tbody id="roomsTableBody"></tbody>
            </table>
          </div>
        </div>

        <div class="card">
          <h3>🚩 User Reports & Feedback Tickets</h3>
          <div style="overflow-x:auto;">
            <table>
              <thead><tr><th>Type</th><th>From User</th><th>Target / Contact</th><th>Reason / Content</th><th>Time</th></tr></thead>
              <tbody id="reportsTableBody"></tbody>
            </table>
          </div>
        </div>
      </section>
    </main>
  </div>

  <div class="toast" id="toast"></div>

  <script>
    function getSecret() {
      const el = document.getElementById('adminKey');
      return el ? el.value.trim() : 'justfun-admin-2026';
    }
    var secret = 'justfun-admin-2026';

    function showToast(msg, isErr = false) {
      const el = document.getElementById('toast');
      el.textContent = msg;
      el.style.borderColor = isErr ? '#ff4d4f' : '#19cc9d';
      el.style.display = 'block';
      clearTimeout(window._toastTimer);
      window._toastTimer = setTimeout(() => { el.style.display = 'none'; }, 3200);
    }

    function switchTab(id, btn) {
      document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
      document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
      document.getElementById('panel-' + id).classList.add('active');
      if (btn) btn.classList.add('active');
    }

    async function api(path, method = 'GET', body = null) {
      const key = getSecret();
      secret = key;
      const opts = {
        method,
        headers: {
          'Content-Type': 'application/json',
          'x-admin-key': key,
          'x-admin-secret': key
        }
      };
      if (body) opts.body = JSON.stringify(body);
      const res = await fetch(path, opts);
      const data = await res.json();
      if (!res.ok || data.error) throw new Error(data.error || 'Request failed');
      return data;
    }

    async function loadAll() {
      try {
        const d = await api('/api/admin/overview');
        document.getElementById('stUsers').textContent = d.stats.totalUsers;
        document.getElementById('stOnline').textContent = d.stats.onlineUsers;
        document.getElementById('stRooms').textContent = d.stats.liveRoomsCount;
        document.getElementById('stGifts').textContent = d.gifts.length;
        document.getElementById('stBanned').textContent = d.stats.bannedUsersCount + ' / ' + d.stats.bannedDevicesCount;
        document.getElementById('stDmLimit').textContent = d.rules.maxUnfollowedDmMessages;

        document.getElementById('ruleDmLimit').value = d.rules.maxUnfollowedDmMessages;
        document.getElementById('ruleMaxAccounts').value = d.rules.maxAccountsPerDevice;
        document.getElementById('ruleInvDay1').value = d.rules.inviteDay1Coins;
        document.getElementById('ruleInvDay3').value = d.rules.inviteDay3Coins;
        document.getElementById('ruleInvWelcome').value = d.rules.inviteFriendWelcomeCoins;
        document.getElementById('ruleCheckInCoins').value = d.rules.dailyCheckInCoins;

        const uBody = document.getElementById('usersTableBody');
        uBody.innerHTML = d.users.map(u => '<tr>' +
          '<td><b>' + u.userId + '</b></td>' +
          '<td>' + (u.nickname || '-') + '</td>' +
          '<td>' + (u.isBanned ? '<span class="badge ban">BANNED</span>' : (u.isOnline ? '<span class="badge ok">ONLINE</span>' : '<span class="badge">OFFLINE</span>')) + '</td>' +
          '<td>Lv.' + (u.level || 0) + ' / ✨' + (u.totalCharmPoints || 0) + '</td>' +
          '<td>🪙 ' + Number(u.coins || 0).toLocaleString() + ' / 💎 ' + Number(u.diamonds || 0).toLocaleString() + '</td>' +
          '<td style="font-size:11px; color:var(--muted); max-width:180px; word-break:break-all;">' + ((u.deviceKeys || []).join(', ') || '-') + '</td>' +
          '<td>' +
            '<button class="' + (u.isBanned ? 'secondary' : 'danger') + '" style="padding:5px 9px; font-size:11px; margin-right:4px;" onclick="quickToggleBan(\\'' + u.userId + '\\', ' + (!u.isBanned) + ')">' + (u.isBanned ? 'Unban ID' : 'Ban ID') + '</button>' +
            '<button class="secondary" style="padding:5px 9px; font-size:11px;" onclick="prefillWallet(\\'' + u.userId + '\\')">+Coins</button>' +
          '</td></tr>'
        ).join('');

        document.getElementById('packsPreview').innerHTML = Object.entries(d.rechargePacks || {})
          .map(([k, v]) => '<b>' + k + '</b>: ' + Number(v.coins).toLocaleString() + ' Coins (₹' + v.priceInr + ')')
          .join(' &nbsp;|&nbsp; ');

        window._cachedCatalogData = { gifts: d.gifts || [], storeCatalog: d.storeCatalog || [], customMedals: d.customMedals || [] };
        const gBody = document.getElementById('giftsTableBody');
        gBody.innerHTML = d.gifts.map(g => '<tr>' +
          '<td><b>' + g.id + '</b></td>' +
          '<td style="font-size:20px;">' + (g.icon && g.icon.startsWith('http') ? '<img src="' + g.icon + '" style="width:28px;height:28px;border-radius:6px;object-fit:cover;"/>' : (g.icon || '🎁')) + '</td>' +
          '<td>' + g.name + '</td>' +
          '<td>' + g.category + ' <span class="badge ok">' + (g.tag || 'HOT') + '</span></td>' +
          '<td>🪙 ' + Number(g.priceCoins).toLocaleString() + '</td>' +
          '<td style="font-size:11px; color:var(--muted); max-width:200px; overflow:hidden; text-overflow:ellipsis;">' + (g.animationUrl || 'Built-in') + '</td>' +
          '<td>' +
            '<button class="secondary" style="padding:5px 9px; font-size:11px; margin-right:4px;" onclick="editGift(\\'' + g.id + '\\')">Edit</button>' +
            '<button class="danger" style="padding:5px 9px; font-size:11px;" onclick="deleteGift(\\'' + g.id + '\\')">Delete</button>' +
          '</td>' +
        '</tr>').join('');

        const grantSel = document.getElementById('grantItemId');
        grantSel.innerHTML = d.storeCatalog.map(item => '<option value="' + item.id + '">[' + item.category + '] ' + item.name + ' (' + item.id + ')</option>').join('');
        document.getElementById('storeTableBody').innerHTML = d.storeCatalog.map(item => '<tr>' +
          '<td><b>' + item.id + '</b></td>' +
          '<td>' + item.name + '</td>' +
          '<td><span class="badge ok">' + item.category + '</span></td>' +
          '<td>🪙 ' + Number(item.basePrice7Days).toLocaleString() + '</td>' +
          '<td style="font-size:11px; color:var(--muted);">' + (item.iconUrl ? '<img src="' + item.iconUrl + '" style="width:24px;height:24px;border-radius:4px;vertical-align:middle;margin-right:4px;object-fit:cover;"/>URL' : 'Built-in') + '</td>' +
          '<td>' +
            '<button class="secondary" style="padding:5px 9px; font-size:11px; margin-right:4px;" onclick="editStoreItem(\\'' + item.id + '\\')">Edit</button>' +
            '<button class="danger" style="padding:5px 9px; font-size:11px;" onclick="deleteStoreItem(\\'' + item.id + '\\')">Remove</button>' +
          '</td>' +
        '</tr>').join('');

        document.getElementById('customMedalsPreview').innerHTML = (d.customMedals || [])
          .map(m => '<span class="badge ok" style="margin:4px 6px 4px 0; display:inline-flex; align-items:center; gap:6px;">🏅 ' + m.id + ': ' + m.name + ' (' + (m.linkedGiftName ? ('Gift: ' + m.linkedGiftName) : (m.category || 'ACHIEVEMENT')) + ' | 1★:' + ((m.starThresholds && m.starThresholds[0]) || m.targetProgress || 1000000).toLocaleString() + ') <a href="#" style="color:#ff8a80; text-decoration:none; font-weight:800;" onclick="deleteCustomMedal(\\'' + m.id + '\\');return false;">✕</a></span>').join('') || 'No custom medals added yet.';

        document.getElementById('eventsTableBody').innerHTML = (d.events || []).map(ev => '<tr>' +
          '<td>' + ev.id + '</td>' +
          '<td><b>' + ev.title + '</b></td>' +
          '<td>' + ev.subtitle + '</td>' +
          '<td style="font-size:11px; color:var(--muted);">' + (ev.bannerUrl || 'Default') + '</td>' +
          '<td><button class="danger" style="padding:5px 9px; font-size:11px;" onclick="deleteEvent(\\'' + ev.id + '\\')">Delete</button></td>' +
        '</tr>').join('');

        document.getElementById('roomsTableBody').innerHTML = (d.rooms || []).map(r => '<tr>' +
          '<td><b>' + r.id + '</b></td>' +
          '<td>' + r.title + '</td>' +
          '<td>' + r.hostName + ' (' + r.hostId + ')</td>' +
          '<td>🔥 ' + r.onlineCount + '</td>' +
          '<td>' + (r.isOfficial ? '<span class="badge ok">OFFICIAL</span>' : '-') + '</td>' +
          '<td>' +
            '<button class="secondary" style="padding:5px 9px; font-size:11px; margin-right:4px;" onclick="toggleOfficialRoom(\\'' + r.id + '\\', ' + (!r.isOfficial) + ')">' + (r.isOfficial ? 'Unpin' : 'Pin Official') + '</button>' +
            '<button class="danger" style="padding:5px 9px; font-size:11px;" onclick="closeRoom(\\'' + r.id + '\\')">Close Room</button>' +
          '</td></tr>'
        ).join('');

        const repRows = [
          ...(d.reports || []).map(r => ({ type: 'REPORT', from: r.reporterId, target: r.targetUserId || r.targetRoomId, text: r.reason + ': ' + (r.details || ''), ts: r.createdAt })),
          ...(d.feedbacks || []).map(f => ({ type: 'FEEDBACK', from: f.userId, target: f.contact || '-', text: '[' + f.type + '] ' + f.content, ts: f.createdAt }))
        ];
        document.getElementById('reportsTableBody').innerHTML = repRows.map(r => '<tr>' +
          '<td><span class="badge ' + (r.type === 'REPORT' ? 'ban' : 'ok') + '">' + r.type + '</span></td>' +
          '<td>' + r.from + '</td>' +
          '<td>' + r.target + '</td>' +
          '<td>' + r.text + '</td>' +
          '<td style="font-size:11px; color:var(--muted);">' + new Date(r.ts || Date.now()).toLocaleString() + '</td>' +
        '</tr>').join('');
      } catch (e) {
        showToast(e.message, true);
      }
    }

    function prefillWallet(uid) {
      document.getElementById('walUserId').value = uid;
      switchTab('wallet', document.querySelectorAll('.nav-btn')[1]);
    }

    async function quickToggleBan(uid, ban) {
      document.getElementById('banUserId').value = uid;
      await banUser(ban);
    }

    async function banUser(ban) {
      try {
        const userId = document.getElementById('banUserId').value.trim();
        const reason = document.getElementById('banReason').value.trim();
        await api('/api/admin/users/ban', 'POST', { userId, ban, reason });
        showToast(ban ? 'User ' + userId + ' BANNED!' : 'User ' + userId + ' UNBANNED!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    async function banDevice(ban) {
      try {
        const target = document.getElementById('banDeviceTarget').value.trim();
        const reason = document.getElementById('banReason').value.trim();
        await api('/api/admin/devices/ban', 'POST', { target, ban, reason });
        showToast(ban ? 'Hardware Device BANNED for ' + target + '!' : 'Hardware Device UNBANNED!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    async function resetDeviceSlots() {
      try {
        const target = document.getElementById('banDeviceTarget').value.trim();
        await api('/api/admin/devices/reset-slots', 'POST', { target });
        showToast('3-ID Device Slots reset for ' + (target || 'ALL') + '!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    async function adjustWallet() {
      try {
        const userId = document.getElementById('walUserId').value.trim();
        const coinsDelta = Number(document.getElementById('walCoinsDelta').value || 0);
        const diamondsDelta = Number(document.getElementById('walDiamondsDelta').value || 0);
        const title = document.getElementById('walNote').value.trim();
        await api('/api/admin/wallet/adjust', 'POST', { userId, coinsDelta, diamondsDelta, title });
        showToast('Wallet updated for ' + userId + '!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    async function saveRechargePack() {
      try {
        const packId = document.getElementById('packId').value.trim();
        const coins = Number(document.getElementById('packCoins').value || 0);
        const priceInr = Number(document.getElementById('packInr').value || 0);
        await api('/api/admin/wallet/packs', 'POST', { packId, coins, priceInr });
        showToast('Pack ' + packId + ' updated!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    function editGift(id) {
      const g = (window._cachedCatalogData?.gifts || []).find(x => x.id === id);
      if (!g) return;
      document.getElementById('giftId').value = g.id || '';
      document.getElementById('giftName').value = g.name || '';
      document.getElementById('giftPrice').value = g.priceCoins || 10;
      document.getElementById('giftCategory').value = g.category || 'Popular';
      document.getElementById('giftTag').value = g.tag || 'HOT';
      document.getElementById('giftIcon').value = g.icon || '🎁';
      document.getElementById('giftAnimUrl').value = g.animationUrl || '';
      document.getElementById('giftDuration').value = g.durationSeconds || 3.0;
    }

    async function saveGift() {
      try {
        const body = {
          id: document.getElementById('giftId').value.trim(),
          name: document.getElementById('giftName').value.trim(),
          priceCoins: Number(document.getElementById('giftPrice').value || 10),
          category: document.getElementById('giftCategory').value,
          tag: document.getElementById('giftTag').value.trim() || 'HOT',
          icon: document.getElementById('giftIcon').value.trim() || '🎁',
          animationUrl: document.getElementById('giftAnimUrl').value.trim(),
          durationSeconds: Number(document.getElementById('giftDuration').value || 3.0),
          active: true
        };
        await api('/api/admin/gifts', 'POST', body);
        showToast('Gift "' + body.name + '" saved & synced to Firestore!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    function onMp4VideoSelected(input) {
      const f = input.files && input.files[0];
      const label = document.getElementById('mp4VideoFileName');
      if (!f) {
        label.textContent = 'No MP4 video selected yet (MIME: video/mp4)';
        label.style.color = 'var(--muted)';
        return;
      }
      const mb = (f.size / (1024 * 1024)).toFixed(2);
      label.textContent = '✅ Selected: ' + f.name + ' (' + mb + ' MB)';
      label.style.color = 'var(--accent)';
    }

    function onMp4ThumbSelected(input) {
      const f = input.files && input.files[0];
      const label = document.getElementById('mp4ThumbFileName');
      const preview = document.getElementById('mp4ThumbPreview');
      if (!f) {
        label.textContent = 'No thumbnail icon selected yet (image/*)';
        label.style.color = 'var(--muted)';
        preview.style.display = 'none';
        preview.src = '';
        return;
      }
      label.textContent = '✅ Selected: ' + f.name;
      label.style.color = 'var(--gold)';
      preview.src = URL.createObjectURL(f);
      preview.style.display = 'block';
    }

    function setMp4UploadProgress(pct, stageText) {
      const clamped = Math.max(0, Math.min(100, Math.round(pct)));
      document.getElementById('mp4ProgressWrap').style.display = 'block';
      document.getElementById('mp4ProgressPct').textContent = clamped + '%';
      document.getElementById('mp4ProgressStage').textContent = stageText || 'Uploading...';
      document.getElementById('mp4ProgressFill').style.width = clamped + '%';
      const btn = document.getElementById('btnUploadSaveMp4Gift');
      if (btn.disabled) btn.textContent = '⏳ Uploading... ' + clamped + '%';
    }

    function uploadDirectToCloudinary(sign, file, onFraction) {
      return new Promise((resolve, reject) => {
        const formData = new FormData();
        formData.append('file', file);
        formData.append('api_key', sign.apiKey);
        formData.append('timestamp', String(sign.timestamp));
        formData.append('signature', sign.signature);
        formData.append('folder', sign.folder);
        formData.append('public_id', sign.publicId);

        const xhr = new XMLHttpRequest();
        xhr.open('POST', sign.uploadUrl, true);
        xhr.upload.onprogress = function(ev) {
          if (ev.lengthComputable && ev.total > 0) {
            onFraction(ev.loaded / ev.total);
          }
        };
        xhr.onload = function() {
          try {
            const data = JSON.parse(xhr.responseText || '{}');
            if (xhr.status >= 200 && xhr.status < 300 && (data.secure_url || data.url)) {
              resolve(data.secure_url || data.url);
            } else {
              reject(new Error((data.error && data.error.message) || ('Cloudinary HTTP ' + xhr.status)));
            }
          } catch (err) {
            reject(new Error('Cloudinary upload failed (HTTP ' + xhr.status + ')'));
          }
        };
        xhr.onerror = function() {
          reject(new Error('Network error while uploading directly to Cloudinary'));
        };
        xhr.send(formData);
      });
    }

    async function uploadAndSaveMp4Gift() {
      const name = document.getElementById('mp4GiftName').value.trim();
      const price = Math.floor(Number(document.getElementById('mp4GiftPrice').value || 0));
      const category = document.getElementById('mp4GiftCategory').value || 'Gifts';
      const tag = document.getElementById('mp4GiftTag').value.trim() || 'MP4';
      const videoFile = document.getElementById('mp4VideoFile').files[0];
      const thumbFile = document.getElementById('mp4ThumbFile').files[0];

      if (!name) return showToast('Please enter Gift Name', true);
      if (!price || price <= 0) return showToast('Please enter a valid Coin Price (Int)', true);
      if (!videoFile) return showToast('Please select an MP4 Video', true);
      if (!thumbFile) return showToast('Please select a Thumbnail Icon', true);

      const submitBtn = document.getElementById('btnUploadSaveMp4Gift');
      const vidBtn = document.getElementById('btnSelectMp4Video');
      const thumbBtn = document.getElementById('btnSelectThumbIcon');
      submitBtn.disabled = true;
      vidBtn.disabled = true;
      thumbBtn.disabled = true;

      try {
        setMp4UploadProgress(1, 'Preparing direct Cloudinary video upload...');
        const videoSign = await api('/api/admin/cloudinary/sign', 'POST', { kind: 'video' });
        const videoUrl = await uploadDirectToCloudinary(
          videoSign,
          videoFile,
          (f) => setMp4UploadProgress(2 + f * 73, 'Uploading MP4 video directly to Cloudinary...')
        );

        setMp4UploadProgress(76, 'Preparing direct Cloudinary thumbnail upload...');
        const iconSign = await api('/api/admin/cloudinary/sign', 'POST', { kind: 'icon' });
        const iconUrl = await uploadDirectToCloudinary(
          iconSign,
          thumbFile,
          (f) => setMp4UploadProgress(76 + f * 16, 'Uploading Thumbnail icon directly to Cloudinary...')
        );

        setMp4UploadProgress(94, 'Saving Cloudinary gift URLs to database...');
        await api('/api/admin/gifts/upload-save', 'POST', {
          name,
          price,
          category,
          tag,
          videoUrl,
          iconUrl,
          type: 'mp4',
          durationSeconds: 5.5
        });

        setMp4UploadProgress(100, 'Gift uploaded to Cloudinary & saved!');
        showToast('Gift "' + name + '" uploaded to Cloudinary & saved!');
        document.getElementById('mp4GiftName').value = '';
        document.getElementById('mp4GiftPrice').value = '';
        document.getElementById('mp4VideoFile').value = '';
        document.getElementById('mp4ThumbFile').value = '';
        onMp4VideoSelected(document.getElementById('mp4VideoFile'));
        onMp4ThumbSelected(document.getElementById('mp4ThumbFile'));
        loadAll();
      } catch (e) {
        showToast('Upload failed: ' + e.message, true);
      } finally {
        submitBtn.disabled = false;
        vidBtn.disabled = false;
        thumbBtn.disabled = false;
        submitBtn.textContent = '☁️ Upload & Save Gift';
      }
    }

    async function deleteGift(id) {
      try {
        await api('/api/admin/gifts/' + encodeURIComponent(id), 'DELETE');
        showToast('Gift ' + id + ' deleted!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    async function grantStoreItem() {
      try {
        const userId = document.getElementById('grantUserId').value.trim();
        const itemId = document.getElementById('grantItemId').value;
        const days = Number(document.getElementById('grantDays').value || 7);
        await api('/api/admin/store/grant', 'POST', { userId, itemId, days });
        showToast('Granted ' + itemId + ' to ' + userId + ' for ' + days + ' days!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    function editStoreItem(id) {
      const item = (window._cachedCatalogData?.storeCatalog || []).find(x => x.id === id);
      if (!item) return;
      document.getElementById('stItemId').value = item.id || '';
      document.getElementById('stItemName').value = item.name || '';
      document.getElementById('stItemSub').value = item.subtitle || '';
      document.getElementById('stItemCat').value = item.category || 'FRAME';
      document.getElementById('stItemPrice').value = item.basePrice7Days || 10000;
      document.getElementById('stItemStars').value = String(item.rarityStars || 2);
      document.getElementById('stItemIconUrl').value = item.iconUrl || '';
      document.getElementById('stSpecialIdNum').value = item.specialIdNumber || '';
    }

    async function saveStoreCatalogItem() {
      try {
        const body = {
          id: document.getElementById('stItemId').value.trim(),
          name: document.getElementById('stItemName').value.trim(),
          subtitle: document.getElementById('stItemSub').value.trim(),
          category: document.getElementById('stItemCat').value,
          basePrice7Days: Number(document.getElementById('stItemPrice').value || 10000),
          rarityStars: Number(document.getElementById('stItemStars').value || 2),
          iconUrl: document.getElementById('stItemIconUrl').value.trim(),
          specialIdNumber: document.getElementById('stSpecialIdNum').value.trim(),
          active: true
        };
        await api('/api/admin/store/catalog', 'POST', body);
        showToast('Store item "' + body.name + '" saved & synced to Firestore!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    async function deleteStoreItem(id) {
      try {
        await api('/api/admin/store/catalog/' + encodeURIComponent(id), 'DELETE');
        showToast('Store item ' + id + ' removed!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    async function updateUserMedalAndLevel() {
      try {
        const userId = document.getElementById('medUserId').value.trim();
        const level = document.getElementById('medLevel').value.trim();
        const medalId = document.getElementById('medIdToGrant').value.trim();
        await api('/api/admin/users/medal-level', 'POST', {
          userId,
          level: level === '' ? null : Number(level),
          medalId
        });
        showToast('Updated Level/Medal for ' + userId + '!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    async function saveCustomMedal() {
      try {
        const s1 = Number(document.getElementById('newMedalStar1')?.value || 1000000);
        const s2 = Number(document.getElementById('newMedalStar2')?.value || s1 * 5);
        const s3 = Number(document.getElementById('newMedalStar3')?.value || s2 * 2);
        const s4 = Number(document.getElementById('newMedalStar4')?.value || s3 * 5);
        const body = {
          id: document.getElementById('newMedalId').value.trim(),
          name: document.getElementById('newMedalName').value.trim(),
          category: document.getElementById('newMedalCat').value,
          linkType: document.getElementById('newMedalLinkType')?.value || 'SPECIFIC_GIFT',
          linkedGiftId: document.getElementById('newMedalGiftId')?.value.trim() || '',
          linkedGiftName: document.getElementById('newMedalGiftName')?.value.trim() || '',
          starThresholds: [s1, s2, s3, s4],
          starCount: 4,
          description: document.getElementById('newMedalDesc').value.trim(),
          iconUrl: document.getElementById('newMedalIconUrl').value.trim(),
          active: true
        };
        await api('/api/admin/medals', 'POST', body);
        showToast('Medal "' + body.name + '" saved & synced to Firestore!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    async function deleteCustomMedal(id) {
      try {
        await api('/api/admin/medals/' + encodeURIComponent(id), 'DELETE');
        showToast('Medal ' + id + ' deleted!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    async function sendBroadcast() {
      try {
        const body = {
          targetUserId: document.getElementById('bcTargetUserId').value.trim(),
          title: document.getElementById('bcTitle').value.trim(),
          content: document.getElementById('bcContent').value.trim(),
          channel: document.getElementById('bcChannel').value
        };
        await api('/api/admin/broadcast', 'POST', body);
        showToast('Live broadcast sent!');
      } catch (e) { showToast(e.message, true); }
    }

    async function addEventBanner() {
      try {
        const body = {
          title: document.getElementById('evTitle').value.trim(),
          subtitle: document.getElementById('evSubtitle').value.trim(),
          bannerUrl: document.getElementById('evBannerUrl').value.trim()
        };
        await api('/api/admin/events', 'POST', body);
        showToast('Event / Banner published!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    async function deleteEvent(id) {
      try {
        await api('/api/admin/events/' + encodeURIComponent(id), 'DELETE');
        showToast('Event deleted!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    async function saveGlobalRules() {
      try {
        const body = {
          maxUnfollowedDmMessages: Number(document.getElementById('ruleDmLimit').value || 5),
          maxAccountsPerDevice: Number(document.getElementById('ruleMaxAccounts').value || 3),
          inviteDay1Coins: Number(document.getElementById('ruleInvDay1').value || 50000),
          inviteDay3Coins: Number(document.getElementById('ruleInvDay3').value || 150000),
          inviteFriendWelcomeCoins: Number(document.getElementById('ruleInvWelcome').value || 250000),
          dailyCheckInCoins: Number(document.getElementById('ruleCheckInCoins').value || 10)
        };
        await api('/api/admin/rules', 'POST', body);
        showToast('Global Rules saved & pushed live!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    async function toggleOfficialRoom(roomId, isOfficial) {
      try {
        await api('/api/admin/rooms/official', 'POST', { roomId, isOfficial });
        showToast('Room ' + roomId + ' official status updated!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    async function closeRoom(roomId) {
      try {
        await api('/api/admin/rooms/close', 'POST', { roomId });
        showToast('Room ' + roomId + ' closed!');
        loadAll();
      } catch (e) { showToast(e.message, true); }
    }

    loadAll();
  </script>
</body>
</html>`;

// Standalone Web Admin Dashboard (/admin and /admin.html)
app.get(['/admin', '/admin.html'], (req, res) => {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  res.status(200).send(EMBEDDED_ADMIN_HTML);
});

function requireAdminAuth(req, res, next) {
  const key = String(
    req.headers['x-admin-key'] ||
      req.headers['x-admin-secret'] ||
      req.query.key ||
      req.query.secret ||
      ''
  ).trim();
  if (key !== ADMIN_SECRET) {
    return res.status(401).json({ error: 'Invalid Admin Secret Key!' });
  }
  next();
}

app.get('/api/admin/overview', requireAdminAuth, (req, res) => {
  const userList = Array.from(users.values()).map((u) => {
    const uid = String(u.userId || '');
    const devKeys = Array.from(userKnownDevices.get(uid) || []);
    const isDevBanned = devKeys.some((k) => bannedDeviceKeys.has(k));
    return {
      userId: uid,
      nickname: u.nickname || `User ${uid}`,
      gender: u.gender || 'Boy',
      level: Number(userLevels.get(uid)?.level ?? u.level ?? 0),
      totalCharmPoints: Number(u.totalCharmPoints || 0),
      coins: getOrInitGreedyBalance(uid),
      diamonds: Number(userDiamonds.get(uid) || 0),
      isOnline: Boolean(userSockets.get(uid)?.size),
      isBanned: bannedUserIds.has(uid) || isDevBanned,
      deviceKeys: devKeys,
    };
  });
  let onlineCount = 0;
  for (const s of userSockets.values()) {
    if (s && s.size > 0) onlineCount += 1;
  }
  res.json({
    stats: {
      totalUsers: users.size,
      onlineUsers: onlineCount,
      liveRoomsCount: getLiveRoomsSummary().length,
      bannedUsersCount: bannedUserIds.size,
      bannedDevicesCount: bannedDeviceKeys.size,
    },
    users: userList,
    gifts: Array.from(DYNAMIC_GIFTS_CATALOG.values()),
    storeCatalog: Object.values(STORE_CATALOG),
    customMedals: Array.from(DYNAMIC_MEDALS_CATALOG.values()),
    events: Array.from(DYNAMIC_EVENTS_CATALOG.values()),
    rechargePacks: WALLET_RECHARGE_PACKS,
    rules: {
      maxUnfollowedDmMessages: MAX_UNFOLLOWED_DM_MESSAGES,
      maxAccountsPerDevice: MAX_ACCOUNTS_PER_DEVICE,
      inviteDay1Coins: INVITE_DAY1_COINS,
      inviteDay3Coins: INVITE_DAY3_COINS,
      inviteFriendWelcomeCoins: INVITE_FRIEND_WELCOME_COINS,
      dailyCheckInCoins: DAILY_CHECKIN_REWARD_COINS,
    },
    rooms: getLiveRoomsSummary(),
    reports: reports.slice(-50).reverse(),
    feedbacks: feedbacks.slice(-50).reverse(),
  });
});

app.post('/api/admin/users/ban', requireAdminAuth, (req, res) => {
  const userId = String(req.body.userId || '').trim();
  const ban = Boolean(req.body.ban);
  const reason = String(req.body.reason || 'Banned by Admin').trim();
  if (!userId) return res.status(400).json({ error: 'userId is required' });
  if (ban) {
    bannedUserIds.set(userId, { reason, bannedAtMs: Date.now() });
    kickAndNotifyBannedUser(userId, reason);
  } else {
    bannedUserIds.delete(userId);
    sendToUser(userId, { type: 'ACCOUNT_UNBANNED', userId });
  }
  firestoreStore.markGlobalDirty();
  res.json({ ok: true, userId, banned: ban });
});

app.post('/api/admin/devices/ban', requireAdminAuth, (req, res) => {
  const target = String(req.body.target || '').trim();
  const ban = Boolean(req.body.ban);
  const reason = String(req.body.reason || 'Hardware device banned by Admin').trim();
  if (!target) return res.status(400).json({ error: 'User ID or Device Key is required' });
  const keysToAffect = new Set();
  if (target.startsWith('fp:') || target.startsWith('drm:')) {
    keysToAffect.add(target);
  } else {
    for (const k of userKnownDevices.get(target) || []) keysToAffect.add(k);
    deviceAccountSlots.forEach((slotMap, devKey) => {
      if (slotMap.has(target)) keysToAffect.add(devKey);
    });
  }
  if (keysToAffect.size === 0) {
    keysToAffect.add(`fp:${target}`);
  }
  keysToAffect.forEach((k) => {
    if (ban) {
      bannedDeviceKeys.set(k, { reason, bannedAtMs: Date.now() });
      for (const uid of deviceUserAccounts.get(k) || []) {
        kickAndNotifyBannedUser(uid, reason);
      }
    } else {
      bannedDeviceKeys.delete(k);
    }
    firestoreStore.markDeviceDirty(k);
  });
  res.json({ ok: true, affectedKeys: Array.from(keysToAffect), banned: ban });
});

app.post('/api/admin/devices/reset-slots', requireAdminAuth, (req, res) => {
  const target = String(req.body.target || '').trim();
  if (!target) {
    const allKeys = Array.from(deviceAccountSlots.keys());
    deviceAccountSlots.clear();
    saveDeviceAccountLedgerToDisk(allKeys);
    return res.json({ ok: true, cleared: 'ALL' });
  }
  const keys = new Set();
  if (target.startsWith('fp:') || target.startsWith('drm:')) {
    keys.add(target);
  } else {
    for (const k of userKnownDevices.get(target) || []) keys.add(k);
    deviceAccountSlots.forEach((slotMap, devKey) => {
      if (slotMap.has(target)) keys.add(devKey);
    });
  }
  keys.forEach((k) => deviceAccountSlots.delete(k));
  saveDeviceAccountLedgerToDisk(Array.from(keys));
  res.json({ ok: true, clearedKeys: Array.from(keys) });
});

app.post('/api/admin/wallet/adjust', requireAdminAuth, (req, res) => {
  const userId = String(req.body.userId || '').trim();
  const coinsDelta = Math.trunc(Number(req.body.coinsDelta || 0));
  const diamondsDelta = Math.trunc(Number(req.body.diamondsDelta || 0));
  const title = String(req.body.title || 'Admin Wallet Adjustment').trim();
  if (!userId) return res.status(400).json({ error: 'userId is required' });

  const curCoins = getOrInitGreedyBalance(userId);
  const nextCoins = Math.max(0, Math.min(GREEDY_MAX_BALANCE, curCoins + coinsDelta));
  greedyUserBalances.set(userId, nextCoins);
  if (coinsDelta !== 0) {
    recordWalletTx(
      userId,
      coinsDelta > 0 ? 'RECHARGE' : 'COIN_USAGE',
      title,
      `${coinsDelta > 0 ? '+' : ''}${coinsDelta.toLocaleString('en-US')} Coins`,
      coinsDelta > 0
    );
  }

  const curDiamonds = Number(userDiamonds.get(userId) || 0);
  const nextDiamonds = Math.max(0, curDiamonds + diamondsDelta);
  userDiamonds.set(userId, nextDiamonds);
  if (diamondsDelta !== 0) {
    recordWalletTx(
      userId,
      'DIAMOND_BILLS',
      title,
      `${diamondsDelta > 0 ? '+' : ''}${diamondsDelta.toLocaleString('en-US')} Diamonds`,
      diamondsDelta > 0
    );
  }

  pushWalletSyncToUser(userId);
  sendToUser(userId, { type: 'WALLET_BALANCE_SYNC', serverBalance: nextCoins });
  pushSystemNotificationToUser(
    userId,
    'Wallet Updated by Official Admin',
    `${title}: Coins=${nextCoins.toLocaleString('en-US')}, Diamonds=${nextDiamonds.toLocaleString('en-US')}`
  );
  firestoreStore.markUserDirty(userId);
  res.json({ ok: true, userId, coins: nextCoins, diamonds: nextDiamonds });
});

app.post('/api/admin/wallet/packs', requireAdminAuth, (req, res) => {
  const packId = String(req.body.packId || '').trim();
  const coins = Math.max(100, Math.floor(Number(req.body.coins || 0)));
  const priceInr = Math.max(1, Math.floor(Number(req.body.priceInr || 0)));
  if (!packId) return res.status(400).json({ error: 'packId is required' });
  WALLET_RECHARGE_PACKS[packId] = { coins, priceInr };
  broadcastDynamicCatalog();
  firestoreStore.markGlobalDirty();
  res.json({ ok: true, rechargePacks: WALLET_RECHARGE_PACKS });
});

app.post('/api/admin/cloudinary/sign', requireAdminAuth, (req, res) => {
  try {
    const kind = String(req.body?.kind || req.query?.kind || 'video').toLowerCase() === 'icon' ? 'icon' : 'video';
    const signed = signCloudinaryUploadLocal({ kind });
    res.json({ ok: true, ...signed });
  } catch (err) {
    console.error('[AdminCloudinarySign]', err);
    res.status(500).json({ error: err.message || 'Cloudinary sign failed' });
  }
});

app.put(
  '/api/admin/storage/upload',
  requireAdminAuth,
  express.raw({ type: ['video/mp4', 'image/*', 'application/octet-stream'], limit: '65mb' }),
  async (req, res) => {
    try {
      const kind = String(req.query.kind || 'video').toLowerCase() === 'icon' ? 'icon' : 'video';
      const buffer = req.body;
      if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
        return res.status(400).json({ error: 'Empty file upload body' });
      }
      const isVideo = kind === 'video';
      const contentType = isVideo ? 'video/mp4' : (req.headers['content-type'] || 'image/png');
      const uploaded = await uploadMediaToCloudinaryLocal({
        buffer,
        kind,
        contentType,
      });
      res.json({ ok: true, ...uploaded });
    } catch (err) {
      console.error('[AdminCloudinaryUpload]', err);
      res.status(500).json({ error: err.message || 'Cloudinary upload failed' });
    }
  }
);

app.post('/api/admin/gifts/upload-save', requireAdminAuth, async (req, res) => {
  try {
    const id = String(req.body.id || `g_mp4_${Date.now()}`).trim();
    const name = String(req.body.name || '').trim();
    const price = Math.max(1, Math.floor(Number(req.body.price || req.body.priceCoins || 10)));
    const videoUrl = String(req.body.videoUrl || '').trim();
    const iconUrl = String(req.body.iconUrl || '').trim();
    const type = String(req.body.type || 'mp4').trim() || 'mp4';
    const category = String(req.body.category || 'Gifts').trim() || 'Gifts';
    const tag = String(req.body.tag || 'MP4').trim() || 'MP4';
    const durationSeconds = Math.max(1, Number(req.body.durationSeconds || 5.5));
    if (!name) return res.status(400).json({ error: 'Gift name is required' });
    if (!videoUrl || !iconUrl) return res.status(400).json({ error: 'Both videoUrl and iconUrl are required' });

    let savedDoc = null;
    let firestoreWarning = '';
    try {
      savedDoc = await firestoreStore.saveGiftToFirestoreCollection({
        giftId: id,
        name,
        price,
        videoUrl,
        iconUrl,
        category,
        tag,
        type,
        durationSeconds,
        active: true,
      });
    } catch (fsErr) {
      if (!firestoreStore.getStatus().firestoreConnected) {
        firestoreWarning = fsErr.message || 'Firestore standby';
        console.warn('[AdminSaveMp4Gift] Firestore standby, saved to catalog + snapshot:', firestoreWarning);
      } else {
        throw fsErr;
      }
    }

    const gift = {
      id,
      name,
      icon: iconUrl,
      priceCoins: price,
      tag,
      category,
      animationUrl: videoUrl,
      durationSeconds,
      type,
      active: true,
    };
    DYNAMIC_GIFTS_CATALOG.set(id, gift);
    giftsCustomizedByAdmin = true;
    broadcastDynamicCatalog();
    firestoreStore.markGlobalDirty();
    firestoreStore.flushDirtyState().catch(() => {});
    res.json({ ok: true, gift, firestoreDoc: savedDoc, warning: firestoreWarning || undefined });
  } catch (err) {
    console.error('[AdminSaveMp4Gift]', err);
    res.status(500).json({ error: err.message || 'Failed to save gift to database' });
  }
});

app.post('/api/admin/gifts', requireAdminAuth, async (req, res) => {
  try {
    const id = String(req.body.id || `g_${Date.now()}`).trim();
    const name = String(req.body.name || '').trim();
    const priceCoins = Math.max(1, Math.floor(Number(req.body.priceCoins || 10)));
    const category = String(req.body.category || 'Popular').trim();
    const tag = String(req.body.tag || 'HOT').trim();
    const icon = String(req.body.icon || '🎁').trim();
    const animationUrl = String(req.body.animationUrl || '').trim();
    const durationSeconds = Math.max(1, Number(req.body.durationSeconds || 3.0));
    const active = req.body.active !== false;
    const type = String(req.body.type || (animationUrl.toLowerCase().includes('.mp4') ? 'mp4' : 'standard')).trim();
    if (!name) return res.status(400).json({ error: 'Gift name is required' });

    let savedDoc = null;
    let firestoreWarning = '';
    try {
      savedDoc = await firestoreStore.saveGiftToFirestoreCollection({
        giftId: id,
        name,
        price: priceCoins,
        videoUrl: animationUrl,
        iconUrl: icon,
        category,
        tag,
        type,
        durationSeconds,
        active,
      });
    } catch (fsErr) {
      if (!firestoreStore.getStatus().firestoreConnected) {
        firestoreWarning = fsErr.message || 'Firestore standby';
        console.warn('[AdminSaveGift] Firestore standby, saved to catalog + snapshot:', firestoreWarning);
      } else {
        console.warn('[AdminSaveGift] Firestore collection save warning:', fsErr.message);
        firestoreWarning = fsErr.message || 'Firestore collection warning';
      }
    }

    const gift = { id, name, icon, priceCoins, tag, category, animationUrl, durationSeconds, type, active };
    DYNAMIC_GIFTS_CATALOG.set(id, gift);
    giftsCustomizedByAdmin = true;
    broadcastDynamicCatalog();
    firestoreStore.markGlobalDirty();
    firestoreStore.flushDirtyState().catch(() => {});
    res.json({ ok: true, gift, firestoreDoc: savedDoc, warning: firestoreWarning || undefined });
  } catch (err) {
    console.error('[AdminSaveGift]', err);
    res.status(500).json({ error: err.message || 'Failed to save gift' });
  }
});

app.delete('/api/admin/gifts/:id', requireAdminAuth, async (req, res) => {
  const id = String(req.params.id || '').trim();
  DYNAMIC_GIFTS_CATALOG.delete(id);
  giftsCustomizedByAdmin = true;
  broadcastDynamicCatalog();
  firestoreStore.markGlobalDirty();
  await firestoreStore.deleteGiftFromFirestoreCollection(id);
  firestoreStore.flushDirtyState().catch(() => {});
  res.json({ ok: true, deletedId: id });
});

app.post('/api/admin/store/catalog', requireAdminAuth, (req, res) => {
  const id = String(req.body.id || `st_${Date.now()}`).trim();
  const name = String(req.body.name || '').trim();
  const prev = STORE_CATALOG[id] || {};
  const subtitle = String(req.body.subtitle || prev.subtitle || 'Exclusive Store Item').trim();
  const category = String(req.body.category || prev.category || 'FRAME').trim().toUpperCase();
  const basePrice7Days = Math.max(100, Math.floor(Number(req.body.basePrice7Days || prev.basePrice7Days || 10000)));
  const rarityStars = Math.max(1, Math.min(3, Math.floor(Number(req.body.rarityStars || prev.rarityStars || 2))));
  const hasAnimationPlay = req.body.hasAnimationPlay !== undefined
    ? Boolean(req.body.hasAnimationPlay)
    : (category === 'VEHICLE' || category === 'MIC_WAVE' || Boolean(prev.hasAnimationPlay));
  const iconUrl = req.body.iconUrl !== undefined ? String(req.body.iconUrl || '').trim() : (prev.iconUrl || '');
  const animationUrl = req.body.animationUrl !== undefined ? String(req.body.animationUrl || '').trim() : (prev.animationUrl || '');
  const specialIdNumber = req.body.specialIdNumber !== undefined ? String(req.body.specialIdNumber || '').trim() : (prev.specialIdNumber || '');
  const sampleBubbleText = req.body.sampleBubbleText !== undefined ? String(req.body.sampleBubbleText || '').trim() : (prev.sampleBubbleText || 'Welcome to Just Fun Voice Party! 🎤');
  const active = req.body.active !== false;
  if (!name) return res.status(400).json({ error: 'Item name is required' });
  STORE_CATALOG[id] = {
    ...prev,
    id,
    name,
    subtitle,
    category,
    basePrice7Days,
    rarityStars,
    hasAnimationPlay,
    iconUrl,
    animationUrl,
    specialIdNumber,
    sampleBubbleText,
    active,
  };
  storeCustomizedByAdmin = true;
  broadcastDynamicCatalog();
  firestoreStore.markGlobalDirty();
  res.json({ ok: true, item: STORE_CATALOG[id] });
});

app.delete('/api/admin/store/catalog/:id', requireAdminAuth, (req, res) => {
  const id = String(req.params.id || '').trim();
  delete STORE_CATALOG[id];
  storeCustomizedByAdmin = true;
  broadcastDynamicCatalog();
  firestoreStore.markGlobalDirty();
  res.json({ ok: true, deletedId: id });
});

app.post('/api/admin/store/grant', requireAdminAuth, (req, res) => {
  const userId = String(req.body.userId || '').trim();
  const itemId = String(req.body.itemId || '').trim();
  const days = Math.max(1, Math.floor(Number(req.body.days || 7)));
  const spec = STORE_CATALOG[itemId];
  if (!userId || !spec) return res.status(400).json({ error: 'Valid userId and itemId required' });
  const nowMs = Date.now();
  const inv = pruneAndSyncUserStore(userId, nowMs);
  const existing = inv.items.get(itemId);
  const baseExpiry = existing && Number(existing.expiresAtMs || 0) > nowMs ? Number(existing.expiresAtMs) : nowMs;
  inv.items.set(itemId, {
    itemId,
    expiresAtMs: baseExpiry + days * STORE_DAY_MS,
    totalDaysPurchased: (existing ? Number(existing.totalDaysPurchased || 0) : 0) + days,
    giftedByName: 'Official Admin',
    purchasedAtMs: nowMs,
  });
  inv.equipped[spec.category] = itemId;
  pruneAndSyncUserStore(userId, nowMs);
  sendToUser(userId, buildStoreStatePayload(userId));
  pushSystemNotificationToUser(
    userId,
    '🎁 VIP Store Item Granted!',
    `Official Admin granted you "${spec.name}" (${days} Days) and equipped it automatically!`
  );
  firestoreStore.markUserDirty(userId);
  res.json({ ok: true, userId, itemId, days });
});

app.post('/api/admin/users/medal-level', requireAdminAuth, (req, res) => {
  const userId = String(req.body.userId || '').trim();
  if (!userId) return res.status(400).json({ error: 'userId is required' });
  const prof = users.get(userId) || { userId, user8DigitId: userId, nickname: `User ${userId}` };
  if (req.body.level !== null && req.body.level !== undefined && req.body.level !== '') {
    const newLv = Math.max(0, Math.min(50, Math.floor(Number(req.body.level))));
    const st = getOrCreateUserLevelState(userId);
    st.level = newLv;
    st.exp = LEVEL_CUMULATIVE_EXP[newLv] ?? 0;
    userLevels.set(userId, st);
    prof.level = newLv;
    broadcastUserLevelToRooms(userId, newLv);
    sendToUser(userId, buildLevelStatePayload(userId));
  }
  const medalId = String(req.body.medalId || '').trim();
  if (medalId) {
    const medals = new Set(Array.isArray(prof.unlockedMedalIds) ? prof.unlockedMedalIds : []);
    medals.add(medalId);
    prof.unlockedMedalIds = Array.from(medals);
  }
  users.set(userId, prof);
  sendToUser(userId, { type: 'USER_REGISTERED', profile: prof });
  pushSystemNotificationToUser(userId, '🏅 Profile Upgraded by Admin!', `Your Level/Medals were updated by Official Admin.`);
  firestoreStore.markUserDirty(userId);
  res.json({ ok: true, profile: prof });
});

const profileAndMedals = profileAndMedalsModule
  ? profileAndMedalsModule.initProfileAndMedalsModule({
      app,
      users,
      rooms,
      userFollowers,
      userFollowing,
      userVisitors,
      blockedUsers,
      DYNAMIC_GIFTS_CATALOG,
      DYNAMIC_MEDALS_CATALOG,
      firestoreStore,
      sendToUser,
      pushSocialSyncToUser,
      pushSystemNotificationToUser,
      broadcastDynamicCatalog,
      requireAdminAuth,
      buildUserSummary,
    })
  : null;

app.delete('/api/admin/medals/:id', requireAdminAuth, (req, res) => {
  const id = String(req.params.id || '').trim();
  DYNAMIC_MEDALS_CATALOG.delete(id);
  broadcastDynamicCatalog();
  firestoreStore.markGlobalDirty();
  res.json({ ok: true, deletedId: id });
});

app.post('/api/admin/broadcast', requireAdminAuth, (req, res) => {
  const targetUserId = String(req.body.targetUserId || '').trim();
  const title = String(req.body.title || 'Official Notice').trim();
  const content = String(req.body.content || '').trim();
  const channel = String(req.body.channel || 'NOTIFICATION').trim();
  if (!content) return res.status(400).json({ error: 'Content is required' });

  const targets = targetUserId ? [targetUserId] : Array.from(users.keys());
  targets.forEach((uid) => {
    if (channel === 'NOTIFICATION') {
      pushSystemNotificationToUser(uid, title, content, 'Go to check');
    }
    const threadKey = getThreadKey('10000', uid);
    const list = dmThreads.get(threadKey) || [];
    const dmItem = {
      id: `dm_admin_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
      fromUserId: '10000',
      fromName: 'Customer service',
      fromAvatarUri: '',
      toUserId: uid,
      toName: users.get(uid)?.nickname || `User ${uid}`,
      toAvatarUri: '',
      text: `${title}: ${content}`,
      msgType: 'TEXT',
      voiceDurationSec: 0,
      audioBase64: '',
      imageBase64: '',
      giftName: '',
      giftIcon: '',
      giftCount: 1,
      createdAtMs: Date.now(),
      isRecalled: false,
      isRead: false,
      deletedBy: [],
    };
    list.push(dmItem);
    dmThreads.set(threadKey, list);
    dmUnreadCounts.set(`${uid}:10000`, (dmUnreadCounts.get(`${uid}:10000`) || 0) + 1);
    sendToUser(uid, { type: 'NEW_DM', peerUserId: '10000', message: dmItem });
    pushSocialSyncToUser(uid);
    firestoreStore.markDmDirty(threadKey);
    firestoreStore.markUserDirty(uid);
  });
  res.json({ ok: true, sentCount: targets.length });
});

app.post('/api/admin/events', requireAdminAuth, (req, res) => {
  const id = `ev_${Date.now()}`;
  const title = String(req.body.title || '').trim();
  const subtitle = String(req.body.subtitle || '').trim();
  const bannerUrl = String(req.body.bannerUrl || '').trim();
  if (!title) return res.status(400).json({ error: 'Event title is required' });
  const ev = { id, title, subtitle, bannerUrl, createdAtMs: Date.now() };
  DYNAMIC_EVENTS_CATALOG.set(id, ev);
  broadcastDynamicCatalog();
  firestoreStore.markGlobalDirty();
  res.json({ ok: true, event: ev });
});

app.delete('/api/admin/events/:id', requireAdminAuth, (req, res) => {
  const id = String(req.params.id || '').trim();
  DYNAMIC_EVENTS_CATALOG.delete(id);
  broadcastDynamicCatalog();
  firestoreStore.markGlobalDirty();
  res.json({ ok: true, deletedId: id });
});

app.post('/api/admin/rules', requireAdminAuth, (req, res) => {
  if (req.body.maxUnfollowedDmMessages !== undefined) {
    MAX_UNFOLLOWED_DM_MESSAGES = Math.max(1, Math.floor(Number(req.body.maxUnfollowedDmMessages)));
  }
  if (req.body.maxAccountsPerDevice !== undefined) {
    MAX_ACCOUNTS_PER_DEVICE = Math.max(1, Math.floor(Number(req.body.maxAccountsPerDevice)));
  }
  if (req.body.inviteDay1Coins !== undefined) {
    INVITE_DAY1_COINS = Math.max(0, Math.floor(Number(req.body.inviteDay1Coins)));
  }
  if (req.body.inviteDay3Coins !== undefined) {
    INVITE_DAY3_COINS = Math.max(0, Math.floor(Number(req.body.inviteDay3Coins)));
  }
  if (req.body.inviteFriendWelcomeCoins !== undefined) {
    INVITE_FRIEND_WELCOME_COINS = Math.max(0, Math.floor(Number(req.body.inviteFriendWelcomeCoins)));
  }
  if (req.body.dailyCheckInCoins !== undefined) {
    DAILY_CHECKIN_REWARD_COINS = Math.max(0, Math.floor(Number(req.body.dailyCheckInCoins)));
  }
  broadcastDynamicCatalog();
  firestoreStore.markGlobalDirty();
  res.json({ ok: true, rules: buildDynamicCatalogPayload().rules });
});

app.post('/api/admin/rooms/official', requireAdminAuth, (req, res) => {
  const roomId = String(req.body.roomId || '').trim();
  const isOfficial = Boolean(req.body.isOfficial);
  const room = rooms.get(roomId);
  if (!room) return res.status(404).json({ error: 'Room not found' });
  room.isOfficial = isOfficial;
  firestoreStore.markRoomDirty(roomId);
  broadcastLiveRoomsToHome();
  res.json({ ok: true, roomId, isOfficial });
});

app.post('/api/admin/rooms/close', requireAdminAuth, (req, res) => {
  const roomId = String(req.body.roomId || '').trim();
  const room = rooms.get(roomId);
  if (!room) return res.status(404).json({ error: 'Room not found' });
  broadcastToRoom(room, { type: 'ROOM_CLOSED_BY_ADMIN', roomId, reason: 'Closed by Official Admin' });
  rooms.delete(roomId);
  broadcastLiveRoomsToHome();
  res.json({ ok: true, closedRoomId: roomId });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, maxPayload: 4 * 1024 * 1024, perMessageDeflate: false });

// Server-side WebSocket heartbeat to detect dead connections cleanly without kicking slow 4G/5G phones
const pingInterval = setInterval(() => {
  for (const ws of wss.clients) {
    const meta = clientMeta.get(ws);
    if (!meta) continue;
    meta.missedPings = (meta.missedPings || 0) + 1;
    if (meta.missedPings >= 3) {
      ws.terminate();
      continue;
    }
    try {
      ws.ping();
    } catch (_) {}
  }
}, 15000);

wss.on('close', () => {
  clearInterval(pingInterval);
});

wss.on('connection', (ws, req) => {
  const forwarded = String(req?.headers?.['x-forwarded-for'] || '').split(',')[0].trim();
  const clientIp = forwarded || String(req?.socket?.remoteAddress || '').replace('::ffff:', '').trim();
  const meta = { userId: '', roomId: '', missedPings: 0, clientIp };
  clientMeta.set(ws, meta);
  homeSubscribers.add(ws);

  // Immediately push live rooms list + authoritative dynamic catalogs on connect
  safeSend(ws, { type: 'LIVE_ROOMS_UPDATE', serverNowMs: Date.now(), rooms: getLiveRoomsSummary() });
  safeSend(ws, buildDynamicCatalogPayload());

  ws.on('pong', () => {
    meta.missedPings = 0;
  });

  ws.on('message', (rawBuffer) => {
    meta.missedPings = 0;
    let msg;
    try {
      msg = JSON.parse(rawBuffer.toString());
    } catch (_) {
      return;
    }
    if (!msg || typeof msg.type !== 'string') return;

    switch (msg.type) {
      case 'PING': {
        safeSend(ws, {
          type: 'PONG',
          ts: Date.now(),
          clientSentElapsedMs: Number(msg.clientSentElapsedMs || 0),
        });
        break;
      }

      case 'SYNC_DEVICE_ACCOUNT_SLOTS': {
        const integrity = msg.deviceIntegrity;
        const keys = getDeviceKeysFromIntegrity(integrity);
        if (keys.length > 0 && Array.isArray(msg.localAccounts)) {
          let changed = false;
          msg.localAccounts.slice(0, MAX_ACCOUNTS_PER_DEVICE).forEach((acc) => {
            if (!acc || typeof acc !== 'object') return;
            const id8 = String(acc.user8DigitId || acc.userId || '').trim();
            if (id8.length !== 8) return;
            const currentList = getRegisteredAccountsForDeviceKeys(keys);
            const alreadyPresent = currentList.some((item) => item.user8DigitId === id8);
            if (alreadyPresent || currentList.length < MAX_ACCOUNTS_PER_DEVICE) {
              upsertAccountSlotAcrossDeviceKeys(keys, {
                user8DigitId: id8,
                accountUid: String(acc.accountUid || id8),
                nickname: String(acc.nickname || ''),
                avatarUri: String(acc.avatarUri || ''),
                gender: String(acc.gender || 'Boy'),
                bio: String(acc.bio || 'Welcome to JustFun'),
                country: String(acc.country || 'India'),
                level: Number(acc.level || 0),
                profileCompleted: Boolean(acc.profileCompleted ?? true),
                createdAtMs: Number(acc.createdAtMs || Date.now()),
              });
              changed = true;
            }
          });
          if (changed) {
            saveDeviceAccountLedgerToDisk(keys);
          }
        }
        const authoritativeAccounts = getRegisteredAccountsForDeviceKeys(keys);
        safeSend(ws, {
          type: 'DEVICE_ACCOUNT_SLOTS_STATE',
          maxAccounts: MAX_ACCOUNTS_PER_DEVICE,
          usedAccounts: authoritativeAccounts.length,
          accounts: authoritativeAccounts,
        });
        break;
      }

      case 'REGISTER_USER': {
        const profile = msg.profile;
        if (!profile || !profile.userId) break;
        const uid = String(profile.userId);
        const accountUid = String(profile.accountUid || uid);

        const banCheck = isUserOrDeviceBanned(uid, msg.deviceIntegrity || null);
        if (banCheck.banned) {
          safeSend(ws, {
            type: 'ACCOUNT_BANNED',
            userId: uid,
            reason: banCheck.reason,
            bannedAtMs: Date.now(),
          });
          break;
        }

        if (msg.deviceIntegrity) {
          const slotCheck = evaluateServerDeviceAccountSlot(
            uid,
            accountUid,
            profile,
            msg.deviceIntegrity
          );
          if (!slotCheck.allowed) {
            safeSend(ws, {
              type: 'DEVICE_ACCOUNT_LIMIT_BLOCKED',
              maxAccounts: MAX_ACCOUNTS_PER_DEVICE,
              usedAccounts: Array.isArray(slotCheck.allowedAccounts) ? slotCheck.allowedAccounts.length : MAX_ACCOUNTS_PER_DEVICE,
              allowedAccounts: slotCheck.allowedAccounts || [],
              error: slotCheck.error,
            });
            break;
          }
          safeSend(ws, {
            type: 'DEVICE_ACCOUNT_SLOTS_STATE',
            maxAccounts: MAX_ACCOUNTS_PER_DEVICE,
            usedAccounts: Array.isArray(slotCheck.accounts) ? slotCheck.accounts.length : 0,
            accounts: slotCheck.accounts || [],
          });
        }

        meta.userId = uid;
        addUserSocket(uid, ws);
        safeSend(ws, buildDynamicCatalogPayload());

        const existing = users.get(uid) || {};
        const srvLevelState = userLevels.get(uid);
        const mergedUnlockedMedals = Array.from(
          new Set([
            ...(Array.isArray(existing.unlockedMedalIds) ? existing.unlockedMedalIds : []),
            ...(Array.isArray(profile.unlockedMedalIds) ? profile.unlockedMedalIds : []),
          ])
        );
        const mergedEquippedMedals = Array.isArray(profile.equippedMedalIds) && profile.equippedMedalIds.length > 0
          ? profile.equippedMedalIds.slice(0, 10)
          : (Array.isArray(existing.equippedMedalIds) ? existing.equippedMedalIds.slice(0, 10) : []);
        const mergedPhotos = Array.isArray(profile.photos) && profile.photos.length > 0
          ? profile.photos.slice(0, 4)
          : (Array.isArray(existing.photos) ? existing.photos.slice(0, 4) : []);
        const merged = {
          ...existing,
          ...profile,
          userId: uid,
          user8DigitId: uid,
          accountUid,
          bgUrl: profile.bgUrl !== undefined && profile.bgUrl !== '' ? profile.bgUrl : (existing.bgUrl || ''),
          photos: mergedPhotos,
          unlockedMedalIds: mergedUnlockedMedals,
          equippedMedalIds: mergedEquippedMedals,
          medalProgress: existing.medalProgress || profile.medalProgress || {},
          receivedGifts: Array.isArray(existing.receivedGifts) && existing.receivedGifts.length > 0
            ? existing.receivedGifts
            : (Array.isArray(profile.receivedGifts) ? profile.receivedGifts : []),
          level: srvLevelState ? srvLevelState.level : Number(profile.level ?? existing.level ?? 0),
          // Keep server-authoritative charm points if higher than client cache
          totalCharmPoints: Math.max(Number(existing.totalCharmPoints || 0), Number(profile.totalCharmPoints || 0)),
          updatedAt: Date.now(),
        };
        users.set(uid, merged);
        pruneAndSyncUserStore(uid);
        if (msg.deviceIntegrity) {
          recordUserDeviceIntegrity(uid, msg.deviceIntegrity, meta.clientIp);
          saveInviteSecurityLedgerToDisk([uid], getDeviceKeysFromIntegrity(msg.deviceIntegrity));
        }
        firestoreStore.markUserDirty(uid);
        evaluateInviteDay3Retention(uid, msg.deviceIntegrity || null);
        safeSend(ws, { type: 'USER_REGISTERED', profile: users.get(uid) || merged });
        safeSend(ws, {
          type: 'MEDAL_PROGRESS_SYNC',
          userId: uid,
          medalProgress: merged.medalProgress || {},
          unlockedMedalIds: merged.unlockedMedalIds || [],
          equippedMedalIds: merged.equippedMedalIds || [],
        });
        safeSend(ws, buildSocialSnapshotForUser(uid));
        safeSend(ws, buildInviteStatePayload(uid, msg.deviceIntegrity || null));
        safeSend(ws, buildWalletStatePayload(uid, msg.deviceIntegrity || null));
        safeSend(ws, buildStoreStatePayload(uid, msg.deviceIntegrity || null));
        break;
      }

      case 'GET_USERS': {
        const viewerId = meta.userId || '';
        const list = Array.from(users.values())
          .slice(0, 50)
          .map((u) => buildUserSummary(u, viewerId))
          .filter(Boolean);
        safeSend(ws, {
          type: 'USERS_LIST',
          users: list,
        });
        break;
      }

      case 'SUBSCRIBE_HOME': {
        homeSubscribers.add(ws);
        safeSend(ws, { type: 'LIVE_ROOMS_UPDATE', serverNowMs: Date.now(), rooms: getLiveRoomsSummary() });
        if (msg.userId) {
          const uid = String(msg.userId);
          meta.userId = uid;
          addUserSocket(uid, ws);
          const ownRoom = rooms.get(uid);
          if (ownRoom) {
            safeSend(ws, {
              type: 'MY_ROOM_UPDATE',
              room: {
                id: ownRoom.id,
                title: ownRoom.title,
                announcement: ownRoom.announcement,
                hostId: ownRoom.hostId,
                hostName: ownRoom.hostName,
                hostAvatarUri: ownRoom.hostAvatarUri,
                onlineCount: ownRoom.activeClients.size,
                isLive: ownRoom.activeClients.size > 0,
                isLockedRoom: Boolean(ownRoom.roomPassword),
              },
            });
          }
          safeSend(ws, buildSocialSnapshotForUser(uid));
        }
        break;
      }

      case 'SEARCH_ALL': {
        const query = msg.query || '';
        const viewerId = msg.viewerId || meta.userId || '';
        const results = searchRoomsAndUsers(query, viewerId);
        safeSend(ws, {
          type: 'SEARCH_RESULTS',
          query,
          rooms: results.rooms,
          users: results.users,
        });
        break;
      }

      case 'GET_USER_PROFILE': {
        const viewerUserId = String(msg.viewerUserId || meta.userId || '');
        const targetUserId = String(msg.targetUserId || '');
        if (!targetUserId) break;
        recordProfileVisit(viewerUserId, targetUserId);
        firestoreStore.markUserDirty(targetUserId);
        const target = users.get(targetUserId) || {
          userId: targetUserId,
          user8DigitId: targetUserId,
          nickname: rooms.get(targetUserId)?.hostName || `User ${targetUserId}`,
          avatarUri: rooms.get(targetUserId)?.hostAvatarUri || '',
        };
        safeSend(ws, {
          type: 'USER_PROFILE_DETAIL',
          profile: buildUserSummary(target, viewerUserId),
        });
        pushSocialSyncToUser(targetUserId);
        break;
      }

      case 'FOLLOW_USER': {
        const fromUserId = String(msg.fromUserId || meta.userId || '');
        const targetUserId = String(msg.targetUserId || '');
        if (!fromUserId || !targetUserId || fromUserId === targetUserId) break;

        getOrCreateSet(userFollowing, fromUserId).add(targetUserId);
        getOrCreateSet(userFollowers, targetUserId).add(fromUserId);

        const followerProfile = users.get(fromUserId) || {};
        const fromName = followerProfile.nickname || `User ${fromUserId}`;
        const fromAvatarUri = followerProfile.avatarUri || '';
        const nowMs = Date.now();

        let alerts = userFollowAlerts.get(targetUserId);
        if (!alerts) {
          alerts = [];
          userFollowAlerts.set(targetUserId, alerts);
        }
        alerts = [
          {
            fromUserId,
            fromName,
            fromAvatarUri,
            timestampMs: nowMs,
            isRead: false,
          },
          ...alerts.filter((a) => a.fromUserId !== fromUserId),
        ].slice(0, 30);
        userFollowAlerts.set(targetUserId, alerts);

        const isNowMutual = Boolean(userFollowing.get(targetUserId)?.has(fromUserId));
        pushSystemNotificationToUser(
          targetUserId,
          isNowMutual ? 'Mutual Follow Unlocked! 🎉' : 'New Follower 👋',
          isNowMutual
            ? `${fromName} followed you back! You can now send unlimited real-time messages to each other.`
            : `${fromName} started following you! Follow back to unlock unlimited 1-on-1 chat.`,
          'Say Hi'
        );

        sendToUser(targetUserId, {
          type: 'FOLLOW_NOTIFICATION',
          fromUserId,
          fromName,
          fromAvatarUri,
          timestampMs: nowMs,
          isMutualFollow: isNowMutual,
        });
        sendToUser(fromUserId, {
          type: 'FOLLOW_STATE_UPDATED',
          peerUserId: targetUserId,
          isFollowedByMe: true,
          isFollowingMe: isNowMutual,
          isMutualFollow: isNowMutual,
        });
        sendToUser(targetUserId, {
          type: 'FOLLOW_STATE_UPDATED',
          peerUserId: fromUserId,
          isFollowedByMe: isNowMutual,
          isFollowingMe: true,
          isMutualFollow: isNowMutual,
        });

        pushSocialSyncToUser(fromUserId);
        pushSocialSyncToUser(targetUserId);
        firestoreStore.markUserDirty(fromUserId);
        firestoreStore.markUserDirty(targetUserId);
        break;
      }

      case 'UNFOLLOW_USER': {
        const fromUserId = String(msg.fromUserId || meta.userId || '');
        const targetUserId = String(msg.targetUserId || '');
        if (!fromUserId || !targetUserId) break;

        userFollowing.get(fromUserId)?.delete(targetUserId);
        userFollowers.get(targetUserId)?.delete(fromUserId);

        const peerStillFollowsFrom = Boolean(userFollowing.get(targetUserId)?.has(fromUserId));
        sendToUser(fromUserId, {
          type: 'FOLLOW_STATE_UPDATED',
          peerUserId: targetUserId,
          isFollowedByMe: false,
          isFollowingMe: peerStillFollowsFrom,
          isMutualFollow: false,
        });
        sendToUser(targetUserId, {
          type: 'FOLLOW_STATE_UPDATED',
          peerUserId: fromUserId,
          isFollowedByMe: peerStillFollowsFrom,
          isFollowingMe: false,
          isMutualFollow: false,
        });

        pushSocialSyncToUser(fromUserId);
        pushSocialSyncToUser(targetUserId);
        firestoreStore.markUserDirty(fromUserId);
        firestoreStore.markUserDirty(targetUserId);
        break;
      }

      case 'GET_SOCIAL_STATE': {
        const uid = String(msg.userId || meta.userId || '');
        if (uid) {
          addUserSocket(uid, ws);
          safeSend(ws, buildSocialSnapshotForUser(uid));
        }
        break;
      }

      case 'DM_TYPING': {
        const fromUserId = String(msg.fromUserId || meta.userId || '');
        const toUserId = String(msg.toUserId || '');
        if (!fromUserId || !toUserId || toUserId === '10000') break;
        sendToUser(toUserId, {
          type: 'DM_TYPING',
          fromUserId,
          toUserId,
          isTyping: Boolean(msg.isTyping),
          ts: Date.now(),
        });
        break;
      }

      case 'SEND_DM': {
        const fromUserId = String(msg.fromUserId || meta.userId || '');
        const toUserId = String(msg.toUserId || '');
        const text = String(msg.text || '');
        const msgType = String(msg.msgType || 'TEXT');
        const hasMedia = Boolean(msg.audioBase64 || msg.imageBase64 || msg.giftName);
        if (!fromUserId || !toUserId || (!text.trim() && !hasMedia)) break;

        const threadKey = getThreadKey(fromUserId, toUserId);
        let thread = dmThreads.get(threadKey);
        if (!thread) {
          thread = [];
          dmThreads.set(threadKey, thread);
        }

        // Enforce real block, DM privacy & 5-Message Mutual Follow rules for user-to-user chat
        if (toUserId !== '10000') {
          const receiverBlocked = blockedUsers.get(toUserId);
          const senderBlocked = blockedUsers.get(fromUserId);
          if ((receiverBlocked && receiverBlocked.has(fromUserId)) || (senderBlocked && senderBlocked.has(toUserId))) {
            safeSend(ws, {
              type: 'DM_REJECTED',
              peerUserId: toUserId,
              reason: 'BLOCKED',
              rejectedMessageId: String(msg.id || ''),
              message: 'Cannot send message: user is blocked',
            });
            break;
          }
          if (userDmPrivacy.get(toUserId) === 'FOLLOWING' && !(userFollowing.get(toUserId)?.has(fromUserId))) {
            safeSend(ws, {
              type: 'DM_REJECTED',
              peerUserId: toUserId,
              reason: 'FOLLOWING_ONLY',
              rejectedMessageId: String(msg.id || ''),
              message: 'This user only accepts messages from people they follow',
            });
            break;
          }

          // Real-Time 5-Message Limit before Mutual Follow (Follow Back Required)
          const iFollowPeer = Boolean(userFollowing.get(fromUserId)?.has(toUserId));
          const peerFollowsMe = Boolean(userFollowing.get(toUserId)?.has(fromUserId));
          const isMutualFollow = iFollowPeer && peerFollowsMe;
          const sentBySenderCount = thread.filter(
            (m) => m.fromUserId === fromUserId && !m.isRecalled
          ).length;

          if (!isMutualFollow && sentBySenderCount >= MAX_UNFOLLOWED_DM_MESSAGES) {
            safeSend(ws, {
              type: 'DM_REJECTED',
              peerUserId: toUserId,
              reason: 'FOLLOW_BACK_REQUIRED',
              rejectedMessageId: String(msg.id || ''),
              sentCount: sentBySenderCount,
              maxAllowed: MAX_UNFOLLOWED_DM_MESSAGES,
              iFollowPeer,
              peerFollowsMe,
              message: '5 message se jyada nahi jayega! Follow back ke baad hi message jayega.',
            });
            break;
          }
        }

        const fromProfile = users.get(fromUserId) || {};
        const toProfile =
          toUserId === '10000'
            ? { nickname: 'Customer service', avatarUri: '' }
            : users.get(toUserId) || {};
        const fromName = msg.fromName || fromProfile.nickname || `User ${fromUserId}`;
        const fromAvatarUri = msg.fromAvatarUri || fromProfile.avatarUri || '';
        const toName = msg.toName || toProfile.nickname || `User ${toUserId}`;
        const toAvatarUri = msg.toAvatarUri || toProfile.avatarUri || '';

        const dmItem = {
          id: msg.id || `dm_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
          fromUserId,
          fromName,
          fromAvatarUri,
          toUserId,
          toName,
          toAvatarUri,
          text,
          msgType,
          voiceDurationSec: Number(msg.voiceDurationSec || 0),
          audioBase64: msg.audioBase64 || '',
          imageBase64: msg.imageBase64 || '',
          giftName: msg.giftName || '',
          giftIcon: msg.giftIcon || '',
          giftCount: Number(msg.giftCount || 1),
          createdAtMs: Number(msg.createdAtMs || Date.now()),
          isRecalled: false,
          isRead: toUserId === '10000',
          deletedBy: [],
        };
        thread.push(dmItem);
        if (thread.length > 150) {
          thread = thread.slice(-100);
          dmThreads.set(threadKey, thread);
        }
        firestoreStore.markDmDirty(threadKey);

        if (toUserId === '10000') {
          sendToUser(fromUserId, { type: 'NEW_DM', peerUserId: '10000', message: dmItem });
          pushSocialSyncToUser(fromUserId);
          setTimeout(() => {
            sendToUser(fromUserId, {
              type: 'DM_TYPING',
              fromUserId: '10000',
              toUserId: fromUserId,
              isTyping: true,
              ts: Date.now(),
            });
          }, 250);
          setTimeout(() => {
            sendToUser(fromUserId, {
              type: 'DM_TYPING',
              fromUserId: '10000',
              toUserId: fromUserId,
              isTyping: false,
              ts: Date.now(),
            });
            const lower = text.toLowerCase();
            let csReply =
              'Hello! Just Fun Official Support here. How can we assist you with your account, coins, voice room, or friends today?';
            if (lower.includes('follow') || lower.includes('message') || lower.includes('5')) {
              csReply =
                'Chat Rule: Bina mutual follow (Follow Back) ke aap kisi user ko maximum 5 messages bhej sakte hain. Jab dono ek dusre ko follow kar lenge, tab unlimited real-time messages bhej sakte hain!';
            } else if (lower.includes('coin') || lower.includes('recharge') || lower.includes('wallet') || lower.includes('diamond')) {
              csReply =
                'Wallet Support: Coins recharge aur Diamond exchange real-time server se sync hote hain. Aap Me -> Wallet me ja kar apna balance aur transaction history dekh sakte hain.';
            } else if (lower.includes('invite') || lower.includes('code') || lower.includes('refer')) {
              csReply =
                'Invite Rewards: Apने dosto ko apna 8-digit ID code share karein! Har valid friend invite par 250,000 Coins tak milte hain.';
            } else if (lower.includes('id') || lower.includes('account') || lower.includes('login')) {
              csReply =
                'Account Security: Ek phone me maximum 3 IDs banayi ja sakti hain. Aapki teeno IDs hamesha safe rehti hain.';
            }
            const replyItem = {
              id: `dm_cs_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
              fromUserId: '10000',
              fromName: 'Customer service',
              fromAvatarUri: '',
              toUserId: fromUserId,
              toName: fromName,
              toAvatarUri: fromAvatarUri,
              text: csReply,
              msgType: 'TEXT',
              voiceDurationSec: 0,
              audioBase64: '',
              imageBase64: '',
              giftName: '',
              giftIcon: '',
              giftCount: 1,
              createdAtMs: Date.now(),
              isRecalled: false,
              isRead: true,
              deletedBy: [],
            };
            const curThread = dmThreads.get(threadKey) || [];
            curThread.push(replyItem);
            dmThreads.set(threadKey, curThread);
            firestoreStore.markDmDirty(threadKey);
            sendToUser(fromUserId, { type: 'NEW_DM', peerUserId: '10000', message: replyItem });
            pushSocialSyncToUser(fromUserId);
          }, 1050);
          break;
        }

        // Increment unread counter for recipient
        const unreadKey = `${toUserId}:${fromUserId}`;
        dmUnreadCounts.set(unreadKey, (dmUnreadCounts.get(unreadKey) || 0) + 1);

        sendToUser(toUserId, { type: 'NEW_DM', peerUserId: fromUserId, message: dmItem });
        sendToUser(fromUserId, { type: 'NEW_DM', peerUserId: toUserId, message: dmItem });
        pushSocialSyncToUser(toUserId);
        pushSocialSyncToUser(fromUserId);
        break;
      }

      case 'DELETE_DM_THREAD': {
        const userId = String(msg.userId || meta.userId || '');
        const peerUserId = String(msg.peerUserId || '');
        if (!userId || !peerUserId) break;
        const threadKey = getThreadKey(userId, peerUserId);
        dmUnreadCounts.set(`${userId}:${peerUserId}`, 0);
        dmThreads.delete(threadKey);
        firestoreStore.markDmDirty(threadKey);
        pushSocialSyncToUser(userId);
        break;
      }

      case 'DELETE_DM_MESSAGE': {
        const userId = String(msg.userId || meta.userId || '');
        const peerUserId = String(msg.peerUserId || '');
        const msgId = String(msg.msgId || '');
        if (!userId || !peerUserId || !msgId) break;
        const threadKey = getThreadKey(userId, peerUserId);
        const thread = dmThreads.get(threadKey);
        if (thread) {
          for (const item of thread) {
            if (item.id === msgId) {
              if (!Array.isArray(item.deletedBy)) item.deletedBy = [];
              if (!item.deletedBy.includes(userId)) item.deletedBy.push(userId);
            }
          }
          firestoreStore.markDmDirty(threadKey);
        }
        sendToUser(userId, { type: 'DM_MESSAGE_DELETED', peerUserId, msgId });
        pushSocialSyncToUser(userId);
        break;
      }

      case 'BLOCK_USER': {
        const fromUserId = String(msg.fromUserId || meta.userId || '');
        const targetUserId = String(msg.targetUserId || '');
        if (!fromUserId || !targetUserId || fromUserId === targetUserId) break;
        let map = blockedUsers.get(fromUserId);
        if (!map) {
          map = new Map();
          blockedUsers.set(fromUserId, map);
        }
        const targetProf = users.get(targetUserId) || {};
        map.set(targetUserId, {
          userId: targetUserId,
          nickname: msg.targetNickname || targetProf.nickname || `User ${targetUserId}`,
          avatarUri: msg.targetAvatarUri || targetProf.avatarUri || '',
          blockedAtMs: Date.now(),
        });
        firestoreStore.markUserDirty(fromUserId);
        pushSocialSyncToUser(fromUserId);
        break;
      }

      case 'UNBLOCK_USER': {
        const fromUserId = String(msg.fromUserId || meta.userId || '');
        const targetUserId = String(msg.targetUserId || '');
        if (!fromUserId || !targetUserId) break;
        blockedUsers.get(fromUserId)?.delete(targetUserId);
        firestoreStore.markUserDirty(fromUserId);
        pushSocialSyncToUser(fromUserId);
        break;
      }

      case 'SET_DM_PRIVACY': {
        const uid = String(msg.userId || meta.userId || '');
        if (!uid) break;
        userDmPrivacy.set(uid, msg.mode === 'FOLLOWING' ? 'FOLLOWING' : 'ALL');
        firestoreStore.markUserDirty(uid);
        pushSocialSyncToUser(uid);
        break;
      }

      case 'SUBMIT_REPORT': {
        const record = {
          id: `rep_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
          reporterId: String(msg.reporterId || meta.userId || ''),
          targetUserId: String(msg.targetUserId || ''),
          targetRoomId: String(msg.targetRoomId || ''),
          reason: String(msg.reason || 'Other'),
          details: String(msg.details || ''),
          createdAt: Date.now(),
        };
        reports.push(record);
        if (reports.length > 500) reports.shift();
        safeSend(ws, { type: 'REPORT_ACK', reportId: record.id });
        break;
      }

      case 'RECALL_DM': {
        const userId = String(msg.userId || meta.userId || '');
        const peerUserId = String(msg.peerUserId || '');
        const msgId = String(msg.msgId || '');
        if (!userId || !peerUserId || !msgId) break;
        const threadKey = getThreadKey(userId, peerUserId);
        const thread = dmThreads.get(threadKey);
        if (thread) {
          for (const item of thread) {
            if (item.id === msgId) {
              item.isRecalled = true;
            }
          }
          firestoreStore.markDmDirty(threadKey);
        }
        sendToUser(userId, { type: 'DM_RECALLED', peerUserId, msgId });
        sendToUser(peerUserId, { type: 'DM_RECALLED', peerUserId: userId, msgId });
        pushSocialSyncToUser(userId);
        pushSocialSyncToUser(peerUserId);
        break;
      }

      case 'MARK_DM_READ': {
        const userId = String(msg.userId || meta.userId || '');
        const peerUserId = String(msg.peerUserId || '');
        if (!userId || !peerUserId) break;
        dmUnreadCounts.set(`${userId}:${peerUserId}`, 0);
        const threadKey = getThreadKey(userId, peerUserId);
        const thread = dmThreads.get(threadKey);
        let markedAny = false;
        if (thread) {
          for (const item of thread) {
            if (item.toUserId === userId && !item.isRead) {
              item.isRead = true;
              markedAny = true;
            }
          }
          if (markedAny) firestoreStore.markDmDirty(threadKey);
        }
        if (markedAny && peerUserId !== '10000') {
          sendToUser(peerUserId, {
            type: 'DM_READ_RECEIPT',
            peerUserId: userId,
            readerUserId: userId,
            readAtMs: Date.now(),
          });
          pushSocialSyncToUser(peerUserId);
        }
        pushSocialSyncToUser(userId);
        break;
      }

      case 'MARK_SYSTEM_NOTICES_READ': {
        const userId = String(msg.userId || meta.userId || '');
        if (!userId) break;
        const notices = getOrInitSystemNotices(userId);
        for (const n of notices) n.isRead = true;
        const alerts = userFollowAlerts.get(userId) || [];
        for (const a of alerts) a.isRead = true;
        firestoreStore.markUserDirty(userId);
        pushSocialSyncToUser(userId);
        break;
      }

      case 'SEND_GIFT_TX': {
        const fromUserId = String(msg.fromUserId || meta.userId || '');
        const singleTarget = String(msg.targetUserId || '');
        const targetListRaw = Array.isArray(msg.targetUserIds)
          ? msg.targetUserIds.map((id) => String(id || '').trim()).filter(Boolean)
          : [];
        const uniqueTargets = Array.from(new Set(
          targetListRaw.length > 0 ? targetListRaw : (singleTarget ? [singleTarget] : [])
        ));
        const roomId = String(msg.roomId || meta.roomId || '');
        const charmDelta = Math.max(0, Math.floor(Number(msg.charmDelta || 0)));
        const coinsSpent = Math.max(0, Math.floor(Number(msg.coinsSpent || charmDelta)));
        const txId = String(msg.txId || '').trim();
        if (!fromUserId || coinsSpent <= 0) break;
        if (meta.userId && fromUserId !== meta.userId) break;
        if (!meta.userId) meta.userId = fromUserId;
        addUserSocket(fromUserId, ws);

        pruneWalletTxCache();
        if (txId && walletProcessedTxIds.has(txId)) {
          // Idempotent duplicate after reconnect: just re-sync wallet & rocket state cleanly
          pushWalletSyncToUser(fromUserId);
          if (roomId && rooms.has(roomId)) {
            safeSend(ws, {
              type: 'ROCKET_STATE_UPDATE',
              ackedTxId: txId,
              rocketState: buildRocketStatePayload(rooms.get(roomId)),
            });
          }
          break;
        }

        // Authoritative balance check / pre-paid spend tx linkage (prevents free gifts & double deduction)
        const prePaidTxId = msg.prePaidTxId ? String(msg.prePaidTxId) : '';
        let authorizedSpend = false;
        if (prePaidTxId && walletProcessedTxIds.has(prePaidTxId)) {
          const txEntry = walletProcessedTxIds.get(prePaidTxId);
          if (txEntry && txEntry.userId === fromUserId && Number(txEntry.remainingGiftCoins || 0) >= coinsSpent) {
            txEntry.remainingGiftCoins -= coinsSpent;
            walletProcessedTxIds.set(prePaidTxId, txEntry);
            authorizedSpend = true;
          }
        }
        if (!authorizedSpend) {
          const senderBal = getOrInitGreedyBalance(fromUserId);
          if (senderBal < coinsSpent) {
            safeSend(ws, {
              type: 'WALLET_ACTION_RESULT',
              action: 'SPEND',
              success: false,
              error: 'Insufficient Coins balance for gift!',
            });
            pushWalletSyncToUser(fromUserId);
            if (roomId && rooms.has(roomId)) {
              safeSend(ws, {
                type: 'ROCKET_STATE_UPDATE',
                ackedTxId: txId,
                rocketState: buildRocketStatePayload(rooms.get(roomId)),
              });
            }
            break;
          }
          greedyUserBalances.set(fromUserId, senderBal - coinsSpent);
          recordWalletTx(
            fromUserId,
            'COIN_USAGE',
            `Gift: ${msg.giftName || 'Gift'} x${Number(msg.giftCount || 1)}${uniqueTargets.length > 1 ? ` (${uniqueTargets.length} mics)` : ''}`,
            `-${coinsSpent.toLocaleString('en-US')} Coins`,
            false,
            txId || null
          );
          pushWalletSyncToUser(fromUserId);
        }

        if (txId) {
          walletProcessedTxIds.set(txId, {
            userId: fromUserId,
            amount: coinsSpent,
            remainingGiftCoins: 0,
            createdAtMs: Date.now(),
          });
        }

        const perTargetCoins = uniqueTargets.length > 0
          ? Math.max(1, Math.floor(coinsSpent / uniqueTargets.length))
          : coinsSpent;
        const perTargetCharm = uniqueTargets.length === 1 && charmDelta > 0
          ? charmDelta
          : perTargetCoins;

        for (const tUid of uniqueTargets) {
          if (!tUid || perTargetCharm <= 0) continue;
          const existing = users.get(tUid) || { userId: tUid, user8DigitId: tUid };
          existing.totalCharmPoints = Number(existing.totalCharmPoints || 0) + perTargetCharm;
          if (msg.giftName) {
            const gifts = Array.isArray(existing.receivedGifts) ? [...existing.receivedGifts] : [];
            const gIdx = gifts.findIndex((g) => g.name === msg.giftName);
            const gCount = Number(msg.giftCount || 1);
            if (gIdx >= 0) {
              gifts[gIdx] = {
                ...gifts[gIdx],
                count: Number(gifts[gIdx].count || 0) + gCount,
                charmValue: Number(gifts[gIdx].charmValue || 0) + perTargetCharm,
              };
            } else {
              gifts.push({
                id: msg.giftId || `g_${Date.now()}`,
                name: msg.giftName,
                icon: msg.giftIcon || '🎁',
                count: gCount,
                charmValue: perTargetCharm,
              });
            }
            existing.receivedGifts = gifts.slice(0, 30);
          }
          users.set(tUid, existing);
          firestoreStore.markUserDirty(tUid);
          pushSocialSyncToUser(tUid);

          // Credit authoritative Diamonds to receiver (1 Coin gift = 1 Diamond, exchange = 33%)
          if (tUid !== fromUserId && perTargetCoins > 0) {
            const curDiamonds = getOrInitDiamondsBalance(tUid);
            userDiamonds.set(tUid, curDiamonds + perTargetCoins);
            recordWalletTx(
              tUid,
              'DIAMOND_INCOME',
              `Gift Income: ${msg.giftName || 'Gift'} x${Number(msg.giftCount || 1)}`,
              `+${perTargetCoins.toLocaleString('en-US')} Diamonds`,
              true
            );
            pushWalletSyncToUser(tUid);
          }
        }

        if (fromUserId && coinsSpent > 0) {
          const giftSendExp = Math.max(1, Math.floor(coinsSpent / 5));
          grantUserExp(fromUserId, giftSendExp, 'GIFT_SEND');
          creditInviteGiftCommission(fromUserId, coinsSpent);
          if (profileAndMedals && typeof profileAndMedals.onGiftTransactionCompleted === 'function') {
            profileAndMedals.onGiftTransactionCompleted({
              fromUserId,
              uniqueTargets,
              coinsSpent,
              perTargetCharm,
              giftId: msg.giftId || '',
              giftName: msg.giftName || '',
              giftIcon: msg.giftIcon || '',
              giftCount: Number(msg.giftCount || 1),
            });
          }
          firestoreStore.markUserDirty(fromUserId);
        }

        if (roomId && coinsSpent > 0) {
          const room = rooms.get(roomId) || getOrCreateRoom(roomId, { hostId: roomId });
          if (room) {
            firestoreStore.markRoomDirty(room.id);
            room.roomTotalCharm = Number(room.roomTotalCharm || 0) + coinsSpent;
            const senderProf = users.get(fromUserId) || {};
            const contribEntry = {
              userId: fromUserId,
              nickname: msg.fromName || senderProf.nickname || `User ${fromUserId}`,
              avatarUri: msg.fromAvatarUri || senderProf.avatarUri || '',
              level: Number(userLevels.get(fromUserId)?.level ?? senderProf.level ?? 0),
              gender: senderProf.gender || 'Boy',
            };
            ['daily', 'weekly', 'monthly'].forEach((period) => {
              const map = room.roomContributions[period];
              const prev = map.get(fromUserId);
              map.set(fromUserId, {
                ...contribEntry,
                coins: Number(prev?.coins || 0) + coinsSpent,
              });
            });
            // Rocket Launcher Fuel Progression & Launch Trigger (25L / 50L / 1Cr)
            ensureRocketDailyFresh(room);
            const rs = room.rocketState;
            if (!rs.allLaunched) {
              rs.currentExp = Number(rs.currentExp || 0) + coinsSpent;
              const prevRc = rs.contributors.get(fromUserId);
              rs.contributors.set(fromUserId, {
                userId: fromUserId,
                nickname: contribEntry.nickname,
                avatarUri: contribEntry.avatarUri,
                coins: Number(prevRc?.coins || 0) + coinsSpent,
              });

              while (!rs.allLaunched) {
                const lvl = Math.min(3, Math.max(1, Number(rs.level || 1)));
                const target = ROCKET_TARGETS[lvl] || ROCKET_TARGETS[1];
                if (rs.currentExp < target) break;

                const excess = rs.currentExp - target;
                const sortedContribs = Array.from(rs.contributors.values())
                  .sort((a, b) => Number(b.coins || 0) - Number(a.coins || 0));
                const eligibleTopContribs = sortedContribs.filter(
                  (c) => Number(c.coins || 0) >= ROCKET_MIN_TOP_COINS
                );
                const topGifter = eligibleTopContribs[0] || sortedContribs[0] || {
                  userId: fromUserId,
                  nickname: contribEntry.nickname,
                  avatarUri: contribEntry.avatarUri,
                  coins: coinsSpent,
                };

                const { rewardsByUser, expByUser, winners, totalPoolCoins } = computeRocketLaunchRewards(lvl, eligibleTopContribs, room);
                const launchId = `rocket_${room.id}_L${lvl}_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
                const launchedAtMs = Date.now();

                const resolvedTopGifterAvatar = resolveLiveUserAvatar(topGifter.userId, topGifter.avatarUri);
                const resolvedTopGifterName = users.get(topGifter.userId)?.nickname || topGifter.nickname || 'Top Gifter';

                rs.history.unshift({
                  launchId,
                  level: lvl,
                  topGifterId: topGifter.userId,
                  topGifterName: resolvedTopGifterName,
                  topGifterAvatarUri: resolvedTopGifterAvatar,
                  totalPoolCoins,
                  launchedAtMs,
                });
                rs.history = rs.history.slice(0, 30);

                // Credit Rocket 1x EXP AND Rocket Blast Coins authoritatively on server
                for (const [winnerUid, expAmt] of Object.entries(expByUser)) {
                  if (Number(expAmt) > 0) {
                    grantUserExp(winnerUid, Number(expAmt), `ROCKET_L${lvl}`);
                  }
                }
                for (const [winnerUid, coinAmtRaw] of Object.entries(rewardsByUser)) {
                  const coinAmt = Math.floor(Number(coinAmtRaw || 0));
                  if (coinAmt > 0) {
                    const prevB = getOrInitGreedyBalance(winnerUid);
                    const nextB = Math.min(GREEDY_MAX_BALANCE, prevB + coinAmt);
                    greedyUserBalances.set(winnerUid, nextB);
                    firestoreStore.markUserDirty(winnerUid);
                    recordWalletTx(
                      winnerUid,
                      'COIN_USAGE',
                      `Lv.${lvl} Rocket Blast Reward`,
                      `+${coinAmt.toLocaleString('en-US')} Coins`,
                      true,
                      `rocket_win_${launchId}_${winnerUid}`
                    );
                    pushWalletSyncToUser(winnerUid);
                  }
                }

                broadcastToRoom(room, {
                  type: 'ROCKET_LAUNCHED',
                  roomId: room.id,
                  launchId,
                  level: lvl,
                  topGifterId: topGifter.userId,
                  topGifterName: resolvedTopGifterName,
                  topGifterAvatarUri: resolvedTopGifterAvatar,
                  totalPoolCoins,
                  rewardsByUser,
                  expByUser,
                  winners,
                  launchedAtMs,
                });

                const sysMsg = {
                  id: `sys_rocket_${launchId}`,
                  senderId: 'system',
                  senderName: 'System',
                  message: `🚀 Lv.${lvl} Rocket launched! Top Fueler: ${topGifter.nickname || 'VIP'} • ${totalPoolCoins.toLocaleString()} coins shared!`,
                  isSystem: true,
                  createdAtMs: launchedAtMs,
                };
                room.messages.push(sysMsg);
                if (room.messages.length > 80) room.messages = room.messages.slice(-60);
                broadcastToRoom(room, { type: 'NEW_MESSAGE', roomId: room.id, message: sysMsg });

                if (lvl < 3) {
                  rs.level = lvl + 1;
                  rs.currentExp = excess;
                  rs.contributors = new Map();
                  if (excess > 0) {
                    rs.contributors.set(fromUserId, {
                      userId: fromUserId,
                      nickname: contribEntry.nickname,
                      avatarUri: contribEntry.avatarUri,
                      coins: excess,
                    });
                  }
                } else {
                  rs.allLaunched = true;
                  rs.currentExp = target;
                  break;
                }
              }
            }

            const rocketPayload = buildRocketStatePayload(room);
            safeSend(ws, {
              type: 'ROCKET_STATE_UPDATE',
              ackedTxId: txId,
              rocketState: rocketPayload,
            });
            broadcastToRoom(room, {
              type: 'ROCKET_STATE_UPDATE',
              ackedTxId: txId,
              rocketState: rocketPayload,
            });
            broadcastToRoom(room, buildContributionsPayload(room));
            broadcastToRoom(room, buildRoomSnapshot(room));
          }
        }
        break;
      }

      case 'GET_ROCKET_STATE': {
        const room = rooms.get(String(msg.roomId || ''));
        if (room) {
          safeSend(ws, {
            type: 'ROCKET_STATE_UPDATE',
            rocketState: buildRocketStatePayload(room),
          });
        }
        break;
      }

      case 'GET_ROOM_CONTRIBUTIONS': {
        const room = rooms.get(String(msg.roomId || ''));
        if (room) {
          safeSend(ws, buildContributionsPayload(room));
        }
        break;
      }

      case 'ROOM_SOUND_EFFECT': {
        const room = rooms.get(String(msg.roomId || ''));
        if (!room) break;
        broadcastToRoom(room, {
          type: 'ROOM_SOUND_EFFECT',
          roomId: room.id,
          senderId: String(msg.senderId || meta.userId || ''),
          senderName: String(msg.senderName || 'User'),
          effectId: String(msg.effectId || 'applause'),
          effectName: String(msg.effectName || 'Applause'),
          emoji: String(msg.emoji || '👏'),
        });
        break;
      }

      case 'VERIFY_ROOM_PASSWORD': {
        const roomId = String(msg.roomId || '');
        const pin = String(msg.password || '');
        const room = rooms.get(roomId);
        const ok = !room || !room.roomPassword || room.roomPassword === pin;
        safeSend(ws, {
          type: 'ROOM_PASSWORD_RESULT',
          roomId,
          ok,
        });
        break;
      }

      case 'CREATE_OR_SAVE_ROOM': {
        const { roomId, title, announcement, hostId, hostName, hostAvatarUri, roomPassword } = msg;
        if (!roomId) break;
        const room = getOrCreateRoom(roomId, {
          title,
          announcement,
          hostId,
          hostName,
          hostAvatarUri,
          roomPassword: roomPassword || '',
        });
        if (title) room.title = title;
        if (announcement) room.announcement = announcement;
        if (hostId) room.hostId = hostId;
        if (hostName) room.hostName = hostName;
        if (hostAvatarUri) room.hostAvatarUri = hostAvatarUri;
        if (roomPassword !== undefined) room.roomPassword = String(roomPassword || '');
        room.updatedAt = Date.now();
        firestoreStore.markRoomDirty(room.id);
        broadcastToRoom(room, buildRoomSnapshot(room));
        broadcastLiveRoomsToHome();
        break;
      }

      case 'JOIN_ROOM': {
        const { roomId, userId, title, announcement, isHost, hostName, hostAvatarUri } = msg;
        if (!roomId || !userId) break;

        const timerKey = `${roomId}:${userId}`;
        if (pendingDisconnectTimers.has(timerKey)) {
          clearTimeout(pendingDisconnectTimers.get(timerKey));
          pendingDisconnectTimers.delete(timerKey);
        }

        if (meta.roomId && meta.roomId !== roomId) {
          removeUserFromRoom(ws, meta.roomId, meta.userId || userId);
        }

        meta.userId = String(userId);
        meta.roomId = String(roomId);
        addUserSocket(meta.userId, ws);

        const room = getOrCreateRoom(roomId, {
          title,
          announcement,
          hostId: isHost ? userId : roomId,
          hostName,
          hostAvatarUri,
        });

        for (const [otherId, otherRoom] of rooms.entries()) {
          if (otherId !== roomId && otherRoom.activeClients.has(userId)) {
            removeUserFromRoom(otherRoom.activeClients.get(userId), otherId, userId);
          }
        }

        if (isHost) {
          if (title) room.title = title;
          if (announcement) room.announcement = announcement;
          room.hostId = userId;
          if (hostName) room.hostName = hostName;
          if (hostAvatarUri) room.hostAvatarUri = hostAvatarUri;
        }

        room.activeClients.set(userId, ws);
        room.updatedAt = Date.now();
        const prevStay = userRoomStayTracking.get(String(userId));
        if (!prevStay || prevStay.roomId !== String(roomId)) {
          const nowTs = Date.now();
          userRoomStayTracking.set(String(userId), {
            roomId: String(roomId),
            joinedAtMs: nowTs,
            lastClaimAtMs: nowTs,
          });
        }

        broadcastToRoom(room, buildRoomSnapshot(room));
        safeSend(ws, buildContributionsPayload(room));
        broadcastLiveRoomsToHome();
        break;
      }

      case 'LEAVE_ROOM': {
        const targetRoomId = msg.roomId || meta.roomId;
        const targetUserId = msg.userId || meta.userId;
        const timerKey = `${targetRoomId}:${targetUserId}`;
        if (pendingDisconnectTimers.has(timerKey)) {
          clearTimeout(pendingDisconnectTimers.get(timerKey));
          pendingDisconnectTimers.delete(timerKey);
        }
        if (meta.roomId === targetRoomId) {
          meta.roomId = '';
        }
        removeUserFromRoom(ws, targetRoomId, targetUserId);
        break;
      }

      case 'UPDATE_SEATS': {
        const { roomId, updatedSeats, localUserId, currentLocalSeatIndex, isModeratorSeatEdit } = msg;
        const room = rooms.get(roomId);
        if (!room || !Array.isArray(updatedSeats)) break;

        room.seats = room.seats.map((serverSeat, idx) => {
          const localSeat = updatedSeats[idx] || serverSeat;
          const serverOccId = serverSeat.occupiedUserId || null;
          const localOccId = localSeat.occupiedUserId || null;

          if (currentLocalSeatIndex !== null && currentLocalSeatIndex !== undefined && idx === currentLocalSeatIndex && localUserId) {
            const effLocked = isModeratorSeatEdit ? Boolean(localSeat.isLocked) : Boolean(serverSeat.isLocked);
            const effForceMuted = isModeratorSeatEdit ? Boolean(localSeat.isForceMuted) : Boolean(serverSeat.isForceMuted);
            const effMuted = Boolean(localSeat.isMuted) || effForceMuted;
            return {
              ...serverSeat,
              ...localSeat,
              seatIndex: idx,
              occupiedUserId: localSeat.occupiedUserId || serverSeat.occupiedUserId || localUserId,
              isLocked: effLocked,
              isForceMuted: effForceMuted,
              isMuted: effMuted,
            };
          } else if (localUserId && idx !== currentLocalSeatIndex && (serverOccId === localUserId || localOccId === localUserId)) {
            const base = isModeratorSeatEdit ? localSeat : serverSeat;
            const effForceMuted = isModeratorSeatEdit ? Boolean(localSeat.isForceMuted) : Boolean(serverSeat.isForceMuted);
            return {
              ...base,
              seatIndex: idx,
              isLocked: isModeratorSeatEdit ? Boolean(localSeat.isLocked) : Boolean(serverSeat.isLocked),
              isForceMuted: effForceMuted,
              occupiedUserId: null,
              occupiedNickname: null,
              occupiedAvatarUri: null,
              isSpeaking: false,
              isMuted: effForceMuted,
            };
          } else if (isModeratorSeatEdit) {
            return {
              ...serverSeat,
              ...localSeat,
              seatIndex: idx,
            };
          }
          return serverSeat;
        });

        room.updatedAt = Date.now();
        broadcastToRoom(room, buildRoomSnapshot(room));
        break;
      }

      case 'UPDATE_ROOM_FIELDS': {
        const { roomId, fields } = msg;
        const room = rooms.get(roomId);
        if (!room || !fields) break;

        const allowedKeys = [
          'title',
          'announcement',
          'hostId',
          'hostName',
          'hostAvatarUri',
          'isHostOnOtherSeat',
          'isHostMuted',
          'themeDrawableResId',
          'isAdminChatOnly',
          'chatClearedAt',
          'roomPassword',
          'adminUsers',
          'bannedChatUserIds',
          'bannedUsers',
          'categoryTag',
          'countryName',
        ];
        for (const key of allowedKeys) {
          if (fields[key] !== undefined) {
            room[key] = fields[key] === null ? '' : fields[key];
          }
        }
        room.updatedAt = Date.now();
        firestoreStore.markRoomDirty(room.id);
        broadcastToRoom(room, buildRoomSnapshot(room));
        if (fields.title !== undefined || fields.hostName !== undefined || fields.hostAvatarUri !== undefined || fields.roomPassword !== undefined) {
          broadcastLiveRoomsToHome();
        }
        break;
      }

      case 'SEND_MESSAGE': {
        const { roomId, message } = msg;
        const room = rooms.get(roomId);
        if (!room || !message) break;
        const knownSender = message.senderId ? users.get(message.senderId) : null;
        const resolvedAvatar = message.senderAvatarUri || (knownSender && knownSender.avatarUri) || '';
        const chatItem = {
          ...message,
          id: message.id || `msg_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
          senderAvatarUri: resolvedAvatar,
          createdAtMs: message.createdAtMs || Date.now(),
        };
        if (
          chatItem.replaceKey &&
          room.messages.length > 0 &&
          room.messages[room.messages.length - 1].replaceKey === chatItem.replaceKey
        ) {
          room.messages[room.messages.length - 1] = chatItem;
        } else {
          room.messages.push(chatItem);
        }
        if (room.messages.length > 200) {
          room.messages = room.messages.slice(-200);
        }
        firestoreStore.markRoomDirty(room.id);
        broadcastToRoom(room, {
          type: 'NEW_MESSAGE',
          roomId,
          message: chatItem,
        });
        break;
      }

      case 'CLEAR_MESSAGES': {
        const { roomId, clearedAtMs } = msg;
        const room = rooms.get(roomId);
        if (!room) break;
        const actorId = String(meta.userId || msg.userId || '').trim();
        const isHostOrAdmin = Boolean(
          actorId &&
          (room.id === actorId ||
            room.hostId === actorId ||
            (room.adminUsers || []).some((a) => a && String(a.userId || '') === actorId))
        );
        if (!isHostOrAdmin) break;
        room.chatClearedAt = clearedAtMs || Date.now();
        room.messages = [];
        firestoreStore.markRoomDirty(room.id);
        broadcastToRoom(room, buildRoomSnapshot(room));
        break;
      }

      case 'WEBRTC_SIGNAL': {
        const { roomId, toPeerId } = msg;
        const room = rooms.get(roomId);
        if (!room) break;
        if (toPeerId && toPeerId !== 'ALL') {
          const targetWs = room.activeClients.get(toPeerId);
          safeSend(targetWs, msg);
        } else {
          broadcastToRoom(room, msg, msg.fromPeerId || meta.userId);
        }
        break;
      }

      case 'VOICE_FRAME': {
        const { roomId, senderId, targetPeerIds } = msg;
        const room = rooms.get(roomId);
        if (!room) break;
        if (Array.isArray(targetPeerIds)) {
          const raw = JSON.stringify(msg);
          for (const targetId of targetPeerIds) {
            if (targetId === senderId) continue;
            const targetWs = room.activeClients.get(targetId);
            if (targetWs && targetWs.readyState === WebSocket.OPEN) {
              try {
                targetWs.send(raw);
              } catch (_) {}
            }
          }
        } else {
          broadcastToRoom(room, msg, senderId || meta.userId);
        }
        break;
      }

      case 'SUBMIT_FEEDBACK': {
        feedbacks.push({
          ...msg.ticket,
          createdAt: Date.now(),
        });
        if (feedbacks.length > 500) feedbacks.shift();
        safeSend(ws, { type: 'FEEDBACK_ACK', ticketId: msg.ticket?.id || '' });
        break;
      }

      case 'LEVEL_INIT_SYNC': {
        const uid = String(meta.userId || msg.userId || '');
        if (!uid) break;
        if (!meta.userId) meta.userId = uid;
        addUserSocket(uid, ws);
        const st = getOrCreateUserLevelState(uid);
        const clientExp = Math.max(0, Math.floor(Number(msg.clientExp || 0)));
        if (st.exp === 0 && clientExp > 0) {
          st.exp = clientExp;
          const computed = computeLevelFromExp(st.exp);
          st.level = computed.level;
          if (Array.isArray(msg.creditedCoinLevels)) {
            for (const lv of msg.creditedCoinLevels) {
              const n = Number(lv);
              if (Number.isInteger(n) && n > 0) st.creditedCoinLevels.add(n);
            }
          }
          const today = getCurrentDayKey();
          if (Number(msg.dayKey) === today) {
            st.dailyOwnRoomStayExp = Math.min(
              ROOM_STAY_MAX_OWN_DAILY,
              Math.max(st.dailyOwnRoomStayExp, Math.floor(Number(msg.dailyOwnRoomStayExpEarned || 0)))
            );
            st.dailyOtherRoomStayExp = Math.min(
              ROOM_STAY_MAX_OTHER_DAILY,
              Math.max(st.dailyOtherRoomStayExp, Math.floor(Number(msg.dailyOtherRoomStayExpEarned || 0)))
            );
          }
          const prof = users.get(uid);
          if (prof) {
            prof.level = st.level;
            users.set(uid, prof);
          }
        }
        safeSend(ws, buildLevelStatePayload(uid));
        break;
      }

      case 'ROOM_STAY_CLAIM': {
        const uid = String(meta.userId || msg.userId || '');
        const roomId = String(msg.roomId || meta.roomId || '');
        if (!uid || !roomId) break;
        const room = rooms.get(roomId);
        if (!room || !room.activeClients.has(uid)) break;

        const nowMs = Date.now();
        let track = userRoomStayTracking.get(uid);
        if (!track || track.roomId !== roomId) {
          track = { roomId, joinedAtMs: nowMs, lastClaimAtMs: nowMs };
          userRoomStayTracking.set(uid, track);
          safeSend(ws, buildLevelStatePayload(uid));
          break;
        }
        if (nowMs - track.lastClaimAtMs < ROOM_STAY_MIN_ELAPSED_MS) {
          // Too soon (anti-cheat interval guard)
          safeSend(ws, buildLevelStatePayload(uid));
          break;
        }
        track.lastClaimAtMs = nowMs;

        const st = getOrCreateUserLevelState(uid);
        const isOwnRoom = roomId === uid || room.hostId === uid;
        if (isOwnRoom) {
          const remaining = Math.max(0, ROOM_STAY_MAX_OWN_DAILY - st.dailyOwnRoomStayExp);
          const gain = Math.min(ROOM_STAY_EXP_PER_STEP, remaining);
          if (gain > 0) {
            st.dailyOwnRoomStayExp += gain;
            grantUserExp(uid, gain, 'ROOM_STAY_OWN');
          } else {
            safeSend(ws, buildLevelStatePayload(uid));
          }
        } else {
          const remaining = Math.max(0, ROOM_STAY_MAX_OTHER_DAILY - st.dailyOtherRoomStayExp);
          const gain = Math.min(ROOM_STAY_EXP_PER_STEP, remaining);
          if (gain > 0) {
            st.dailyOtherRoomStayExp += gain;
            grantUserExp(uid, gain, 'ROOM_STAY_OTHER');
          } else {
            safeSend(ws, buildLevelStatePayload(uid));
          }
        }
        break;
      }

      case 'WALLET_INIT_SYNC': {
        const uid = String(meta.userId || msg.userId || '');
        if (!uid) break;
        if (!meta.userId) meta.userId = uid;
        if (msg.deviceIntegrity) {
          recordUserDeviceIntegrity(uid, msg.deviceIntegrity);
        }
        safeSend(ws, buildWalletStatePayload(uid, msg.deviceIntegrity || null));
        safeSend(ws, { type: 'WALLET_BALANCE_SYNC', serverBalance: getOrInitGreedyBalance(uid) });
        break;
      }

      case 'WALLET_DAILY_CHECKIN': {
        const uid = String(meta.userId || msg.userId || '');
        const integrity = msg.deviceIntegrity || null;
        if (!uid) break;
        if (meta.userId && msg.userId && String(msg.userId) !== meta.userId) break;
        if (!meta.userId) meta.userId = uid;

        if (integrity) recordUserDeviceIntegrity(uid, integrity);
        const check = validateIntegrityForWallet(uid, integrity);
        if (!check.ok) {
          safeSend(ws, { type: 'WALLET_ACTION_RESULT', action: 'CHECKIN', success: false, error: check.error });
          break;
        }

        const todayKey = getTodayDateKeyIst();
        if (walletDailyCheckIns.get(uid) === todayKey) {
          safeSend(ws, { type: 'WALLET_ACTION_RESULT', action: 'CHECKIN', success: false, error: 'Already checked in today!' });
          safeSend(ws, buildWalletStatePayload(uid, integrity));
          break;
        }
        const devKeys = getDeviceKeysFromIntegrity(integrity);
        if (devKeys.some((dk) => deviceDailyCheckIns.get(dk) === todayKey)) {
          safeSend(ws, { type: 'WALLET_ACTION_RESULT', action: 'CHECKIN', success: false, error: 'Daily check-in already claimed on this device today.' });
          break;
        }

        walletDailyCheckIns.set(uid, todayKey);
        devKeys.forEach((dk) => {
          deviceDailyCheckIns.set(dk, todayKey);
          firestoreStore.markDeviceDirty(dk);
        });

        const curBal = getOrInitGreedyBalance(uid);
        const newBal = Math.min(GREEDY_MAX_BALANCE, curBal + DAILY_CHECKIN_REWARD_COINS);
        greedyUserBalances.set(uid, newBal);
        recordWalletTx(uid, 'COIN_USAGE', 'Daily Check-In Bonus', `+${DAILY_CHECKIN_REWARD_COINS} Coins`, true, `checkin_${uid}_${todayKey}`);
        firestoreStore.markUserDirty(uid);

        safeSend(ws, buildWalletStatePayload(uid, integrity));
        safeSend(ws, { type: 'WALLET_BALANCE_SYNC', serverBalance: newBal });
        safeSend(ws, { type: 'WALLET_ACTION_RESULT', action: 'CHECKIN', success: true, message: `Daily Check-In +${DAILY_CHECKIN_REWARD_COINS} Coins credited!` });
        break;
      }

      case 'WALLET_CREDIT_PACK': {
        const uid = String(meta.userId || msg.userId || '');
        const integrity = msg.deviceIntegrity || null;
        if (!uid) break;
        if (meta.userId && msg.userId && String(msg.userId) !== meta.userId) break;
        if (!meta.userId) meta.userId = uid;
        if (integrity) recordUserDeviceIntegrity(uid, integrity);

        const check = validateIntegrityForWallet(uid, integrity);
        if (!check.ok) {
          safeSend(ws, { type: 'WALLET_RECHARGE_RESULT', success: false, error: check.error });
          break;
        }

        const packId = String(msg.packId || '');
        const packSpec = WALLET_RECHARGE_PACKS[packId];
        if (!packSpec) {
          safeSend(ws, { type: 'WALLET_RECHARGE_RESULT', success: false, error: 'Invalid recharge package.' });
          break;
        }

        const channel = ['PhonePe', 'Paytm', 'UPI', 'GPay'].includes(String(msg.channel || ''))
          ? String(msg.channel)
          : 'UPI';

        const now = Date.now();
        pruneWalletTxCache(now);
        const orderId = String(msg.clientOrderId || '').trim();
        if (!orderId || orderId.length < 8) {
          safeSend(ws, { type: 'WALLET_RECHARGE_RESULT', success: false, error: 'Missing valid order reference.' });
          break;
        }
        if (walletProcessedTxIds.has(`rc_${orderId}`)) {
          safeSend(ws, { type: 'WALLET_RECHARGE_RESULT', success: false, error: 'Duplicate order blocked by anti-cheat.' });
          safeSend(ws, buildWalletStatePayload(uid, integrity));
          break;
        }

        const prevRcAt = walletLastRechargeAt.get(uid) || 0;
        if (now - prevRcAt < WALLET_RECHARGE_COOLDOWN_MS) {
          safeSend(ws, { type: 'WALLET_RECHARGE_RESULT', success: false, error: 'Please wait a few seconds between recharges.' });
          break;
        }

        walletProcessedTxIds.set(`rc_${orderId}`, { userId: uid, amount: packSpec.coins, remainingGiftCoins: 0, createdAtMs: now });
        walletLastRechargeAt.set(uid, now);

        const curBal = getOrInitGreedyBalance(uid);
        const newBal = Math.min(GREEDY_MAX_BALANCE, curBal + packSpec.coins);
        greedyUserBalances.set(uid, newBal);

        recordWalletTx(
          uid,
          'COIN_RECHARGE',
          `Recharge Coins (${channel})`,
          `+${packSpec.coins.toLocaleString('en-US')} Coins (₹${packSpec.priceInr.toFixed(1)})`,
          true,
          `rc_rec_${orderId}`
        );
        recordWalletTx(
          uid,
          'COIN_USAGE',
          'Account Recharge',
          `+${packSpec.coins.toLocaleString('en-US')} Coins`,
          true,
          `rc_use_${orderId}`
        );

        creditInviteRechargeProgress(uid, packSpec.coins);
        firestoreStore.markUserDirty(uid);
        safeSend(ws, buildWalletStatePayload(uid, integrity));
        safeSend(ws, { type: 'WALLET_BALANCE_SYNC', serverBalance: newBal });
        safeSend(ws, {
          type: 'WALLET_RECHARGE_RESULT',
          success: true,
          packId,
          orderId,
          coinsAdded: packSpec.coins,
          priceInr: packSpec.priceInr,
          channel,
          message: `Recharged ₹${packSpec.priceInr.toFixed(1)} (+${packSpec.coins.toLocaleString('en-US')} Coins) via ${channel}!`,
        });
        break;
      }

      case 'WALLET_EXCHANGE_DIAMONDS': {
        const uid = String(meta.userId || msg.userId || '');
        const integrity = msg.deviceIntegrity || null;
        if (!uid) break;
        if (meta.userId && msg.userId && String(msg.userId) !== meta.userId) break;
        if (!meta.userId) meta.userId = uid;
        if (integrity) recordUserDeviceIntegrity(uid, integrity);

        const check = validateIntegrityForWallet(uid, integrity);
        if (!check.ok) {
          safeSend(ws, { type: 'WALLET_EXCHANGE_RESULT', success: false, error: check.error });
          break;
        }

        const exCoins = Number(msg.coins);
        const requiredDiamonds = WALLET_EXCHANGE_DIAMOND_COST[exCoins];
        if (!Number.isInteger(exCoins) || !requiredDiamonds) {
          safeSend(ws, { type: 'WALLET_EXCHANGE_RESULT', success: false, error: 'Invalid exchange package.' });
          break;
        }

        const curDiamonds = getOrInitDiamondsBalance(uid);
        if (curDiamonds < requiredDiamonds) {
          safeSend(ws, {
            type: 'WALLET_EXCHANGE_RESULT',
            success: false,
            error: `Insufficient Diamonds! Need ${requiredDiamonds.toLocaleString('en-US')} Diamonds.`,
          });
          safeSend(ws, buildWalletStatePayload(uid, integrity));
          break;
        }

        userDiamonds.set(uid, curDiamonds - requiredDiamonds);
        const curBal = getOrInitGreedyBalance(uid);
        const newBal = Math.min(GREEDY_MAX_BALANCE, curBal + exCoins);
        greedyUserBalances.set(uid, newBal);

        recordWalletTx(
          uid,
          'DIAMOND_EXCHANGE',
          'Exchange diamonds to coins',
          `-${requiredDiamonds.toLocaleString('en-US')} Diamonds`,
          false
        );
        recordWalletTx(
          uid,
          'COIN_USAGE',
          'Exchange from Diamonds',
          `+${exCoins.toLocaleString('en-US')} Coins`,
          true
        );
        firestoreStore.markUserDirty(uid);

        safeSend(ws, buildWalletStatePayload(uid, integrity));
        safeSend(ws, { type: 'WALLET_BALANCE_SYNC', serverBalance: newBal });
        safeSend(ws, {
          type: 'WALLET_EXCHANGE_RESULT',
          success: true,
          coinsAdded: exCoins,
          diamondsSpent: requiredDiamonds,
          message: `Exchanged ${requiredDiamonds.toLocaleString('en-US')} Diamonds for +${exCoins.toLocaleString('en-US')} Coins!`,
        });
        break;
      }

      case 'WALLET_ADD_REWARD': {
        // Blocked by Anti-Cheat: rewards can only be credited by authoritative server events (Level, Rocket, Invite, Check-In).
        const uid = String(meta.userId || msg.userId || '');
        safeSend(ws, {
          type: 'WALLET_ACTION_RESULT',
          action: 'ADD_REWARD',
          success: false,
          error: 'Direct client coin minting is blocked by anti-cheat.',
        });
        if (uid) {
          safeSend(ws, buildWalletStatePayload(uid));
        }
        break;
      }

      case 'WALLET_SPEND': {
        const uid = String(meta.userId || msg.userId || '');
        if (!uid) break;
        if (meta.userId && msg.userId && String(msg.userId) !== meta.userId) break;
        if (!meta.userId) meta.userId = uid;

        const amt = Number(msg.amount);
        const txId = String(msg.txId || '').trim();
        const title = String(msg.title || 'Purchase').slice(0, 96);
        if (!Number.isInteger(amt) || amt <= 0) break;

        pruneWalletTxCache();
        if (txId && walletProcessedTxIds.has(txId)) {
          safeSend(ws, buildWalletStatePayload(uid));
          safeSend(ws, { type: 'WALLET_SPEND_RESULT', success: true, txId, serverBalance: getOrInitGreedyBalance(uid) });
          break;
        }

        const curBal = getOrInitGreedyBalance(uid);
        if (curBal < amt) {
          safeSend(ws, {
            type: 'WALLET_SPEND_RESULT',
            success: false,
            txId,
            error: 'Insufficient Coins balance on server!',
            serverBalance: curBal,
          });
          safeSend(ws, buildWalletStatePayload(uid));
          break;
        }

        const newBal = curBal - amt;
        greedyUserBalances.set(uid, newBal);
        if (txId) {
          walletProcessedTxIds.set(txId, {
            userId: uid,
            amount: amt,
            remainingGiftCoins: title.toLowerCase().includes('gift') ? amt : 0,
            createdAtMs: Date.now(),
          });
        }
        recordWalletTx(uid, 'COIN_USAGE', title, `-${amt.toLocaleString('en-US')} Coins`, false, txId || null);
        firestoreStore.markUserDirty(uid);

        safeSend(ws, buildWalletStatePayload(uid));
        safeSend(ws, { type: 'WALLET_BALANCE_SYNC', serverBalance: newBal });
        safeSend(ws, { type: 'WALLET_SPEND_RESULT', success: true, txId, serverBalance: newBal });
        break;
      }

      case 'STORE_INIT_SYNC': {
        const uid = String(meta.userId || msg.userId || '');
        if (!uid) break;
        if (!meta.userId) meta.userId = uid;
        addUserSocket(uid, ws);
        if (msg.deviceIntegrity) {
          recordUserDeviceIntegrity(uid, msg.deviceIntegrity);
        }
        safeSend(ws, buildStoreStatePayload(uid, msg.deviceIntegrity || null));
        break;
      }

      case 'STORE_BUY_ITEM': {
        const uid = String(meta.userId || msg.userId || '');
        const integrity = msg.deviceIntegrity || null;
        if (!uid) break;
        if (meta.userId && msg.userId && String(msg.userId) !== meta.userId) break;
        if (!meta.userId) meta.userId = uid;
        addUserSocket(uid, ws);
        if (integrity) recordUserDeviceIntegrity(uid, integrity);

        const check = validateIntegrityForWallet(uid, integrity);
        if (!check.ok) {
          safeSend(ws, { type: 'STORE_ACTION_RESULT', action: 'BUY', success: false, error: check.error });
          break;
        }

        const itemId = String(msg.itemId || '');
        const durationDays = Number(msg.durationDays);
        const equipImmediately = Boolean(msg.equipImmediately);
        const orderId = String(msg.clientOrderId || '').trim();
        const itemSpec = STORE_CATALOG[itemId];
        const durationMult = STORE_DURATION_MULT[durationDays];
        if (!itemSpec || itemSpec.active === false || !durationMult) {
          safeSend(ws, { type: 'STORE_ACTION_RESULT', action: 'BUY', success: false, error: 'Store item is unavailable or discontinued.' });
          break;
        }
        if (!orderId || orderId.length < 8) {
          safeSend(ws, { type: 'STORE_ACTION_RESULT', action: 'BUY', success: false, error: 'Missing order reference.' });
          break;
        }

        const nowMs = Date.now();
        pruneStoreOrderCache(nowMs);
        if (storeProcessedOrderIds.has(`sb_${orderId}`)) {
          safeSend(ws, { type: 'STORE_ACTION_RESULT', action: 'BUY', success: false, error: 'Duplicate purchase blocked by anti-cheat.' });
          safeSend(ws, buildStoreStatePayload(uid, integrity));
          safeSend(ws, buildWalletStatePayload(uid, integrity));
          break;
        }

        const lastAct = storeLastActionAt.get(uid) || 0;
        if (nowMs - lastAct < STORE_ACTION_COOLDOWN_MS) {
          safeSend(ws, { type: 'STORE_ACTION_RESULT', action: 'BUY', success: false, error: 'Please wait a moment before next purchase.' });
          break;
        }

        const price = Math.floor(itemSpec.basePrice7Days * durationMult);
        const curBal = getOrInitGreedyBalance(uid);
        if (curBal < price) {
          safeSend(ws, {
            type: 'STORE_ACTION_RESULT',
            action: 'BUY',
            success: false,
            insufficientCoins: true,
            requiredCoins: price,
            serverBalance: curBal,
            itemId,
            durationDays,
            error: `Insufficient Coins! Need ${price.toLocaleString('en-US')} Coins.`,
          });
          safeSend(ws, buildWalletStatePayload(uid, integrity));
          break;
        }

        storeProcessedOrderIds.set(`sb_${orderId}`, nowMs);
        storeLastActionAt.set(uid, nowMs);

        const newBal = curBal - price;
        greedyUserBalances.set(uid, newBal);

        const inv = pruneAndSyncUserStore(uid, nowMs);
        const existing = inv.items.get(itemId);
        const isRenewal = Boolean(existing);
        const baseStart = existing ? Math.max(Number(existing.expiresAtMs || 0), nowMs) : nowMs;
        const nextExpiresAt = baseStart + durationDays * STORE_DAY_MS;
        inv.items.set(itemId, {
          itemId,
          category: itemSpec.category,
          expiresAtMs: nextExpiresAt,
          totalDaysPurchased: (existing ? Number(existing.totalDaysPurchased || 0) : 0) + durationDays,
          purchasedAtMs: existing ? existing.purchasedAtMs : nowMs,
          giftedById: existing?.giftedById || '',
          giftedByName: existing?.giftedByName || '',
        });

        if (equipImmediately) {
          inv.equipped[itemSpec.category] = itemId;
        }
        pruneAndSyncUserStore(uid, nowMs);

        const actionLabel = isRenewal ? 'Store Renew' : 'Store Buy';
        recordWalletTx(
          uid,
          'COIN_USAGE',
          `${actionLabel}: ${itemSpec.name} (+${durationDays} Days)`,
          `-${price.toLocaleString('en-US')} Coins`,
          false,
          `sb_tx_${orderId}`
        );

        const expGained = Math.max(1, Math.floor(price / 5));
        grantUserExp(uid, expGained, 'STORE_BUY');
        firestoreStore.markUserDirty(uid);

        const activeRoomId = findUserActiveRoomId(uid);
        if (activeRoomId && rooms.has(activeRoomId)) {
          broadcastToRoom(rooms.get(activeRoomId), buildRoomSnapshot(rooms.get(activeRoomId)));
        }

        safeSend(ws, buildWalletStatePayload(uid, integrity));
        safeSend(ws, { type: 'WALLET_BALANCE_SYNC', serverBalance: newBal });
        safeSend(ws, buildStoreStatePayload(uid, integrity));
        safeSend(ws, {
          type: 'STORE_ACTION_RESULT',
          action: 'BUY',
          success: true,
          itemId,
          durationDays,
          totalCost: price,
          expGained,
          isRenewal,
          equippedNow: equipImmediately,
          serverBalance: newBal,
          message: isRenewal
            ? `${itemSpec.name} renewed for +${durationDays} Days!`
            : `${itemSpec.name} (${durationDays} Days) purchased!`,
        });
        break;
      }

      case 'STORE_GIFT_ITEM': {
        const uid = String(meta.userId || msg.userId || '');
        const integrity = msg.deviceIntegrity || null;
        if (!uid) break;
        if (meta.userId && msg.userId && String(msg.userId) !== meta.userId) break;
        if (!meta.userId) meta.userId = uid;
        addUserSocket(uid, ws);
        if (integrity) recordUserDeviceIntegrity(uid, integrity);

        const check = validateIntegrityForWallet(uid, integrity);
        if (!check.ok) {
          safeSend(ws, { type: 'STORE_ACTION_RESULT', action: 'GIFT', success: false, error: check.error });
          break;
        }

        const targetUserId = String(msg.targetUserId || '').trim();
        const targetName = String(msg.targetName || `User ${targetUserId}`).trim().slice(0, 48);
        const itemId = String(msg.itemId || '');
        const durationDays = Number(msg.durationDays);
        const orderId = String(msg.clientOrderId || '').trim();
        const itemSpec = STORE_CATALOG[itemId];
        const durationMult = STORE_DURATION_MULT[durationDays];

        if (!/^\d{6,10}$/.test(targetUserId)) {
          safeSend(ws, { type: 'STORE_ACTION_RESULT', action: 'GIFT', success: false, error: 'Enter a valid numeric Friend ID.' });
          break;
        }
        if (targetUserId === uid) {
          safeSend(ws, { type: 'STORE_ACTION_RESULT', action: 'GIFT', success: false, error: 'You cannot send a Store gift to yourself. Use Buy instead.' });
          break;
        }
        if (!itemSpec || itemSpec.active === false || !durationMult) {
          safeSend(ws, { type: 'STORE_ACTION_RESULT', action: 'GIFT', success: false, error: 'Store item is unavailable or discontinued.' });
          break;
        }
        if (!orderId || orderId.length < 8) {
          safeSend(ws, { type: 'STORE_ACTION_RESULT', action: 'GIFT', success: false, error: 'Missing order reference.' });
          break;
        }

        const nowMs = Date.now();
        pruneStoreOrderCache(nowMs);
        if (storeProcessedOrderIds.has(`sg_${orderId}`)) {
          safeSend(ws, { type: 'STORE_ACTION_RESULT', action: 'GIFT', success: false, error: 'Duplicate gift blocked by anti-cheat.' });
          safeSend(ws, buildWalletStatePayload(uid, integrity));
          break;
        }

        const lastAct = storeLastActionAt.get(uid) || 0;
        if (nowMs - lastAct < STORE_ACTION_COOLDOWN_MS) {
          safeSend(ws, { type: 'STORE_ACTION_RESULT', action: 'GIFT', success: false, error: 'Please wait a moment before next gift.' });
          break;
        }

        const price = Math.floor(itemSpec.basePrice7Days * durationMult);
        const curBal = getOrInitGreedyBalance(uid);
        if (curBal < price) {
          safeSend(ws, {
            type: 'STORE_ACTION_RESULT',
            action: 'GIFT',
            success: false,
            insufficientCoins: true,
            requiredCoins: price,
            serverBalance: curBal,
            itemId,
            durationDays,
            error: `Insufficient Coins! Need ${price.toLocaleString('en-US')} Coins.`,
          });
          safeSend(ws, buildWalletStatePayload(uid, integrity));
          break;
        }

        storeProcessedOrderIds.set(`sg_${orderId}`, nowMs);
        storeLastActionAt.set(uid, nowMs);

        const newBal = curBal - price;
        greedyUserBalances.set(uid, newBal);

        const senderName = users.get(uid)?.nickname || `User ${uid}`;
        const recipientInv = pruneAndSyncUserStore(targetUserId, nowMs);
        const existingTarget = recipientInv.items.get(itemId);
        const baseStart = existingTarget ? Math.max(Number(existingTarget.expiresAtMs || 0), nowMs) : nowMs;
        recipientInv.items.set(itemId, {
          itemId,
          category: itemSpec.category,
          expiresAtMs: baseStart + durationDays * STORE_DAY_MS,
          totalDaysPurchased: (existingTarget ? Number(existingTarget.totalDaysPurchased || 0) : 0) + durationDays,
          purchasedAtMs: existingTarget ? existingTarget.purchasedAtMs : nowMs,
          giftedById: uid,
          giftedByName: senderName,
        });
        pruneAndSyncUserStore(targetUserId, nowMs);

        const recipientProf = users.get(targetUserId);
        const resolvedRecipientName = recipientProf?.nickname || targetName;

        recordWalletTx(
          uid,
          'COIN_USAGE',
          `Store Gift to ${resolvedRecipientName} (${targetUserId}): ${itemSpec.name} (${durationDays} Days)`,
          `-${price.toLocaleString('en-US')} Coins`,
          false,
          `sg_tx_${orderId}`
        );

        const expGained = Math.max(1, Math.floor(price / 5));
        grantUserExp(uid, expGained, 'STORE_GIFT');
        firestoreStore.markUserDirty(uid);
        firestoreStore.markUserDirty(targetUserId);

        // Push real-time Store & My Items update to the recipient if online
        pushStoreSyncToUser(targetUserId);
        sendToUser(targetUserId, {
          type: 'STORE_GIFT_RECEIVED',
          itemId,
          itemName: itemSpec.name,
          durationDays,
          fromUserId: uid,
          fromName: senderName,
          message: `🎁 ${senderName} gifted you ${itemSpec.name} (${durationDays} Days) in My Items!`,
        });

        safeSend(ws, buildWalletStatePayload(uid, integrity));
        safeSend(ws, { type: 'WALLET_BALANCE_SYNC', serverBalance: newBal });
        safeSend(ws, buildStoreStatePayload(uid, integrity));
        safeSend(ws, {
          type: 'STORE_ACTION_RESULT',
          action: 'GIFT',
          success: true,
          itemId,
          durationDays,
          totalCost: price,
          expGained,
          recipientId: targetUserId,
          recipientName: `${resolvedRecipientName} (ID: ${targetUserId})`,
          serverBalance: newBal,
          message: `Sent ${itemSpec.name} (${durationDays} Days) to ${resolvedRecipientName}!`,
        });
        break;
      }

      case 'STORE_EQUIP_ITEM': {
        const uid = String(meta.userId || msg.userId || '');
        const integrity = msg.deviceIntegrity || null;
        if (!uid) break;
        if (meta.userId && msg.userId && String(msg.userId) !== meta.userId) break;
        if (!meta.userId) meta.userId = uid;
        addUserSocket(uid, ws);
        if (integrity) recordUserDeviceIntegrity(uid, integrity);

        const check = validateIntegrityForWallet(uid, integrity);
        if (!check.ok) {
          safeSend(ws, { type: 'STORE_ACTION_RESULT', action: 'EQUIP', success: false, error: check.error });
          break;
        }

        const itemId = String(msg.itemId || '');
        const equip = Boolean(msg.equip);
        const itemSpec = STORE_CATALOG[itemId];
        if (!itemSpec) {
          safeSend(ws, { type: 'STORE_ACTION_RESULT', action: 'EQUIP', success: false, error: 'Unknown store item.' });
          break;
        }

        const nowMs = Date.now();
        const inv = pruneAndSyncUserStore(uid, nowMs);
        if (equip) {
          const owned = inv.items.get(itemId);
          if (!owned || Number(owned.expiresAtMs || 0) <= nowMs) {
            safeSend(ws, {
              type: 'STORE_ACTION_RESULT',
              action: 'EQUIP',
              success: false,
              itemId,
              error: 'Item not owned or expired! Buy or renew it in Store first.',
            });
            safeSend(ws, buildStoreStatePayload(uid, integrity));
            break;
          }
          inv.equipped[itemSpec.category] = itemId;
        } else {
          if (inv.equipped[itemSpec.category] === itemId) {
            inv.equipped[itemSpec.category] = null;
          }
        }
        pruneAndSyncUserStore(uid, nowMs);
        firestoreStore.markUserDirty(uid);

        const activeRoomId = findUserActiveRoomId(uid);
        if (activeRoomId && rooms.has(activeRoomId)) {
          broadcastToRoom(rooms.get(activeRoomId), buildRoomSnapshot(rooms.get(activeRoomId)));
        }

        safeSend(ws, buildStoreStatePayload(uid, integrity));
        safeSend(ws, {
          type: 'STORE_ACTION_RESULT',
          action: 'EQUIP',
          success: true,
          itemId,
          category: itemSpec.category,
          equipped: inv.equipped[itemSpec.category] === itemId,
          message: inv.equipped[itemSpec.category] === itemId
            ? `${itemSpec.name} equipped!`
            : `${itemSpec.name} unequipped`,
        });
        break;
      }

      case 'GREEDY_OPEN': {
        greedySubscribers.add(ws);
        ensureGreedyDayFresh();
        const uid = String(meta.userId || msg.userId || '');
        if (uid && !meta.userId) meta.userId = uid;
        const { nowMs, roundNumber, elapsedMs } = getGreedyCurrentRoundInfo();
        pruneOldGreedyRounds(roundNumber);

        const serverBalance = uid ? getOrInitGreedyBalance(uid) : GREEDY_INITIAL_COINS;
        const roundBettors = greedyRoundBets.get(roundNumber);
        const myEntry = uid && roundBettors ? roundBettors.get(uid) : null;
        const userSlotBets = myEntry ? myEntry.slotBets : {};

        // Never leak the current round's outcome before DRAWING starts (elapsedMs >= 30000)!
        const latestCompletedForHistory = elapsedMs >= 35000 ? roundNumber : roundNumber - 1;
        const drawReveal = elapsedMs >= 30000
          ? buildGreedyPerUserOutcomePayload('GREEDY_DRAW_REVEAL', roundNumber, uid, msg.clientSentElapsedMs)
          : null;
        const settlement = elapsedMs >= 35000
          ? buildGreedyPerUserOutcomePayload('GREEDY_SETTLEMENT', roundNumber, uid, msg.clientSentElapsedMs)
          : null;

        const rankState = buildGreedyRankState(uid);
        safeSend(ws, {
          type: 'GREEDY_STATE_SYNC',
          serverNowMs: nowMs,
          clientSentElapsedMs: Number(msg.clientSentElapsedMs || 0),
          roundNumber,
          elapsedMs,
          serverBalance,
          slotTotals: computeGreedySlotTotals(roundNumber),
          userSlotBets,
          recentChips: (greedyRecentChips.get(roundNumber) || []).slice(-32),
          resultHistory: getGreedyResultHistory(latestCompletedForHistory),
          drawReveal,
          settlement,
          ...rankState,
        });
        break;
      }

      case 'GREEDY_GET_RANK': {
        const uid = String(meta.userId || msg.userId || '');
        if (uid && !meta.userId) meta.userId = uid;
        safeSend(ws, {
          type: 'GREEDY_RANK_SYNC',
          ...buildGreedyRankState(uid),
        });
        break;
      }

      case 'GREEDY_GET_RECORDS': {
        const uid = String(meta.userId || msg.userId || '');
        if (uid && !meta.userId) meta.userId = uid;
        safeSend(ws, {
          type: 'GREEDY_RECORDS_SYNC',
          records: getPrunedGreedyBetRecords(uid),
          serverNowMs: Date.now(),
        });
        break;
      }

      case 'GREEDY_CLOSE': {
        greedySubscribers.delete(ws);
        break;
      }

      case 'GREEDY_PLACE_BET': {
        const uid = String(meta.userId || msg.userId || '');
        if (!uid) break;
        if (meta.userId && msg.userId && String(msg.userId) !== meta.userId) {
          safeSend(ws, { type: 'GREEDY_BET_REJECTED', reason: 'Invalid user session' });
          break;
        }
        if (!meta.userId) meta.userId = uid;

        const { nowMs, roundNumber: curRound, elapsedMs } = getGreedyCurrentRoundInfo();
        const curBal = getOrInitGreedyBalance(uid);

        // Strict round & timing check: reject any bet once Betting Window locks at 29.5s!
        const reqRound = Number(msg.roundNumber);
        if (!Number.isFinite(reqRound) || reqRound !== curRound || elapsedMs >= GREEDY_BET_CUTOFF_MS) {
          safeSend(ws, {
            type: 'GREEDY_BET_REJECTED',
            reason: 'Betting closed for this round!',
            roundNumber: curRound,
            serverBalance: curBal,
            serverNowMs: nowMs,
          });
          break;
        }

        // Anti-spam rate limiting (allows rapid coalesced bet batches & hold-to-bet without false rejections)
        const prevBetAt = greedyLastBetAtMs.get(uid) || 0;
        if (nowMs - prevBetAt < 10) {
          safeSend(ws, {
            type: 'GREEDY_BET_REJECTED',
            reason: 'Too many bets! Slow down.',
            roundNumber: curRound,
            serverBalance: curBal,
            serverNowMs: nowMs,
          });
          break;
        }
        let roundCounts = greedyRoundBetCounts.get(curRound);
        if (!roundCounts) {
          roundCounts = new Map();
          greedyRoundBetCounts.set(curRound, roundCounts);
        }
        const curCount = roundCounts.get(uid) || 0;
        if (curCount >= GREEDY_MAX_BETS_PER_ROUND) {
          safeSend(ws, {
            type: 'GREEDY_BET_REJECTED',
            reason: 'Max bets per round reached!',
            roundNumber: curRound,
            serverBalance: curBal,
            serverNowMs: nowMs,
          });
          break;
        }

        // Validate every slot and chip denomination strictly (can batch multiple taps on up to 8 slots)
        const incomingBets = msg.betsBySlot || {};
        const entries = Object.entries(incomingBets);
        if (entries.length === 0 || entries.length > 8) {
          safeSend(ws, { type: 'GREEDY_BET_REJECTED', reason: 'Invalid bet slots', serverBalance: curBal });
          break;
        }

        const validBets = [];
        let totalCost = 0;
        let isInvalid = false;
        for (const [sKey, sAmtRaw] of entries) {
          const sId = Number(sKey);
          const sAmt = Number(sAmtRaw);
          if (!Number.isInteger(sId) || sId < 0 || sId > 7 || !Number.isInteger(sAmt) || sAmt < 100 || sAmt > 50000000 || sAmt % 100 !== 0) {
            isInvalid = true;
            break;
          }
          validBets.push([sId, sAmt]);
          totalCost += sAmt;
        }
        if (isInvalid || validBets.length === 0 || totalCost <= 0 || totalCost > 50000000) {
          safeSend(ws, { type: 'GREEDY_BET_REJECTED', reason: 'Invalid wager amount', serverBalance: curBal });
          break;
        }

        if (curBal < totalCost) {
          safeSend(ws, {
            type: 'GREEDY_BET_REJECTED',
            reason: 'Insufficient Gold Coins!',
            roundNumber: curRound,
            serverBalance: curBal,
            serverNowMs: nowMs,
          });
          break;
        }

        // Atomic balance deduction & record
        const newBal = curBal - totalCost;
        greedyUserBalances.set(uid, newBal);
        greedyLastBetAtMs.set(uid, nowMs);
        roundCounts.set(uid, curCount + 1);
        pruneOldGreedyRounds(curRound);

        let roundBettors = greedyRoundBets.get(curRound);
        if (!roundBettors) {
          roundBettors = new Map();
          greedyRoundBets.set(curRound, roundBettors);
        }
        const userProf = users.get(uid) || {};
        const incomingHttpAvatar = msg.avatarUri && String(msg.avatarUri).startsWith('http') ? String(msg.avatarUri) : '';
        const existing = roundBettors.get(uid) || {
          userId: uid,
          nickname: msg.nickname || userProf.nickname || `User ${uid}`,
          avatarUri: incomingHttpAvatar,
          slotBets: {},
          updatedAt: nowMs,
        };
        if (msg.nickname) existing.nickname = String(msg.nickname).slice(0, 32);
        if (incomingHttpAvatar) existing.avatarUri = incomingHttpAvatar;
        existing.updatedAt = nowMs;

        let cachedTotals = greedyRoundSlotTotals.get(curRound);
        if (!cachedTotals) {
          cachedTotals = computeGreedySlotTotals(curRound);
          greedyRoundSlotTotals.set(curRound, cachedTotals);
        }

        const betSlots = [];
        for (const [sId, sAmt] of validBets) {
          existing.slotBets[sId] = Number(existing.slotBets[sId] || 0) + sAmt;
          cachedTotals[sId] = Number(cachedTotals[sId] || 0) + sAmt;
          betSlots.push(sId);
        }
        roundBettors.set(uid, existing);

        let chipHist = greedyRecentChips.get(curRound);
        if (!chipHist) {
          chipHist = [];
          greedyRecentChips.set(curRound, chipHist);
        }
        const chipIdx = Math.max(0, Math.min(8, Number(msg.chipIndex || 1)));
        betSlots.forEach((sId) => {
          chipHist.push({ slotId: sId, chipIndex: chipIdx, userId: uid });
        });
        if (chipHist.length > 32) {
          chipHist.splice(0, chipHist.length - 32);
        }

        ensureGreedyDayFresh();
        const prevLb = greedyTodayLeaderboard.get(uid);
        const nextCoinsPlay = getGreedyEntryCoinsPlay(prevLb) + totalCost;
        greedyTodayLeaderboard.set(uid, {
          userId: uid,
          userName: existing.nickname || userProf.nickname || `User ${uid}`,
          avatarUri: incomingHttpAvatar || (prevLb && prevLb.avatarUri) || '',
          coinsPlay: nextCoinsPlay,
          winCoins: nextCoinsPlay,
        });
        firestoreStore.markGlobalDirty();

        const slotTotals = [...cachedTotals];
        safeSend(ws, {
          type: 'GREEDY_BET_ACK',
          roundNumber: curRound,
          userSlotBets: existing.slotBets,
          slotTotals,
          serverBalance: newBal,
          serverNowMs: nowMs,
          myTodayRank: computeMyGreedyRank(greedyTodayLeaderboard, uid),
          todayRankList: formatGreedyLeaderboard(greedyTodayLeaderboard, 15),
        });

        queueGreedyCrowdBetEvent(curRound, uid, betSlots, chipIdx, slotTotals, ws);
        break;
      }

      case 'GREEDY_GET_DRAW': {
        const uid = String(meta.userId || msg.userId || '');
        const reqRound = Number(msg.roundNumber);
        if (!Number.isFinite(reqRound)) break;
        const { roundNumber: curRound, elapsedMs } = getGreedyCurrentRoundInfo();
        // Anti-cheat: strictly refuse to reveal draw outcome before 30.0s (after betting is closed)!
        if (reqRound > curRound || (reqRound === curRound && elapsedMs < 30000) || reqRound < curRound - 2) {
          break;
        }
        safeSend(ws, buildGreedyPerUserOutcomePayload('GREEDY_DRAW_REVEAL', reqRound, uid, msg.clientSentElapsedMs));
        break;
      }

      case 'GREEDY_GET_SETTLEMENT': {
        const uid = String(meta.userId || msg.userId || '');
        const reqRound = Number(msg.roundNumber);
        if (!Number.isFinite(reqRound)) break;
        const { roundNumber: curRound, elapsedMs } = getGreedyCurrentRoundInfo();
        if (reqRound > curRound || (reqRound === curRound && elapsedMs < 35000) || reqRound < curRound - 2) {
          break;
        }
        safeSend(ws, buildGreedyPerUserOutcomePayload('GREEDY_SETTLEMENT', reqRound, uid, msg.clientSentElapsedMs));
        break;
      }

      case 'INVITE_SYNC': {
        const uid = String(msg.userId || meta.userId || '');
        if (!uid) break;
        if (!meta.userId) meta.userId = uid;
        addUserSocket(uid, ws);
        if (msg.deviceIntegrity) {
          recordUserDeviceIntegrity(uid, msg.deviceIntegrity);
        }
        evaluateInviteDay3Retention(uid, msg.deviceIntegrity || null);
        safeSend(ws, buildInviteStatePayload(uid, msg.deviceIntegrity || null));
        break;
      }

      case 'INVITE_LOOKUP': {
        const uid = String(msg.userId || meta.userId || '');
        const code = String(msg.code || '').trim();
        if (!/^\d{8}$/.test(code)) {
          safeSend(ws, {
            type: 'INVITE_LOOKUP_RESULT',
            code,
            found: false,
            error: 'Please enter a valid 8-digit invitation ID.',
          });
          break;
        }
        if (uid && code === uid) {
          safeSend(ws, {
            type: 'INVITE_LOOKUP_RESULT',
            code,
            found: false,
            error: 'You cannot bind your own invitation code.',
          });
          break;
        }
        const inviterUser = users.get(code);
        const inviterRoom = rooms.get(code);
        if (!inviterUser && !inviterRoom) {
          safeSend(ws, {
            type: 'INVITE_LOOKUP_RESULT',
            code,
            found: false,
            error: 'User ID not found. Ask your friend to open JustFun online first.',
          });
          break;
        }
        const targetLedger = getOrCreateInviteLedger(code);
        if (uid && targetLedger && targetLedger.boundInviterId === uid) {
          safeSend(ws, {
            type: 'INVITE_LOOKUP_RESULT',
            code,
            found: false,
            error: 'Mutual invite loops are blocked by anti-cheat.',
          });
          break;
        }
        const inviterLevel = Number(userLevels.get(code)?.level ?? inviterUser?.level ?? 0);
        safeSend(ws, {
          type: 'INVITE_LOOKUP_RESULT',
          code,
          found: true,
          inviter: {
            userId: code,
            nickname: inviterUser?.nickname || inviterRoom?.hostName || `User ${code}`,
            avatarUri: inviterUser?.avatarUri || inviterRoom?.hostAvatarUri || '',
            level: inviterLevel,
          },
        });
        break;
      }

      case 'INVITE_BIND_CODE': {
        const uid = String(msg.userId || meta.userId || '');
        const code = String(msg.code || '').trim();
        const integrity = msg.deviceIntegrity || null;
        if (!uid) break;
        if (!meta.userId) meta.userId = uid;
        addUserSocket(uid, ws);

        // Rate limit bind attempts (max 6 per 60s)
        const now = Date.now();
        const attempts = (inviteBindRateLimits.get(uid) || []).filter((t) => now - t < 60000);
        if (attempts.length >= 6) {
          safeSend(ws, {
            type: 'INVITE_BIND_RESULT',
            success: false,
            error: 'Too many attempts. Please wait a minute before trying again.',
          });
          break;
        }
        attempts.push(now);
        inviteBindRateLimits.set(uid, attempts);

        // 1. Validate format, self-invite & account existence
        if (!/^\d{8}$/.test(code)) {
          safeSend(ws, { type: 'INVITE_BIND_RESULT', success: false, error: 'Invalid 8-digit invitation ID.' });
          break;
        }
        if (code === uid) {
          safeSend(ws, { type: 'INVITE_BIND_RESULT', success: false, error: 'Self-invitation is blocked by anti-cheat!' });
          break;
        }
        const inviterUser = users.get(code);
        const inviterRoom = rooms.get(code);
        if (!inviterUser && !inviterRoom) {
          safeSend(ws, { type: 'INVITE_BIND_RESULT', success: false, error: 'Inviter account does not exist.' });
          break;
        }

        // 2. Non-Clone, Non-Emulator & Hardware Fingerprint Check
        const intCheck = validateIntegrityForInvite(integrity);
        if (!intCheck.ok) {
          safeSend(ws, { type: 'INVITE_BIND_RESULT', success: false, error: intCheck.error });
          break;
        }
        recordUserDeviceIntegrity(uid, integrity, meta.clientIp);

        // 3. Account-level already bound check
        const myLedger = getOrCreateInviteLedger(uid);
        if (myLedger.boundInviterId) {
          safeSend(ws, {
            type: 'INVITE_BIND_RESULT',
            success: false,
            error: `Already bound to ID ${myLedger.boundInviterId}. Only one bind allowed.`,
          });
          break;
        }

        // 4. Mutual invite loop check
        const inviterLedger = getOrCreateInviteLedger(code);
        if (inviterLedger.boundInviterId === uid) {
          safeSend(ws, {
            type: 'INVITE_BIND_RESULT',
            success: false,
            error: 'Mutual invite loops are blocked by anti-cheat.',
          });
          break;
        }

        // 5. Same-device & 1-device-lifetime-claim anti-fraud lock
        if (isDeviceAlreadyClaimedForKeys(intCheck.keys)) {
          safeSend(ws, {
            type: 'INVITE_BIND_RESULT',
            success: false,
            error: 'This physical device has already claimed an invitation reward!',
          });
          break;
        }
        if (shareSameDevice(uid, code, intCheck.keys, integrity, meta.clientIp)) {
          safeSend(ws, {
            type: 'INVITE_BIND_RESULT',
            success: false,
            error: 'Inviter and friend share the same device. Blocked by anti-cheat!',
          });
          break;
        }

        // All Anti-Cheat & Non-Clone Checks Passed!
        intCheck.keys.forEach((k) => {
          deviceInviteClaims.set(k, { firstUserId: uid, boundInviterId: code, boundAtMs: now });
        });

        myLedger.boundInviterId = code;
        myLedger.boundAtMs = now;

        const friendProfile = users.get(uid) || {};
        const joinDate = new Date(now).toISOString().slice(0, 10);
        inviterLedger.invitedFriends.set(uid, {
          userId: uid,
          nickname: friendProfile.nickname || `User ${uid}`,
          avatarUri: friendProfile.avatarUri || '',
          day1Credited: true,
          day3Credited: false,
          hasRecharged: false,
          rechargeCoins: 0,
          hasSentGifts: false,
          giftCoinsReward: 0,
          coinsRewarded: INVITE_DAY1_COINS,
          joinDate,
          boundAtMs: now,
        });
        inviterLedger.withdrawableCoins += INVITE_DAY1_COINS;
        inviterLedger.totalCoinsRewarded += INVITE_DAY1_COINS;

        // Credit Friend's Wallet (+250,000 Coins welcome reward)
        const curBal = getOrInitGreedyBalance(uid);
        const newBal = Math.min(GREEDY_MAX_BALANCE, curBal + INVITE_FRIEND_WELCOME_COINS);
        greedyUserBalances.set(uid, newBal);
        recordWalletTx(uid, 'COIN_USAGE', 'Invite Friend Welcome Bonus', `+${INVITE_FRIEND_WELCOME_COINS.toLocaleString('en-US')} Coins`, true);
        safeSend(ws, buildWalletStatePayload(uid, integrity));
        safeSend(ws, { type: 'WALLET_BALANCE_SYNC', serverBalance: newBal });

        const inviterName = inviterUser?.nickname || inviterRoom?.hostName || `User ${code}`;
        pushInviteTicker(inviterName, INVITE_DAY1_COINS, 'FRIEND_JOIN');
        saveInviteSecurityLedgerToDisk([uid, code], intCheck.keys);

        safeSend(ws, {
          type: 'INVITE_BIND_RESULT',
          success: true,
          boundInviterId: code,
          boundInviterName: inviterName,
          welcomeCoins: INVITE_FRIEND_WELCOME_COINS,
          message: `Verified & bound to ${inviterName}! +250,000 Coins credited to your Wallet.`,
        });
        safeSend(ws, buildInviteStatePayload(uid, integrity));
        pushInviteSyncToUser(code);
        break;
      }

      case 'INVITE_WITHDRAW': {
        const uid = String(msg.userId || meta.userId || '');
        const integrity = msg.deviceIntegrity || null;
        if (!uid) break;
        if (!meta.userId) meta.userId = uid;

        if (integrity && (integrity.isClonedEnvironment || Number(integrity.userProfileSlot || 0) !== 0 || integrity.uidMatchesPackage === false || integrity.isKernelPathClean === false)) {
          safeSend(ws, {
            type: 'INVITE_WITHDRAW_RESULT',
            success: false,
            error: 'Clone / Dual-App detected. Withdrawals are disabled in cloned environments.',
          });
          break;
        }

        const ledger = getOrCreateInviteLedger(uid);
        const amount = Math.max(0, Math.floor(ledger.withdrawableCoins));
        if (amount <= 0) {
          safeSend(ws, {
            type: 'INVITE_WITHDRAW_RESULT',
            success: false,
            error: 'No withdrawable invitation coins available yet.',
          });
          break;
        }

        ledger.withdrawableCoins = 0;
        const curBal = getOrInitGreedyBalance(uid);
        const newBal = Math.min(GREEDY_MAX_BALANCE, curBal + amount);
        greedyUserBalances.set(uid, newBal);
        recordWalletTx(uid, 'COIN_USAGE', 'Invite Friends Reward', `+${amount.toLocaleString('en-US')} Coins`, true);
        firestoreStore.markUserDirty(uid);

        safeSend(ws, buildWalletStatePayload(uid, integrity));
        safeSend(ws, { type: 'WALLET_BALANCE_SYNC', serverBalance: newBal });
        safeSend(ws, {
          type: 'INVITE_WITHDRAW_RESULT',
          success: true,
          withdrawnAmount: amount,
          serverBalance: newBal,
        });
        safeSend(ws, buildInviteStatePayload(uid, integrity));
        break;
      }

      default: {
        if (profileAndMedals && typeof profileAndMedals.handleSocketMessage === 'function') {
          profileAndMedals.handleSocketMessage(ws, msg, meta);
        }
        break;
      }
    }
  });

  const scheduleGraceDisconnect = () => {
    homeSubscribers.delete(ws);
    greedySubscribers.delete(ws);
    if (meta.userId) {
      removeUserSocket(meta.userId, ws);
    }
    if (meta.roomId && meta.userId) {
      const targetRoomId = meta.roomId;
      const targetUserId = meta.userId;
      const timerKey = `${targetRoomId}:${targetUserId}`;
      if (pendingDisconnectTimers.has(timerKey)) {
        clearTimeout(pendingDisconnectTimers.get(timerKey));
      }
      const t = setTimeout(() => {
        pendingDisconnectTimers.delete(timerKey);
        removeUserFromRoom(ws, targetRoomId, targetUserId);
      }, 2500);
      pendingDisconnectTimers.set(timerKey, t);
    }
  };

  ws.on('close', scheduleGraceDisconnect);
  ws.on('error', scheduleGraceDisconnect);
});

firestoreStore.bindStateRefs({
  users,
  greedyUserBalances,
  greedyBetRecords,
  userDiamonds,
  userLevels,
  walletTransactions,
  walletDailyCheckIns,
  userStoreInventories,
  userInviteLedgers,
  userFollowers,
  userFollowing,
  userVisitors,
  blockedUsers,
  userDmPrivacy,
  userSystemNotices,
  userFollowAlerts,
  userKnownDevices,
  userHardwareSignatures,
  userLastIps,
  rooms,
  getOrCreateRoom,
  deviceAccountSlots,
  deviceInviteClaims,
  deviceDailyCheckIns,
  bannedDeviceKeys,
  dmThreads,
  DYNAMIC_GIFTS_CATALOG,
  STORE_CATALOG,
  DYNAMIC_MEDALS_CATALOG,
  DYNAMIC_EVENTS_CATALOG,
  bannedUserIds,
  getGiftsCustomizedByAdmin: () => giftsCustomizedByAdmin,
  getStoreCustomizedByAdmin: () => storeCustomizedByAdmin,
  getGreedyDayIndex: () => greedyDayIndex,
  getGreedyTodayJackpotPool: () => greedyTodayJackpotPool,
  getGreedyYesterdayJackpotPool: () => greedyYesterdayJackpotPool,
  getGreedyTodayLeaderboard: () => greedyTodayLeaderboard,
  getGreedyYesterdayLeaderboard: () => greedyYesterdayLeaderboard,
  getAdminRules: () => ({
    maxUnfollowedDmMessages: MAX_UNFOLLOWED_DM_MESSAGES,
    maxAccountsPerDevice: MAX_ACCOUNTS_PER_DEVICE,
    inviteDay1Coins: INVITE_DAY1_COINS,
    inviteDay3Coins: INVITE_DAY3_COINS,
    inviteFriendWelcomeCoins: INVITE_FRIEND_WELCOME_COINS,
    dailyCheckInCoins: DAILY_CHECKIN_REWARD_COINS,
  }),
  applyHydratedGlobalState: (doc) => {
    if (!doc || typeof doc !== 'object') return;
    if (typeof doc.greedyDayIndex === 'number') {
      greedyDayIndex = doc.greedyDayIndex;
    }
    if (typeof doc.greedyTodayJackpotPool === 'number') {
      greedyTodayJackpotPool = Math.max(0, Math.floor(doc.greedyTodayJackpotPool));
    }
    if (typeof doc.greedyYesterdayJackpotPool === 'number') {
      greedyYesterdayJackpotPool = Math.max(0, Math.floor(doc.greedyYesterdayJackpotPool));
    }
    if (Array.isArray(doc.greedyTodayLeaderboard)) {
      greedyTodayLeaderboard = new Map();
      doc.greedyTodayLeaderboard.forEach((e) => {
        if (e && e.userId) {
          const cp = getGreedyEntryCoinsPlay(e);
          greedyTodayLeaderboard.set(e.userId, { ...e, coinsPlay: cp, winCoins: cp });
        }
      });
    }
    if (Array.isArray(doc.greedyYesterdayLeaderboard)) {
      greedyYesterdayLeaderboard = new Map();
      doc.greedyYesterdayLeaderboard.forEach((e) => {
        if (e && e.userId) {
          const cp = getGreedyEntryCoinsPlay(e);
          greedyYesterdayLeaderboard.set(e.userId, { ...e, coinsPlay: cp, winCoins: cp });
        }
      });
    }
    ensureGreedyDayFresh();
    if (typeof doc.giftsCustomizedByAdmin === 'boolean') {
      giftsCustomizedByAdmin = doc.giftsCustomizedByAdmin;
    }
    if (Array.isArray(doc.dynamicGifts)) {
      DYNAMIC_GIFTS_CATALOG.clear();
      doc.dynamicGifts.forEach((g) => {
        if (!g || !g.id) return;
        const isLegacyBuiltIn = LEGACY_BUILTIN_GIFT_IDS.has(String(g.id)) && !String(g.animationUrl || '').trim();
        if (!isLegacyBuiltIn) {
          DYNAMIC_GIFTS_CATALOG.set(g.id, g);
        }
      });
    }
    if (typeof doc.storeCustomizedByAdmin === 'boolean') {
      storeCustomizedByAdmin = doc.storeCustomizedByAdmin;
    }
    if (Array.isArray(doc.storeCatalog)) {
      if (storeCustomizedByAdmin) {
        Object.keys(STORE_CATALOG).forEach((k) => delete STORE_CATALOG[k]);
      }
      doc.storeCatalog.forEach((item) => {
        if (item && item.id) STORE_CATALOG[item.id] = item;
      });
    }
    if (Array.isArray(doc.dynamicMedals)) {
      DYNAMIC_MEDALS_CATALOG.clear();
      doc.dynamicMedals.forEach((m) => {
        if (m && m.id) DYNAMIC_MEDALS_CATALOG.set(m.id, m);
      });
    }
    if (Array.isArray(doc.dynamicEvents)) {
      doc.dynamicEvents.forEach((ev) => {
        if (ev && ev.id) DYNAMIC_EVENTS_CATALOG.set(ev.id, ev);
      });
    }
    if (Array.isArray(doc.bannedUserIds)) {
      doc.bannedUserIds.forEach((entry) => {
        if (Array.isArray(entry) && entry[0]) bannedUserIds.set(entry[0], entry[1]);
      });
    }
    if (doc.rules && typeof doc.rules === 'object') {
      if (typeof doc.rules.maxUnfollowedDmMessages === 'number') {
        MAX_UNFOLLOWED_DM_MESSAGES = Math.max(1, doc.rules.maxUnfollowedDmMessages);
      }
      if (typeof doc.rules.maxAccountsPerDevice === 'number') {
        MAX_ACCOUNTS_PER_DEVICE = Math.max(1, doc.rules.maxAccountsPerDevice);
      }
      if (typeof doc.rules.inviteDay1Coins === 'number') INVITE_DAY1_COINS = doc.rules.inviteDay1Coins;
      if (typeof doc.rules.inviteDay3Coins === 'number') INVITE_DAY3_COINS = doc.rules.inviteDay3Coins;
      if (typeof doc.rules.inviteFriendWelcomeCoins === 'number') {
        INVITE_FRIEND_WELCOME_COINS = doc.rules.inviteFriendWelcomeCoins;
      }
      if (typeof doc.rules.dailyCheckInCoins === 'number') DAILY_CHECKIN_REWARD_COINS = doc.rules.dailyCheckInCoins;
    }
  },
  applyHydratedGiftsCollection: (giftDocs) => {
    if (!Array.isArray(giftDocs) || giftDocs.length === 0) return;
    giftDocs.forEach((doc) => {
      if (!doc || !doc.id || !doc.name) return;
      const cleanId = String(doc.id).trim();
      const videoUrl = String(doc.videoUrl || '').trim();
      const isLegacyBuiltIn = LEGACY_BUILTIN_GIFT_IDS.has(cleanId) && !videoUrl;
      if (isLegacyBuiltIn) return;
      const existing = DYNAMIC_GIFTS_CATALOG.get(cleanId) || {};
      DYNAMIC_GIFTS_CATALOG.set(cleanId, {
        id: cleanId,
        name: String(doc.name).trim(),
        icon: String(doc.iconUrl || existing.icon || '🎁').trim() || '🎁',
        priceCoins: Math.max(1, Math.floor(Number(doc.price || existing.priceCoins || 10))),
        tag: String(doc.tag || existing.tag || (String(doc.type || '').toLowerCase() === 'mp4' ? 'MP4' : 'HOT')).trim(),
        category: String(doc.category || existing.category || 'Gifts').trim() || 'Gifts',
        animationUrl: videoUrl || existing.animationUrl || '',
        durationSeconds: Math.max(1, Number(doc.durationSeconds || existing.durationSeconds || (videoUrl ? 5.5 : 3.0))),
        type: String(doc.type || existing.type || (videoUrl ? 'mp4' : 'standard')).trim(),
        active: doc.active !== false,
      });
    });
    giftsCustomizedByAdmin = true;
  },
});

async function startServer() {
  try {
    await firestoreStore.hydrateFromCloud();
    if (profileAndMedals && typeof profileAndMedals.ensureDefaultMedalsSeeded === 'function') {
      profileAndMedals.ensureDefaultMedalsSeeded();
    }
    // Rebuild reverse device->user index after hydration
    userKnownDevices.forEach((keys, uid) => {
      keys.forEach((k) => {
        let dUsers = deviceUserAccounts.get(k);
        if (!dUsers) {
          dUsers = new Set();
          deviceUserAccounts.set(k, dUsers);
        }
        dUsers.add(uid);
      });
    });
  } catch (err) {
    console.warn('Startup hydration warning:', err.message);
  }
  server.listen(PORT, () => {
    console.log(`JustFun Real-Time Voice & Social Backend running on port ${PORT}`);
  });
}

async function gracefulShutdown(signal) {
  console.log(`Received ${signal}, flushing state before exit...`);
  try {
    await firestoreStore.flushDirtyState();
  } catch (_) {}
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

startServer();
