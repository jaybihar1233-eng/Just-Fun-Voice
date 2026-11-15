const express = require('express');
const http = require('http');
const cors = require('cors');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

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
// reports: Array<ReportRecord>
const reports = [];

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
const GREEDY_MAX_BETS_PER_ROUND = 40;
const WALLET_RECHARGE_PACKS = {
  p1: 800000,
  p2: 3549000,
  p3: 7099000,
  p4: 17290000,
};
const WALLET_EXCHANGE_COINS = new Set([330, 3300, 33000, 330000, 3300000]);
const GREEDY_HMAC_SECRET = process.env.GREEDY_HMAC_SECRET || crypto.randomBytes(32).toString('hex');

const greedySubscribers = new Set(); // Only sockets actively viewing Greedy Baby
const greedyRoundBets = new Map();   // Map<roundNumber, Map<userId, { userId, nickname, avatarUri, slotBets, updatedAt }>>
const greedyRecentChips = new Map(); // Map<roundNumber, Array<{ slotId, chipIndex, userId }>>
const greedySettlements = new Map(); // Map<roundNumber, CachedSettlement>
const greedyUserBalances = new Map(); // Map<userId, number> (Server-authoritative unified Wallet + Game coins)
const greedyRoundBetCounts = new Map(); // Map<roundNumber, Map<userId, number>>
const greedyLastBetAtMs = new Map(); // Map<userId, number>
const greedyLastRewardAtMs = new Map(); // Map<userId, number>
let greedyDayIndex = Math.floor(Date.now() / 86400000);
let greedyTodayLeaderboard = new Map(); // Map<userId, { userId, userName, avatarUri, winCoins }>
let greedyYesterdayLeaderboard = new Map();

function ensureGreedyDayFresh() {
  const curDay = Math.floor(Date.now() / 86400000);
  if (curDay !== greedyDayIndex) {
    if (curDay === greedyDayIndex + 1) {
      greedyYesterdayLeaderboard = greedyTodayLeaderboard;
    } else {
      greedyYesterdayLeaderboard = new Map();
    }
    greedyTodayLeaderboard = new Map();
    greedyDayIndex = curDay;
  }
}

function getOrInitGreedyBalance(userId, clientBalanceHint = null) {
  if (!userId) return GREEDY_INITIAL_COINS;
  const parsedHint = Number(clientBalanceHint);
  const hasValidHint = Number.isFinite(parsedHint) && parsedHint >= 0 && parsedHint <= GREEDY_MAX_BALANCE;
  if (!greedyUserBalances.has(userId)) {
    const startBal = hasValidHint ? Math.floor(parsedHint) : GREEDY_INITIAL_COINS;
    greedyUserBalances.set(userId, startBal);
    return startBal;
  }
  const curBal = greedyUserBalances.get(userId);
  // Only allow passive clientBalanceHint to adjust DOWNWARD; upward wallet changes must use WALLET_* handlers.
  if (hasValidHint && Math.floor(parsedHint) < curBal) {
    greedyUserBalances.set(userId, Math.floor(parsedHint));
    return Math.floor(parsedHint);
  }
  return curBal;
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
  const totals = [0, 0, 0, 0, 0, 0, 0, 0];
  const bettors = greedyRoundBets.get(roundNumber);
  if (!bettors) return totals;
  for (const b of bettors.values()) {
    for (let s = 0; s < 8; s++) {
      totals[s] += Number(b.slotBets[s] || 0);
    }
  }
  return totals;
}

function formatGreedyLeaderboard(map) {
  return Array.from(map.values())
    .filter((e) => Number(e.winCoins || 0) > 0)
    .sort((a, b) => Number(b.winCoins || 0) - Number(a.winCoins || 0))
    .slice(0, 20)
    .map((e, idx) => ({
      rank: idx + 1,
      userId: e.userId,
      userName: e.userName || `User ${e.userId}`,
      avatarUri: e.avatarUri || '',
      winCoins: Number(e.winCoins || 0),
    }));
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

  if (bettors) {
    for (const b of bettors.values()) {
      let betTotal = 0;
      for (let s = 0; s < 8; s++) {
        betTotal += Number(b.slotBets[s] || 0);
      }
      userBetTotalsById[b.userId] = betTotal;

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
        const avatar = b.avatarUri || userProf.avatarUri || '';
        allWinners.push({
          userId: b.userId,
          userName: name,
          avatarUri: avatar,
          winAmount,
        });
        const prevLb = greedyTodayLeaderboard.get(b.userId);
        greedyTodayLeaderboard.set(b.userId, {
          userId: b.userId,
          userName: name,
          avatarUri: avatar,
          winCoins: Number(prevLb?.winCoins || 0) + winAmount,
        });
      }
    }
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
    todayRankList: formatGreedyLeaderboard(greedyTodayLeaderboard),
    yesterdayRankList: formatGreedyLeaderboard(greedyYesterdayLeaderboard),
    completedHistory: getGreedyResultHistory(roundNumber),
  };
  greedySettlements.set(roundNumber, cached);
  return cached;
}

function buildGreedyPerUserOutcomePayload(type, roundNumber, userId, clientSentElapsedMs = 0) {
  const cached = computeAndCacheGreedySettlement(roundNumber);
  return {
    type,
    roundNumber: cached.roundNumber,
    winningFoodId: cached.winningFoodId,
    topWinners: cached.topWinners,
    userBetTotal: Number(cached.userBetTotalsById[userId] || 0),
    userWinAmount: Number(cached.userWinAmountsById[userId] || 0),
    serverBalance: userId ? getOrInitGreedyBalance(userId) : GREEDY_INITIAL_COINS,
    todayRankList: cached.todayRankList,
    yesterdayRankList: cached.yesterdayRankList,
    completedHistory: cached.completedHistory,
    serverNowMs: Date.now(),
    clientSentElapsedMs: Number(clientSentElapsedMs || 0),
  };
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
    unlockedMedalIds: Array.isArray(u.unlockedMedalIds) ? u.unlockedMedalIds : [],
    receivedGifts: Array.isArray(u.receivedGifts) ? u.receivedGifts : [],
    photos: Array.isArray(u.photos) ? u.photos.slice(0, 6) : [],
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
    const peerProfile = users.get(peerId) || {};
    const unreadKey = `${uid}:${peerId}`;
    const unreadCount = dmUnreadCounts.get(unreadKey) || 0;
    totalUnreadDm += unreadCount;
    const lastMsg = messages[messages.length - 1];
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
    myThreads.push({
      peerUserId: peerId,
      peerName: peerProfile.nickname || lastMsg?.fromName || lastMsg?.toName || `User ${peerId}`,
      peerAvatarUri: peerProfile.avatarUri || '',
      unreadCount,
      lastMessageText: previewText,
      lastMessageAtMs: lastMsg ? lastMsg.createdAtMs : 0,
      messages: messages.slice(-80),
    });
  }
  myThreads.sort((a, b) => (b.lastMessageAtMs || 0) - (a.lastMessageAtMs || 0));

  const myBlockedMap = blockedUsers.get(uid);
  const blockedList = myBlockedMap ? Array.from(myBlockedMap.values()) : [];

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
//   2. Rocket Launch (`ROCKET_LAUNCHED`): Top 1-3 win 15x Coin Reward as EXP
//   3. Room Stay (`ROOM_STAY_CLAIM`): 10 mins = 1,000 EXP
//      - Own Room Max: 10,000 EXP/day
//      - Other Room Max: 20,000 EXP/day
// ============================================================================
const LEVEL_CUMULATIVE_EXP = {
  0: 0,
  1: 5000,
  2: 15000,
  3: 30000,
  4: 50000,
  5: 80000,
  6: 120000,
  7: 170000,
  8: 230000,
  9: 300000,
  10: 400000,
  11: 520000,
  12: 660000,
  13: 820000,
  14: 1000000,
  15: 1200000,
  16: 1420000,
  17: 1660000,
  18: 1920000,
  19: 2200000,
  20: 2500000,
  21: 2820000,
  22: 3160000,
  23: 3520000,
  24: 3900000,
  25: 4300000,
  26: 4600000,
  27: 4902282,
  28: 5363700,
  29: 6500000,
  30: 8000000,
  31: 12000000,
  32: 17000000,
  33: 24000000,
  34: 32000000,
  35: 42000000,
  36: 55000000,
  37: 70000000,
  38: 88000000,
  39: 110000000,
  40: 135000000,
  41: 162000000,
  42: 192000000,
  43: 224000000,
  44: 258000000,
  45: 290000000,
  46: 317066904,
  47: 330256800,
  48: 350000000,
  49: 375000000,
  50: 400000000,
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
    sendToUser(uid, { type: 'WALLET_BALANCE_SYNC', serverBalance: newBal });
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

  return {
    newLevel: st.level,
    newExp: st.exp,
    leveledUp: st.level > prevLevel,
    milestoneCoinsAdded,
  };
}

const ROCKET_TARGETS = {
  1: 2500000,
  2: 5000000,
  3: 10000000,
};

function getNextDailyResetMs() {
  const dayMs = 24 * 60 * 60 * 1000;
  return (Math.floor(Date.now() / dayMs) + 1) * dayMs;
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
  }
}

function buildRocketStatePayload(room) {
  ensureRocketDailyFresh(room);
  const rs = room.rocketState;
  const lvl = Math.min(3, Math.max(1, Number(rs.level || 1)));
  const contribs = Array.from(rs.contributors.values())
    .sort((a, b) => Number(b.coins || 0) - Number(a.coins || 0))
    .slice(0, 20);
  return {
    roomId: room.id,
    level: lvl,
    currentExp: Number(rs.currentExp || 0),
    targetExp: ROCKET_TARGETS[lvl] || ROCKET_TARGETS[1],
    allLaunched: Boolean(rs.allLaunched),
    resetAtMs: Number(rs.resetAtMs || getNextDailyResetMs()),
    contributors: contribs,
    history: Array.isArray(rs.history) ? rs.history.slice(0, 20) : [],
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

  for (let i = 0; i < Math.min(3, sortedContribs.length); i++) {
    const c = sortedContribs[i];
    if (!c || !c.userId) continue;
    const amt = topRewards[i];
    const expWon = amt * 15; // Matches Rocket Reward Card ("15 times EXP")
    rewardsByUser[c.userId] = amt;
    expByUser[c.userId] = expWon;
    winners.push({
      userId: c.userId,
      nickname: c.nickname || `User ${c.userId}`,
      avatarUri: c.avatarUri || '',
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

function broadcastLiveRoomsToHome() {
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

wss.on('connection', (ws) => {
  const meta = { userId: '', roomId: '', missedPings: 0 };
  clientMeta.set(ws, meta);
  homeSubscribers.add(ws);

  // Immediately push live rooms list + authoritative server clock on connect
  safeSend(ws, { type: 'LIVE_ROOMS_UPDATE', serverNowMs: Date.now(), rooms: getLiveRoomsSummary() });

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

      case 'REGISTER_USER': {
        const profile = msg.profile;
        if (!profile || !profile.userId) break;
        const uid = String(profile.userId);
        meta.userId = uid;
        addUserSocket(uid, ws);

        const existing = users.get(uid) || {};
        const srvLevelState = userLevels.get(uid);
        const merged = {
          ...existing,
          ...profile,
          userId: uid,
          user8DigitId: uid,
          level: srvLevelState ? srvLevelState.level : Number(profile.level ?? existing.level ?? 0),
          // Keep server-authoritative charm points if higher than client cache
          totalCharmPoints: Math.max(Number(existing.totalCharmPoints || 0), Number(profile.totalCharmPoints || 0)),
          updatedAt: Date.now(),
        };
        users.set(uid, merged);
        safeSend(ws, { type: 'USER_REGISTERED', profile: merged });
        safeSend(ws, buildSocialSnapshotForUser(uid));
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

        pushSocialSyncToUser(fromUserId);
        pushSocialSyncToUser(targetUserId);

        const followerProfile = users.get(fromUserId) || {};
        sendToUser(targetUserId, {
          type: 'FOLLOW_NOTIFICATION',
          fromUserId,
          fromName: followerProfile.nickname || `User ${fromUserId}`,
          fromAvatarUri: followerProfile.avatarUri || '',
        });
        break;
      }

      case 'UNFOLLOW_USER': {
        const fromUserId = String(msg.fromUserId || meta.userId || '');
        const targetUserId = String(msg.targetUserId || '');
        if (!fromUserId || !targetUserId) break;

        userFollowing.get(fromUserId)?.delete(targetUserId);
        userFollowers.get(targetUserId)?.delete(fromUserId);

        pushSocialSyncToUser(fromUserId);
        pushSocialSyncToUser(targetUserId);
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

      case 'SEND_DM': {
        const fromUserId = String(msg.fromUserId || meta.userId || '');
        const toUserId = String(msg.toUserId || '');
        const text = String(msg.text || '');
        const msgType = String(msg.msgType || 'TEXT');
        const hasMedia = Boolean(msg.audioBase64 || msg.imageBase64 || msg.giftName);
        if (!fromUserId || !toUserId || (!text.trim() && !hasMedia)) break;

        // Enforce real block & DM privacy rules
        const receiverBlocked = blockedUsers.get(toUserId);
        const senderBlocked = blockedUsers.get(fromUserId);
        if ((receiverBlocked && receiverBlocked.has(fromUserId)) || (senderBlocked && senderBlocked.has(toUserId))) {
          safeSend(ws, {
            type: 'DM_REJECTED',
            peerUserId: toUserId,
            reason: 'BLOCKED',
          });
          break;
        }
        if (userDmPrivacy.get(toUserId) === 'FOLLOWING' && !(userFollowing.get(toUserId)?.has(fromUserId))) {
          safeSend(ws, {
            type: 'DM_REJECTED',
            peerUserId: toUserId,
            reason: 'FOLLOWING_ONLY',
          });
          break;
        }

        const fromProfile = users.get(fromUserId) || {};
        const toProfile = users.get(toUserId) || {};
        const fromName = msg.fromName || fromProfile.nickname || `User ${fromUserId}`;
        const fromAvatarUri = msg.fromAvatarUri || fromProfile.avatarUri || '';
        const toName = msg.toName || toProfile.nickname || `User ${toUserId}`;
        const toAvatarUri = msg.toAvatarUri || toProfile.avatarUri || '';

        const threadKey = getThreadKey(fromUserId, toUserId);
        let thread = dmThreads.get(threadKey);
        if (!thread) {
          thread = [];
          dmThreads.set(threadKey, thread);
        }

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
        };
        thread.push(dmItem);
        if (thread.length > 150) {
          thread = thread.slice(-100);
          dmThreads.set(threadKey, thread);
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
        dmUnreadCounts.set(`${userId}:${peerUserId}`, 0);
        dmThreads.delete(getThreadKey(userId, peerUserId));
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
        pushSocialSyncToUser(fromUserId);
        break;
      }

      case 'UNBLOCK_USER': {
        const fromUserId = String(msg.fromUserId || meta.userId || '');
        const targetUserId = String(msg.targetUserId || '');
        if (!fromUserId || !targetUserId) break;
        blockedUsers.get(fromUserId)?.delete(targetUserId);
        pushSocialSyncToUser(fromUserId);
        break;
      }

      case 'SET_DM_PRIVACY': {
        const uid = String(msg.userId || meta.userId || '');
        if (!uid) break;
        userDmPrivacy.set(uid, msg.mode === 'FOLLOWING' ? 'FOLLOWING' : 'ALL');
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
        const thread = dmThreads.get(getThreadKey(userId, peerUserId));
        if (thread) {
          for (const item of thread) {
            if (item.id === msgId) {
              item.isRecalled = true;
            }
          }
        }
        pushSocialSyncToUser(userId);
        pushSocialSyncToUser(peerUserId);
        break;
      }

      case 'MARK_DM_READ': {
        const userId = String(msg.userId || meta.userId || '');
        const peerUserId = String(msg.peerUserId || '');
        if (!userId || !peerUserId) break;
        dmUnreadCounts.set(`${userId}:${peerUserId}`, 0);
        pushSocialSyncToUser(userId);
        break;
      }

      case 'SEND_GIFT_TX': {
        const fromUserId = String(msg.fromUserId || meta.userId || '');
        const targetUserId = String(msg.targetUserId || '');
        const roomId = String(msg.roomId || '');
        const charmDelta = Number(msg.charmDelta || 0);
        const coinsSpent = Number(msg.coinsSpent || charmDelta);
        if (targetUserId && charmDelta > 0) {
          const existing = users.get(targetUserId) || { userId: targetUserId, user8DigitId: targetUserId };
          existing.totalCharmPoints = Number(existing.totalCharmPoints || 0) + charmDelta;
          if (msg.giftName) {
            const gifts = Array.isArray(existing.receivedGifts) ? [...existing.receivedGifts] : [];
            const gIdx = gifts.findIndex((g) => g.name === msg.giftName);
            const gCount = Number(msg.giftCount || 1);
            if (gIdx >= 0) {
              gifts[gIdx] = {
                ...gifts[gIdx],
                count: Number(gifts[gIdx].count || 0) + gCount,
                charmValue: Number(gifts[gIdx].charmValue || 0) + charmDelta,
              };
            } else {
              gifts.push({
                id: msg.giftId || `g_${Date.now()}`,
                name: msg.giftName,
                icon: msg.giftIcon || '🎁',
                count: gCount,
                charmValue: charmDelta,
              });
            }
            existing.receivedGifts = gifts.slice(0, 30);
          }
          users.set(targetUserId, existing);
          pushSocialSyncToUser(targetUserId);
        }
        if (fromUserId && coinsSpent > 0) {
          const giftSendExp = Math.max(1, Math.floor(coinsSpent / 5));
          grantUserExp(fromUserId, giftSendExp, 'GIFT_SEND');
        }
        if (roomId && coinsSpent > 0) {
          const room = rooms.get(roomId);
          if (room) {
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
                const topGifter = sortedContribs[0] || {
                  userId: fromUserId,
                  nickname: contribEntry.nickname,
                  avatarUri: contribEntry.avatarUri,
                  coins: coinsSpent,
                };

                const { rewardsByUser, expByUser, winners, totalPoolCoins } = computeRocketLaunchRewards(lvl, sortedContribs, room);
                const launchId = `rocket_${room.id}_L${lvl}_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
                const launchedAtMs = Date.now();

                rs.history.unshift({
                  launchId,
                  level: lvl,
                  topGifterId: topGifter.userId,
                  topGifterName: topGifter.nickname || 'Top Gifter',
                  topGifterAvatarUri: topGifter.avatarUri || '',
                  totalPoolCoins,
                  launchedAtMs,
                });
                rs.history = rs.history.slice(0, 30);

                // Credit Rocket 15x EXP to Top 1-3 contributors authoritatively
                for (const [winnerUid, expAmt] of Object.entries(expByUser)) {
                  if (Number(expAmt) > 0) {
                    grantUserExp(winnerUid, Number(expAmt), `ROCKET_L${lvl}`);
                  }
                }

                broadcastToRoom(room, {
                  type: 'ROCKET_LAUNCHED',
                  roomId: room.id,
                  launchId,
                  level: lvl,
                  topGifterId: topGifter.userId,
                  topGifterName: topGifter.nickname || 'Top Gifter',
                  topGifterAvatarUri: topGifter.avatarUri || '',
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

            broadcastToRoom(room, {
              type: 'ROCKET_STATE_UPDATE',
              rocketState: buildRocketStatePayload(room),
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
        const chatItem = {
          ...message,
          id: message.id || `msg_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
          createdAtMs: message.createdAtMs || Date.now(),
        };
        room.messages.push(chatItem);
        if (room.messages.length > 80) {
          room.messages = room.messages.slice(-60);
        }
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
        room.chatClearedAt = clearedAtMs || Date.now();
        room.messages = [];
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
        const serverBalance = getOrInitGreedyBalance(uid, msg.clientBalance);
        safeSend(ws, { type: 'WALLET_BALANCE_SYNC', serverBalance });
        break;
      }

      case 'WALLET_CREDIT_PACK': {
        const uid = String(meta.userId || msg.userId || '');
        if (!uid) break;
        if (meta.userId && msg.userId && String(msg.userId) !== meta.userId) break;
        if (!meta.userId) meta.userId = uid;
        const packCoins = WALLET_RECHARGE_PACKS[String(msg.packId || '')];
        if (!packCoins) break;
        const curBal = getOrInitGreedyBalance(uid);
        const newBal = Math.min(GREEDY_MAX_BALANCE, curBal + packCoins);
        greedyUserBalances.set(uid, newBal);
        safeSend(ws, { type: 'WALLET_BALANCE_SYNC', serverBalance: newBal });
        break;
      }

      case 'WALLET_EXCHANGE_DIAMONDS': {
        const uid = String(meta.userId || msg.userId || '');
        if (!uid) break;
        if (meta.userId && msg.userId && String(msg.userId) !== meta.userId) break;
        if (!meta.userId) meta.userId = uid;
        const exCoins = Number(msg.coins);
        if (!Number.isInteger(exCoins) || !WALLET_EXCHANGE_COINS.has(exCoins)) break;
        const curBal = getOrInitGreedyBalance(uid);
        const newBal = Math.min(GREEDY_MAX_BALANCE, curBal + exCoins);
        greedyUserBalances.set(uid, newBal);
        safeSend(ws, { type: 'WALLET_BALANCE_SYNC', serverBalance: newBal });
        break;
      }

      case 'WALLET_ADD_REWARD': {
        const uid = String(meta.userId || msg.userId || '');
        if (!uid) break;
        if (meta.userId && msg.userId && String(msg.userId) !== meta.userId) break;
        if (!meta.userId) meta.userId = uid;
        const amt = Number(msg.amount);
        if (!Number.isInteger(amt) || amt <= 0 || amt > 15000000) break;
        const now = Date.now();
        const prevAt = greedyLastRewardAtMs.get(uid) || 0;
        if (now - prevAt < 250) break;
        greedyLastRewardAtMs.set(uid, now);
        const curBal = getOrInitGreedyBalance(uid);
        const newBal = Math.min(GREEDY_MAX_BALANCE, curBal + amt);
        greedyUserBalances.set(uid, newBal);
        safeSend(ws, { type: 'WALLET_BALANCE_SYNC', serverBalance: newBal });
        break;
      }

      case 'WALLET_SPEND': {
        const uid = String(meta.userId || msg.userId || '');
        if (!uid) break;
        if (meta.userId && msg.userId && String(msg.userId) !== meta.userId) break;
        if (!meta.userId) meta.userId = uid;
        const amt = Number(msg.amount);
        if (!Number.isInteger(amt) || amt <= 0) break;
        const curBal = getOrInitGreedyBalance(uid);
        const newBal = Math.max(0, curBal - amt);
        greedyUserBalances.set(uid, newBal);
        safeSend(ws, { type: 'WALLET_BALANCE_SYNC', serverBalance: newBal });
        break;
      }

      case 'GREEDY_OPEN': {
        greedySubscribers.add(ws);
        ensureGreedyDayFresh();
        const uid = String(meta.userId || msg.userId || '');
        if (uid && !meta.userId) meta.userId = uid;
        const { nowMs, roundNumber, elapsedMs } = getGreedyCurrentRoundInfo();
        pruneOldGreedyRounds(roundNumber);

        const serverBalance = uid ? getOrInitGreedyBalance(uid, msg.clientBalanceHint) : GREEDY_INITIAL_COINS;
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
          todayRankList: formatGreedyLeaderboard(greedyTodayLeaderboard),
          yesterdayRankList: formatGreedyLeaderboard(greedyYesterdayLeaderboard),
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
        const curBal = getOrInitGreedyBalance(uid, msg.clientBalanceHint);

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

        // Anti-spam rate limiting
        const prevBetAt = greedyLastBetAtMs.get(uid) || 0;
        if (nowMs - prevBetAt < 40) {
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

        // Validate every slot and chip denomination strictly
        const incomingBets = msg.betsBySlot || {};
        const entries = Object.entries(incomingBets);
        if (entries.length === 0 || entries.length > 4) {
          safeSend(ws, { type: 'GREEDY_BET_REJECTED', reason: 'Invalid bet slots', serverBalance: curBal });
          break;
        }

        const validBets = [];
        let totalCost = 0;
        let isInvalid = false;
        for (const [sKey, sAmtRaw] of entries) {
          const sId = Number(sKey);
          const sAmt = Number(sAmtRaw);
          if (!Number.isInteger(sId) || sId < 0 || sId > 7 || !Number.isInteger(sAmt) || !GREEDY_ALLOWED_CHIPS.has(sAmt)) {
            isInvalid = true;
            break;
          }
          validBets.push([sId, sAmt]);
          totalCost += sAmt;
        }
        if (isInvalid || validBets.length === 0 || totalCost <= 0 || totalCost > 20000000) {
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
        const existing = roundBettors.get(uid) || {
          userId: uid,
          nickname: msg.nickname || userProf.nickname || `User ${uid}`,
          avatarUri: msg.avatarUri || userProf.avatarUri || '',
          slotBets: {},
          updatedAt: nowMs,
        };
        if (msg.nickname) existing.nickname = String(msg.nickname).slice(0, 32);
        if (msg.avatarUri) existing.avatarUri = String(msg.avatarUri);
        existing.updatedAt = nowMs;

        const betSlots = [];
        for (const [sId, sAmt] of validBets) {
          existing.slotBets[sId] = Number(existing.slotBets[sId] || 0) + sAmt;
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
        if (chipHist.length > 48) {
          chipHist.splice(0, chipHist.length - 48);
        }

        const slotTotals = computeGreedySlotTotals(curRound);
        safeSend(ws, {
          type: 'GREEDY_BET_ACK',
          roundNumber: curRound,
          userSlotBets: existing.slotBets,
          slotTotals,
          serverBalance: newBal,
          serverNowMs: nowMs,
        });

        broadcastToGreedySubscribers({
          type: 'GREEDY_BET_EVENT',
          serverNowMs: nowMs,
          roundNumber: curRound,
          userId: uid,
          slotIds: betSlots,
          chipIndex: chipIdx,
          slotTotals,
        }, ws);
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

server.listen(PORT, () => {
  console.log(`JustFun Real-Time Voice & Social Backend running on port ${PORT}`);
});
