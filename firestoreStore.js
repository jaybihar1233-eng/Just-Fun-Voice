const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID || 'gen-lang-client-0479270201';
const FIRESTORE_DATABASE_ID =
  process.env.FIRESTORE_DATABASE_ID || 'ai-studio-android-justfun-265e983a-5bb7-41e8-ad50-4de63df92ee4';
const FIREBASE_STORAGE_BUCKET =
  process.env.FIREBASE_STORAGE_BUCKET || 'gen-lang-client-0479270201.firebasestorage.app';
const LOCAL_SNAPSHOT_FILE = path.join(__dirname, '.justfun_runtime_state.json');

let db = null;
let firestoreEnabled = false;
let firestoreInitError = '';
let stateRefs = null;

const dirtyUsers = new Set();
const dirtyRooms = new Set();
const dirtyDevices = new Set();
const dirtyDms = new Set();
let dirtyGlobal = false;

let flushTimer = null;
let isFlushing = false;

function encodeDocId(rawKey) {
  return Buffer.from(String(rawKey || ''), 'utf8').toString('base64url').slice(0, 450) || '_';
}

function capString(str, maxLen = 180000) {
  if (typeof str !== 'string') return '';
  return str.length > maxLen ? str.slice(0, maxLen) : str;
}

function loadServiceAccountCredential() {
  const inlineRaw = (process.env.FIREBASE_SERVICE_ACCOUNT_JSON || '').trim();
  if (inlineRaw) {
    try {
      const jsonStr = inlineRaw.startsWith('{')
        ? inlineRaw
        : Buffer.from(inlineRaw, 'base64').toString('utf8');
      return JSON.parse(jsonStr);
    } catch (e) {
      firestoreInitError = `Invalid FIREBASE_SERVICE_ACCOUNT_JSON: ${e.message}`;
      return null;
    }
  }

  const candidatePaths = [
    process.env.FIREBASE_SERVICE_ACCOUNT_PATH,
    process.env.GOOGLE_APPLICATION_CREDENTIALS,
    path.join(__dirname, '.firebase-service-account.json'),
  ].filter(Boolean);

  for (const p of candidatePaths) {
    try {
      if (fs.existsSync(p)) {
        return JSON.parse(fs.readFileSync(p, 'utf8'));
      }
    } catch (e) {
      firestoreInitError = `Failed reading service account file ${p}: ${e.message}`;
    }
  }
  return null;
}

function initFirestoreClient() {
  if (db) return db;
  try {
    const adminApp = require('firebase-admin/app');
    const adminFirestore = require('firebase-admin/firestore');
    const serviceAccount = loadServiceAccountCredential();
    const isOnGcp = Boolean(process.env.K_SERVICE || process.env.GOOGLE_CLOUD_PROJECT);

    if (!serviceAccount && !isOnGcp) {
      firestoreEnabled = false;
      firestoreInitError =
        firestoreInitError ||
        'FIREBASE_SERVICE_ACCOUNT_JSON not configured yet (running with non-blocking async snapshot fallback).';
      console.log(`[FirestoreStore] Standby: ${firestoreInitError}`);
      return null;
    }

    const app = adminApp.getApps().length
      ? adminApp.getApp()
      : adminApp.initializeApp({
          credential: serviceAccount
            ? adminApp.cert(serviceAccount)
            : adminApp.applicationDefault(),
          projectId: serviceAccount?.project_id || FIREBASE_PROJECT_ID,
          storageBucket: FIREBASE_STORAGE_BUCKET,
        });

    db = adminFirestore.getFirestore(app, FIRESTORE_DATABASE_ID);
    try {
      db.settings({ ignoreUndefinedProperties: true });
    } catch (_) {}
    firestoreEnabled = true;
    firestoreInitError = '';
    console.log(
      `[FirestoreStore] Connected to Firestore (${FIREBASE_PROJECT_ID} / db: ${FIRESTORE_DATABASE_ID})`
    );
    return db;
  } catch (err) {
    firestoreEnabled = false;
    firestoreInitError = err.message || String(err);
    console.warn(`[FirestoreStore] Init failed, using async snapshot fallback: ${firestoreInitError}`);
    return null;
  }
}

function bindStateRefs(refs) {
  stateRefs = refs;
  initFirestoreClient();
}

function markUserDirty(uid) {
  if (!uid) return;
  dirtyUsers.add(String(uid));
  scheduleFlush();
}

function markRoomDirty(roomId) {
  if (!roomId) return;
  dirtyRooms.add(String(roomId));
  scheduleFlush();
}

function markDeviceDirty(deviceKey) {
  if (!deviceKey) return;
  dirtyDevices.add(String(deviceKey));
  scheduleFlush();
}

function markDmDirty(threadKey) {
  if (!threadKey) return;
  dirtyDms.add(String(threadKey));
  scheduleFlush();
}

function markGlobalDirty() {
  dirtyGlobal = true;
  scheduleFlush();
}

function scheduleFlush(delayMs = 2500) {
  if (flushTimer) return;
  flushTimer = setTimeout(() => {
    flushTimer = null;
    flushDirtyState().catch((e) => {
      console.warn('[FirestoreStore] Background flush error:', e.message);
    });
  }, delayMs);
}

function serializeUserDoc(uid) {
  if (!stateRefs) return null;
  const r = stateRefs;
  const prof = r.users.get(uid) || { userId: uid, user8DigitId: uid };
  const sanitizedProfile = {
    ...prof,
    userId: uid,
    user8DigitId: prof.user8DigitId || uid,
    avatarUri: capString(prof.avatarUri || '', 180000),
    photos: Array.isArray(prof.photos) ? prof.photos.slice(0, 4).map((p) => capString(p, 90000)) : [],
  };

  const lv = r.userLevels.get(uid);
  const levelState = lv
    ? {
        exp: Number(lv.exp || 0),
        level: Number(lv.level || 0),
        creditedCoinLevels: Array.from(lv.creditedCoinLevels || []),
        dayKey: Number(lv.dayKey || 0),
        dailyOwnRoomStayExp: Number(lv.dailyOwnRoomStayExp || 0),
        dailyOtherRoomStayExp: Number(lv.dailyOtherRoomStayExp || 0),
      }
    : null;

  const inv = r.userStoreInventories.get(uid);
  const storeInventory = inv
    ? {
        items: Array.from(inv.items.entries()).map(([id, entry]) => ({ ...entry, itemId: id })),
        equipped: { ...inv.equipped },
      }
    : null;

  const invite = r.userInviteLedgers.get(uid);
  const inviteLedger = invite
    ? {
        boundInviterId: invite.boundInviterId || null,
        boundAtMs: Number(invite.boundAtMs || 0),
        day3Credited: Boolean(invite.day3Credited),
        friendRechargedTotal: Number(invite.friendRechargedTotal || 0),
        rechargeBonusCredited: Boolean(invite.rechargeBonusCredited),
        giftRebateTotal: Number(invite.giftRebateTotal || 0),
        withdrawableCoins: Number(invite.withdrawableCoins || 0),
        totalCoinsRewarded: Number(invite.totalCoinsRewarded || 0),
        invitedFriends: Array.from((invite.invitedFriends || new Map()).entries()),
      }
    : null;

  const visitors = (r.userVisitors.get(uid) || []).slice(0, 30).map((v) => ({
    ...v,
    avatarUri: v.avatarUri && String(v.avatarUri).startsWith('http') ? v.avatarUri : '',
  }));

  const blockedMap = r.blockedUsers.get(uid);
  const blocked = blockedMap
    ? Array.from(blockedMap.values()).map((b) => ({
        ...b,
        avatarUri: b.avatarUri && String(b.avatarUri).startsWith('http') ? b.avatarUri : '',
      }))
    : [];

  return {
    userId: uid,
    profile: sanitizedProfile,
    coinsBalance: Number(r.greedyUserBalances.get(uid) ?? 0),
    diamondsBalance: Number(r.userDiamonds.get(uid) ?? 0),
    levelState,
    walletTransactions: (r.walletTransactions.get(uid) || []).slice(0, 40),
    dailyCheckInDate: r.walletDailyCheckIns.get(uid) || '',
    storeInventory,
    inviteLedger,
    followers: Array.from(r.userFollowers.get(uid) || []),
    following: Array.from(r.userFollowing.get(uid) || []),
    visitors,
    blocked,
    dmPrivacy: r.userDmPrivacy.get(uid) || 'ALL',
    systemNotices: (r.userSystemNotices.get(uid) || []).slice(0, 20),
    followAlerts: (r.userFollowAlerts.get(uid) || []).slice(0, 20),
    knownDevices: Array.from(r.userKnownDevices.get(uid) || []).slice(-10),
    hwSigs: Array.from(r.userHardwareSignatures.get(uid) || []).slice(-10),
    lastIps: Array.from(r.userLastIps.get(uid) || []).slice(-6),
    updatedAtMs: Date.now(),
  };
}

function serializeRoomDoc(roomId) {
  if (!stateRefs) return null;
  const room = stateRefs.rooms.get(roomId);
  if (!room) return null;
  const rs = room.rocketState || {};
  const formatPeriod = (m) =>
    m
      ? Array.from(m.values())
          .sort((a, b) => Number(b.coins || 0) - Number(a.coins || 0))
          .slice(0, 40)
          .map((e) => ({
            ...e,
            avatarUri: e.avatarUri && String(e.avatarUri).startsWith('http') ? e.avatarUri : '',
          }))
      : [];

  return {
    id: room.id,
    title: room.title,
    announcement: room.announcement,
    hostId: room.hostId,
    hostName: room.hostName,
    hostAvatarUri: capString(room.hostAvatarUri || '', 160000),
    themeDrawableResId: Number(room.themeDrawableResId || 0),
    isAdminChatOnly: Boolean(room.isAdminChatOnly),
    chatClearedAt: Number(room.chatClearedAt || 0),
    roomPassword: String(room.roomPassword || ''),
    roomTotalCharm: Number(room.roomTotalCharm || 0),
    categoryTag: room.categoryTag || 'Hot Party',
    countryName: room.countryName || 'India',
    adminUsers: (room.adminUsers || []).slice(0, 30).map((a) => ({
      ...a,
      avatarUri: a.avatarUri && String(a.avatarUri).startsWith('http') ? a.avatarUri : '',
    })),
    bannedChatUserIds: (room.bannedChatUserIds || []).slice(0, 100),
    bannedUsers: (room.bannedUsers || []).slice(0, 100).map((b) => ({
      ...b,
      avatarUri: b.avatarUri && String(b.avatarUri).startsWith('http') ? b.avatarUri : '',
    })),
    contributions: {
      daily: formatPeriod(room.roomContributions?.daily),
      weekly: formatPeriod(room.roomContributions?.weekly),
      monthly: formatPeriod(room.roomContributions?.monthly),
    },
    rocketState: {
      level: Number(rs.level || 1),
      currentExp: Number(rs.currentExp || 0),
      allLaunched: Boolean(rs.allLaunched),
      resetAtMs: Number(rs.resetAtMs || 0),
      contributors: rs.contributors
        ? Array.from(rs.contributors.values())
            .sort((a, b) => Number(b.coins || 0) - Number(a.coins || 0))
            .slice(0, 40)
            .map((c) => ({
              ...c,
              avatarUri: c.avatarUri && String(c.avatarUri).startsWith('http') ? c.avatarUri : '',
            }))
        : [],
      history: Array.isArray(rs.history)
        ? rs.history.slice(0, 20).map((h) => ({
            ...h,
            topGifterAvatarUri:
              h.topGifterAvatarUri && String(h.topGifterAvatarUri).startsWith('http')
                ? h.topGifterAvatarUri
                : '',
          }))
        : [],
    },
    messages: (room.messages || []).slice(-40).map((m) => ({
      ...m,
      avatarUri: m.avatarUri && String(m.avatarUri).startsWith('http') ? m.avatarUri : '',
    })),
    updatedAtMs: Date.now(),
  };
}

function serializeDeviceDoc(devKey) {
  if (!stateRefs) return null;
  const r = stateRefs;
  const slotMap = r.deviceAccountSlots.get(devKey);
  const accountSlots = slotMap
    ? Array.from(slotMap.values()).map((s) => ({
        ...s,
        avatarUri: capString(s.avatarUri || '', 120000),
      }))
    : [];
  const inviteClaim = r.deviceInviteClaims.get(devKey) || null;
  const dailyCheckInDate = r.deviceDailyCheckIns.get(devKey) || '';
  const banned = r.bannedDeviceKeys.get(devKey) || null;
  return {
    deviceKey: devKey,
    accountSlots,
    inviteClaim,
    dailyCheckInDate,
    banned,
    updatedAtMs: Date.now(),
  };
}

function serializeDmDoc(threadKey) {
  if (!stateRefs) return null;
  const thread = stateRefs.dmThreads.get(threadKey) || [];
  return {
    threadKey,
    messages: thread.slice(-50).map((m) => ({
      ...m,
      fromAvatarUri: m.fromAvatarUri && String(m.fromAvatarUri).startsWith('http') ? m.fromAvatarUri : '',
      toAvatarUri: m.toAvatarUri && String(m.toAvatarUri).startsWith('http') ? m.toAvatarUri : '',
      imageBase64: capString(m.imageBase64 || '', 120000),
      audioBase64: capString(m.audioBase64 || '', 120000),
    })),
    updatedAtMs: Date.now(),
  };
}

function serializeGlobalDoc() {
  if (!stateRefs) return null;
  const r = stateRefs;
  const stripLb = (map) =>
    Array.from(map.values())
      .filter((e) => Number(e.winCoins || 0) > 0)
      .sort((a, b) => Number(b.winCoins || 0) - Number(a.winCoins || 0))
      .slice(0, 30)
      .map((e) => ({
        userId: e.userId,
        userName: e.userName,
        winCoins: Number(e.winCoins || 0),
        avatarUri: e.avatarUri && String(e.avatarUri).startsWith('http') ? e.avatarUri : '',
      }));

  return {
    greedyDayIndex: Number(r.getGreedyDayIndex()),
    greedyTodayLeaderboard: stripLb(r.getGreedyTodayLeaderboard()),
    greedyYesterdayLeaderboard: stripLb(r.getGreedyYesterdayLeaderboard()),
    dynamicGifts: Array.from(r.DYNAMIC_GIFTS_CATALOG.values()),
    storeCatalog: Object.values(r.STORE_CATALOG || {}),
    giftsCustomizedByAdmin: Boolean(r.getGiftsCustomizedByAdmin ? r.getGiftsCustomizedByAdmin() : false),
    storeCustomizedByAdmin: Boolean(r.getStoreCustomizedByAdmin ? r.getStoreCustomizedByAdmin() : false),
    dynamicMedals: Array.from(r.DYNAMIC_MEDALS_CATALOG.values()),
    dynamicEvents: Array.from(r.DYNAMIC_EVENTS_CATALOG.values()),
    bannedUserIds: Array.from(r.bannedUserIds.entries()),
    rules: r.getAdminRules(),
    updatedAtMs: Date.now(),
  };
}

async function writeLocalAsyncSnapshot() {
  if (!stateRefs) return;
  try {
    const r = stateRefs;
    const out = {
      users: Array.from(r.users.keys()).map((uid) => serializeUserDoc(uid)).filter(Boolean),
      rooms: Array.from(r.rooms.keys()).map((rid) => serializeRoomDoc(rid)).filter(Boolean),
      devices: Array.from(
        new Set([
          ...r.deviceAccountSlots.keys(),
          ...r.deviceInviteClaims.keys(),
          ...r.deviceDailyCheckIns.keys(),
          ...r.bannedDeviceKeys.keys(),
        ])
      )
        .map((dk) => serializeDeviceDoc(dk))
        .filter(Boolean),
      dmThreads: Array.from(r.dmThreads.keys()).map((tk) => serializeDmDoc(tk)).filter(Boolean),
      globalState: serializeGlobalDoc(),
      savedAtMs: Date.now(),
    };
    const tmp = `${LOCAL_SNAPSHOT_FILE}.tmp`;
    await fs.promises.writeFile(tmp, JSON.stringify(out), 'utf8');
    await fs.promises.rename(tmp, LOCAL_SNAPSHOT_FILE);
  } catch (err) {
    console.warn('[FirestoreStore] Async local snapshot warning:', err.message);
  }
}

async function flushDirtyState() {
  if (!stateRefs || isFlushing) return;
  if (!dirtyUsers.size && !dirtyRooms.size && !dirtyDevices.size && !dirtyDms.size && !dirtyGlobal) {
    return;
  }
  isFlushing = true;
  try {
    const ops = [];

    const userBatchKeys = Array.from(dirtyUsers);
    dirtyUsers.clear();
    for (const uid of userBatchKeys) {
      const data = serializeUserDoc(uid);
      if (data && db) ops.push({ ref: db.collection('users').doc(encodeDocId(uid)), data });
    }

    const roomBatchKeys = Array.from(dirtyRooms);
    dirtyRooms.clear();
    for (const rid of roomBatchKeys) {
      const data = serializeRoomDoc(rid);
      if (data && db) ops.push({ ref: db.collection('rooms').doc(encodeDocId(rid)), data });
    }

    const devBatchKeys = Array.from(dirtyDevices);
    dirtyDevices.clear();
    for (const dk of devBatchKeys) {
      const data = serializeDeviceDoc(dk);
      if (data && db) ops.push({ ref: db.collection('device_locks').doc(encodeDocId(dk)), data });
    }

    const dmBatchKeys = Array.from(dirtyDms);
    dirtyDms.clear();
    for (const tk of dmBatchKeys) {
      const data = serializeDmDoc(tk);
      if (data && db) ops.push({ ref: db.collection('dm_threads').doc(encodeDocId(tk)), data });
    }

    if (dirtyGlobal) {
      dirtyGlobal = false;
      const data = serializeGlobalDoc();
      if (data && db) ops.push({ ref: db.collection('app_config').doc('global_state'), data });
    }

    if (db && firestoreEnabled && ops.length > 0) {
      for (let i = 0; i < ops.length; i += 350) {
        const chunk = ops.slice(i, i + 350);
        const batch = db.batch();
        for (const op of chunk) {
          batch.set(op.ref, op.data, { merge: true });
        }
        await batch.commit();
      }
    } else {
      await writeLocalAsyncSnapshot();
    }
  } catch (err) {
    console.warn('[FirestoreStore] Flush failed (keeping async local snapshot):', err.message);
    await writeLocalAsyncSnapshot();
  } finally {
    isFlushing = false;
  }
}

function applyHydratedUser(doc) {
  if (!doc || !doc.userId || !stateRefs) return;
  const r = stateRefs;
  const uid = String(doc.userId);
  if (doc.profile && !r.users.has(uid)) {
    r.users.set(uid, doc.profile);
  }
  if (typeof doc.coinsBalance === 'number' && !r.greedyUserBalances.has(uid)) {
    r.greedyUserBalances.set(uid, Math.max(0, Math.floor(doc.coinsBalance)));
  }
  if (typeof doc.diamondsBalance === 'number' && !r.userDiamonds.has(uid)) {
    r.userDiamonds.set(uid, Math.max(0, Math.floor(doc.diamondsBalance)));
  }
  if (doc.levelState && !r.userLevels.has(uid)) {
    r.userLevels.set(uid, {
      exp: Number(doc.levelState.exp || 0),
      level: Number(doc.levelState.level || 0),
      creditedCoinLevels: new Set(Array.isArray(doc.levelState.creditedCoinLevels) ? doc.levelState.creditedCoinLevels : []),
      dayKey: Number(doc.levelState.dayKey || 0),
      dailyOwnRoomStayExp: Number(doc.levelState.dailyOwnRoomStayExp || 0),
      dailyOtherRoomStayExp: Number(doc.levelState.dailyOtherRoomStayExp || 0),
    });
  }
  if (Array.isArray(doc.walletTransactions) && !r.walletTransactions.has(uid)) {
    r.walletTransactions.set(uid, doc.walletTransactions);
  }
  if (doc.dailyCheckInDate && !r.walletDailyCheckIns.has(uid)) {
    r.walletDailyCheckIns.set(uid, doc.dailyCheckInDate);
  }
  if (doc.storeInventory && !r.userStoreInventories.has(uid)) {
    const itemsMap = new Map();
    (doc.storeInventory.items || []).forEach((item) => {
      if (item && item.itemId) itemsMap.set(item.itemId, item);
    });
    r.userStoreInventories.set(uid, {
      items: itemsMap,
      equipped: doc.storeInventory.equipped || {
        VEHICLE: null,
        FRAME: null,
        BUBBLE: null,
        MIC_WAVE: null,
        SPECIAL_ID: null,
      },
    });
  }
  if (doc.inviteLedger && !r.userInviteLedgers.has(uid)) {
    r.userInviteLedgers.set(uid, {
      boundInviterId: doc.inviteLedger.boundInviterId || null,
      boundAtMs: Number(doc.inviteLedger.boundAtMs || 0),
      day3Credited: Boolean(doc.inviteLedger.day3Credited),
      friendRechargedTotal: Number(doc.inviteLedger.friendRechargedTotal || 0),
      rechargeBonusCredited: Boolean(doc.inviteLedger.rechargeBonusCredited),
      giftRebateTotal: Number(doc.inviteLedger.giftRebateTotal || 0),
      withdrawableCoins: Number(doc.inviteLedger.withdrawableCoins || 0),
      totalCoinsRewarded: Number(doc.inviteLedger.totalCoinsRewarded || 0),
      invitedFriends: new Map(
        Array.isArray(doc.inviteLedger.invitedFriends) ? doc.inviteLedger.invitedFriends : []
      ),
    });
  }
  if (Array.isArray(doc.followers) && doc.followers.length && !r.userFollowers.has(uid)) {
    r.userFollowers.set(uid, new Set(doc.followers));
  }
  if (Array.isArray(doc.following) && doc.following.length && !r.userFollowing.has(uid)) {
    r.userFollowing.set(uid, new Set(doc.following));
  }
  if (Array.isArray(doc.visitors) && doc.visitors.length && !r.userVisitors.has(uid)) {
    r.userVisitors.set(uid, doc.visitors);
  }
  if (Array.isArray(doc.blocked) && doc.blocked.length && !r.blockedUsers.has(uid)) {
    const m = new Map();
    doc.blocked.forEach((b) => {
      if (b && b.userId) m.set(b.userId, b);
    });
    r.blockedUsers.set(uid, m);
  }
  if (doc.dmPrivacy && !r.userDmPrivacy.has(uid)) {
    r.userDmPrivacy.set(uid, doc.dmPrivacy);
  }
  if (Array.isArray(doc.systemNotices) && !r.userSystemNotices.has(uid)) {
    r.userSystemNotices.set(uid, doc.systemNotices);
  }
  if (Array.isArray(doc.followAlerts) && !r.userFollowAlerts.has(uid)) {
    r.userFollowAlerts.set(uid, doc.followAlerts);
  }
  if (Array.isArray(doc.knownDevices) && !r.userKnownDevices.has(uid)) {
    r.userKnownDevices.set(uid, new Set(doc.knownDevices));
  }
  if (Array.isArray(doc.hwSigs) && !r.userHardwareSignatures.has(uid)) {
    r.userHardwareSignatures.set(uid, new Set(doc.hwSigs));
  }
  if (Array.isArray(doc.lastIps) && !r.userLastIps.has(uid)) {
    r.userLastIps.set(uid, new Set(doc.lastIps));
  }
}

function applyHydratedRoom(doc) {
  if (!doc || !doc.id || !stateRefs) return;
  const r = stateRefs;
  const rid = String(doc.id);
  if (r.rooms.has(rid)) return;
  const room = r.getOrCreateRoom(rid, {
    title: doc.title,
    announcement: doc.announcement,
    hostId: doc.hostId,
    hostName: doc.hostName,
    hostAvatarUri: doc.hostAvatarUri,
    themeDrawableResId: doc.themeDrawableResId,
    roomPassword: doc.roomPassword,
    categoryTag: doc.categoryTag,
    countryName: doc.countryName,
  });
  room.isAdminChatOnly = Boolean(doc.isAdminChatOnly);
  room.chatClearedAt = Number(doc.chatClearedAt || 0);
  room.roomTotalCharm = Number(doc.roomTotalCharm || 0);
  if (Array.isArray(doc.adminUsers)) room.adminUsers = doc.adminUsers;
  if (Array.isArray(doc.bannedChatUserIds)) room.bannedChatUserIds = doc.bannedChatUserIds;
  if (Array.isArray(doc.bannedUsers)) room.bannedUsers = doc.bannedUsers;
  if (Array.isArray(doc.messages)) room.messages = doc.messages;
  if (doc.contributions) {
    ['daily', 'weekly', 'monthly'].forEach((p) => {
      (doc.contributions[p] || []).forEach((entry) => {
        if (entry && entry.userId) room.roomContributions[p].set(entry.userId, entry);
      });
    });
  }
  if (doc.rocketState) {
    room.rocketState.level = Number(doc.rocketState.level || 1);
    room.rocketState.currentExp = Number(doc.rocketState.currentExp || 0);
    room.rocketState.allLaunched = Boolean(doc.rocketState.allLaunched);
    if (Number(doc.rocketState.resetAtMs || 0) > 0) {
      room.rocketState.resetAtMs = Number(doc.rocketState.resetAtMs);
    }
    (doc.rocketState.contributors || []).forEach((c) => {
      if (c && c.userId) room.rocketState.contributors.set(c.userId, c);
    });
    if (Array.isArray(doc.rocketState.history)) {
      room.rocketState.history = doc.rocketState.history;
    }
  }
}

function applyHydratedDevice(doc) {
  if (!doc || !doc.deviceKey || !stateRefs) return;
  const r = stateRefs;
  const dk = String(doc.deviceKey);
  if (Array.isArray(doc.accountSlots) && doc.accountSlots.length && !r.deviceAccountSlots.has(dk)) {
    const m = new Map();
    doc.accountSlots.forEach((s) => {
      if (s && s.user8DigitId) m.set(s.user8DigitId, s);
    });
    if (m.size > 0) r.deviceAccountSlots.set(dk, m);
  }
  if (doc.inviteClaim && !r.deviceInviteClaims.has(dk)) {
    r.deviceInviteClaims.set(dk, doc.inviteClaim);
  }
  if (doc.dailyCheckInDate && !r.deviceDailyCheckIns.has(dk)) {
    r.deviceDailyCheckIns.set(dk, doc.dailyCheckInDate);
  }
  if (doc.banned && !r.bannedDeviceKeys.has(dk)) {
    r.bannedDeviceKeys.set(dk, doc.banned);
  }
}

function applyHydratedGlobal(doc) {
  if (!doc || !stateRefs) return;
  stateRefs.applyHydratedGlobalState(doc);
}

async function hydrateFromCloud() {
  if (!stateRefs) return;
  if (db && firestoreEnabled) {
    try {
      const [usersSnap, roomsSnap, devSnap, dmSnap, globalSnap] = await Promise.all([
        db.collection('users').get(),
        db.collection('rooms').get(),
        db.collection('device_locks').get(),
        db.collection('dm_threads').get(),
        db.collection('app_config').doc('global_state').get(),
      ]);

      usersSnap.forEach((d) => applyHydratedUser(d.data()));
      roomsSnap.forEach((d) => applyHydratedRoom(d.data()));
      devSnap.forEach((d) => applyHydratedDevice(d.data()));
      dmSnap.forEach((d) => {
        const data = d.data();
        if (data?.threadKey && Array.isArray(data.messages) && !stateRefs.dmThreads.has(data.threadKey)) {
          stateRefs.dmThreads.set(data.threadKey, data.messages);
        }
      });
      if (globalSnap.exists) applyHydratedGlobal(globalSnap.data());
      console.log(
        `[FirestoreStore] Hydrated from Firestore: ${usersSnap.size} users, ${roomsSnap.size} rooms, ${devSnap.size} device locks, ${dmSnap.size} DM threads`
      );
      return;
    } catch (err) {
      if (!loadServiceAccountCredential()) {
        firestoreEnabled = false;
        firestoreInitError =
          'FIREBASE_SERVICE_ACCOUNT_JSON not configured yet (running with non-blocking async snapshot fallback).';
      }
      console.warn('[FirestoreStore] Firestore hydration skipped, using async snapshot fallback:', err.message);
    }
  }

  try {
    if (fs.existsSync(LOCAL_SNAPSHOT_FILE)) {
      const raw = await fs.promises.readFile(LOCAL_SNAPSHOT_FILE, 'utf8');
      if (raw) {
        const snap = JSON.parse(raw);
        (snap.users || []).forEach(applyHydratedUser);
        (snap.rooms || []).forEach(applyHydratedRoom);
        (snap.devices || []).forEach(applyHydratedDevice);
        (snap.dmThreads || []).forEach((t) => {
          if (t?.threadKey && Array.isArray(t.messages) && !stateRefs.dmThreads.has(t.threadKey)) {
            stateRefs.dmThreads.set(t.threadKey, t.messages);
          }
        });
        if (snap.globalState) applyHydratedGlobal(snap.globalState);
        console.log(
          `[FirestoreStore] Hydrated from local snapshot: ${(snap.users || []).length} users, ${(snap.rooms || []).length} rooms`
        );
      }
    }
  } catch (err) {
    console.warn('[FirestoreStore] Local snapshot read warning:', err.message);
  }
}

async function uploadMediaToFirebaseStorage({ buffer, objectPath, contentType }) {
  const client = initFirestoreClient();
  if (!client) {
    throw new Error(
      firestoreInitError || 'Firebase Admin is not configured yet (set FIREBASE_SERVICE_ACCOUNT_JSON on server).'
    );
  }
  const adminApp = require('firebase-admin/app');
  const adminStorage = require('firebase-admin/storage');
  const app = adminApp.getApp();
  const storage = adminStorage.getStorage(app);

  const candidateBuckets = Array.from(
    new Set([
      FIREBASE_STORAGE_BUCKET,
      `${FIREBASE_PROJECT_ID}.firebasestorage.app`,
      `${FIREBASE_PROJECT_ID}.appspot.com`,
    ].filter(Boolean))
  );

  const downloadToken = crypto.randomUUID();
  let lastErr = null;

  for (const bucketName of candidateBuckets) {
    try {
      const bucket = storage.bucket(bucketName);
      const file = bucket.file(objectPath);
      await file.save(buffer, {
        resumable: false,
        metadata: {
          contentType: contentType || 'application/octet-stream',
          cacheControl: 'public, max-age=31536000',
          metadata: {
            firebaseStorageDownloadTokens: downloadToken,
          },
        },
      });
      const encodedPath = encodeURIComponent(objectPath);
      const downloadUrl = `https://firebasestorage.googleapis.com/v0/b/${bucketName}/o/${encodedPath}?alt=media&token=${downloadToken}`;
      return { downloadUrl, bucketName, objectPath };
    } catch (err) {
      lastErr = err;
      const msg = String(err?.message || '');
      // Try next bucket naming convention only if the bucket itself does not exist
      if (err?.code === 404 || msg.includes('does not exist')) {
        continue;
      }
      throw err;
    }
  }
  throw lastErr || new Error('Unable to upload file to Firebase Storage bucket.');
}

async function saveGiftToFirestoreCollection({
  giftId,
  name,
  price,
  videoUrl,
  iconUrl,
  category = 'Gifts',
  tag = 'MP4',
  type = 'mp4',
  active = true,
}) {
  const client = initFirestoreClient();
  if (!client) {
    throw new Error(
      firestoreInitError || 'Firestore is not configured yet (set FIREBASE_SERVICE_ACCOUNT_JSON on server).'
    );
  }
  const adminFirestore = require('firebase-admin/firestore');
  const docId = String(giftId || `g_${Date.now()}`).trim();
  const payload = {
    name: String(name || '').trim(),
    price: Math.max(1, Math.floor(Number(price || 10))),
    videoUrl: String(videoUrl || '').trim(),
    iconUrl: String(iconUrl || '').trim(),
    type: String(type || 'mp4'),
    category: String(category || 'Gifts').trim() || 'Gifts',
    tag: String(tag || 'MP4').trim() || 'MP4',
    active: active !== false,
    createdAt: adminFirestore.FieldValue.serverTimestamp(),
  };
  await client.collection('gifts').doc(docId).set(payload, { merge: true });
  return { id: docId, ...payload };
}

async function deleteGiftFromFirestoreCollection(giftId) {
  const client = initFirestoreClient();
  if (!client || !giftId) return;
  await client.collection('gifts').doc(String(giftId)).delete().catch(() => {});
}

function getStatus() {
  return {
    firestoreConnected: firestoreEnabled,
    projectId: FIREBASE_PROJECT_ID,
    databaseId: FIRESTORE_DATABASE_ID,
    storageBucket: FIREBASE_STORAGE_BUCKET,
    note: firestoreEnabled ? 'Active (Batch Write-Behind)' : firestoreInitError,
    pendingFlushCount:
      dirtyUsers.size + dirtyRooms.size + dirtyDevices.size + dirtyDms.size + (dirtyGlobal ? 1 : 0),
  };
}

module.exports = {
  bindStateRefs,
  hydrateFromCloud,
  flushDirtyState,
  markUserDirty,
  markRoomDirty,
  markDeviceDirty,
  markDmDirty,
  markGlobalDirty,
  uploadMediaToFirebaseStorage,
  saveGiftToFirestoreCollection,
  deleteGiftFromFirestoreCollection,
  getStatus,
};
