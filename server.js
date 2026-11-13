const express = require('express');
const http = require('http');
const cors = require('cors');
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

function buildUserSummary(u, viewerId = '') {
  if (!u || !u.userId) return null;
  const uid = String(u.userId);
  const followersSet = userFollowers.get(uid);
  const followingSet = userFollowing.get(uid);
  const visitorsArr = userVisitors.get(uid);
  const viewerFollowingSet = viewerId ? userFollowing.get(String(viewerId)) : null;

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
    myThreads.push({
      peerUserId: peerId,
      peerName: peerProfile.nickname || lastMsg?.fromName || lastMsg?.toName || `User ${peerId}`,
      peerAvatarUri: peerProfile.avatarUri || '',
      unreadCount,
      lastMessageText: lastMsg ? (lastMsg.isRecalled ? 'Message recalled' : lastMsg.text) : '',
      lastMessageAtMs: lastMsg ? lastMsg.createdAtMs : 0,
      messages: messages.slice(-80),
    });
  }
  myThreads.sort((a, b) => (b.lastMessageAtMs || 0) - (a.lastMessageAtMs || 0));

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
      const hostUser = users.get(room.hostId || room.id);
      matchedRoomsMap.set(room.id, {
        id: room.id,
        title: room.title || hostUser?.nickname || 'Voice Room',
        announcement: room.announcement || 'Welcome!',
        categoryTag: room.categoryTag || 'Popular',
        countryName: room.countryName || 'India',
        onlineCount: room.activeClients.size,
        hostId: room.hostId || room.id,
        hostName: room.hostName || hostUser?.nickname || 'Host',
        hostAvatarUri: room.hostAvatarUri || hostUser?.avatarUri || '',
        isLive: room.activeClients.size > 0,
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
        matchedRoomsMap.set(uid, {
          id: uid,
          title: existingRoom?.title || u.nickname || 'Voice Room',
          announcement: existingRoom?.announcement || u.bio || 'Welcome to my room!',
          categoryTag: existingRoom?.categoryTag || 'Popular',
          countryName: u.country || 'India',
          onlineCount: existingRoom ? existingRoom.activeClients.size : 0,
          hostId: uid,
          hostName: u.nickname || 'Host',
          hostAvatarUri: existingRoom?.hostAvatarUri || u.avatarUri || '',
          isLive: existingRoom ? existingRoom.activeClients.size > 0 : false,
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
      list.push({
        id: room.id,
        title: room.title,
        announcement: room.announcement,
        categoryTag: room.categoryTag,
        countryName: room.countryName,
        onlineCount: activeCount,
        hostId: room.hostId,
        hostName: room.hostName,
        hostAvatarUri: room.hostAvatarUri,
        isLive: true,
        activeUserIds: Array.from(room.activeClients.keys()),
      });
    }
  }
  list.sort((a, b) => b.onlineCount - a.onlineCount);
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
      onlineCount: room.activeClients.size,
      isLive: room.activeClients.size > 0,
      activeUserIds: Array.from(room.activeClients.keys()),
      adminUsers: room.adminUsers,
      bannedChatUserIds: room.bannedChatUserIds,
      bannedUsers: room.bannedUsers,
      seats: room.seats,
      messages: room.messages.slice(-60),
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

  // Immediately push live rooms list on connect
  safeSend(ws, { type: 'LIVE_ROOMS_UPDATE', rooms: getLiveRoomsSummary() });

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
        safeSend(ws, { type: 'PONG', ts: Date.now() });
        break;
      }

      case 'REGISTER_USER': {
        const profile = msg.profile;
        if (!profile || !profile.userId) break;
        const uid = String(profile.userId);
        meta.userId = uid;
        addUserSocket(uid, ws);

        const existing = users.get(uid) || {};
        const merged = {
          ...existing,
          ...profile,
          userId: uid,
          user8DigitId: uid,
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
        safeSend(ws, { type: 'LIVE_ROOMS_UPDATE', rooms: getLiveRoomsSummary() });
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
        if (!fromUserId || !toUserId || !text.trim()) break;

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
          msgType: msg.msgType || 'TEXT',
          voiceDurationSec: Number(msg.voiceDurationSec || 0),
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
        const targetUserId = String(msg.targetUserId || '');
        const charmDelta = Number(msg.charmDelta || 0);
        if (targetUserId && charmDelta > 0) {
          const existing = users.get(targetUserId) || { userId: targetUserId, user8DigitId: targetUserId };
          existing.totalCharmPoints = Number(existing.totalCharmPoints || 0) + charmDelta;
          users.set(targetUserId, existing);
          pushSocialSyncToUser(targetUserId);
        }
        break;
      }

      case 'CREATE_OR_SAVE_ROOM': {
        const { roomId, title, announcement, hostId, hostName, hostAvatarUri } = msg;
        if (!roomId) break;
        const room = getOrCreateRoom(roomId, {
          title,
          announcement,
          hostId,
          hostName,
          hostAvatarUri,
        });
        if (title) room.title = title;
        if (announcement) room.announcement = announcement;
        if (hostId) room.hostId = hostId;
        if (hostName) room.hostName = hostName;
        if (hostAvatarUri) room.hostAvatarUri = hostAvatarUri;
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

        broadcastToRoom(room, buildRoomSnapshot(room));
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
          'adminUsers',
          'bannedChatUserIds',
          'bannedUsers',
          'categoryTag',
          'countryName',
        ];
        for (const key of allowedKeys) {
          if (fields[key] !== undefined) {
            room[key] = fields[key];
          }
        }
        room.updatedAt = Date.now();
        broadcastToRoom(room, buildRoomSnapshot(room));
        if (fields.title !== undefined || fields.hostName !== undefined || fields.hostAvatarUri !== undefined) {
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
    }
  });

  const scheduleGraceDisconnect = () => {
    homeSubscribers.delete(ws);
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
      }, 4000);
      pendingDisconnectTimers.set(timerKey, t);
    }
  };

  ws.on('close', scheduleGraceDisconnect);
  ws.on('error', scheduleGraceDisconnect);
});

server.listen(PORT, () => {
  console.log(`JustFun Real-Time Voice & Social Backend running on port ${PORT}`);
});
