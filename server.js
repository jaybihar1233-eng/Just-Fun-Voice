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
// clientMeta: WeakMap<WebSocket, { userId, roomId, userName, avatarUri, level, isAlive }>
const clientMeta = new WeakMap();
// feedbacks: Array<FeedbackTicket>
const feedbacks = [];

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
  // Only delete if this specific socket (or no socket) owns the presence
  if (existingWs && existingWs !== ws && existingWs.readyState === WebSocket.OPEN) {
    return;
  }

  room.activeClients.delete(userId);

  // Automatically vacate any seat occupied by this user
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
  } else {
    // Reset seats cleanly when room goes completely empty
    if (seatChanged) {
      room.updatedAt = Date.now();
    }
  }

  broadcastLiveRoomsToHome();
}

// HTTP Health & Metrics Routes
app.get('/', (req, res) => {
  const liveRooms = getLiveRoomsSummary();
  res.json({
    service: 'JustFun Real-Time Voice Backend',
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
const wss = new WebSocketServer({ server, maxPayload: 4 * 1024 * 1024 });

// Server-side WebSocket heartbeat to detect silent network drops within 20s
const pingInterval = setInterval(() => {
  for (const ws of wss.clients) {
    const meta = clientMeta.get(ws);
    if (!meta) continue;
    if (meta.isAlive === false) {
      ws.terminate();
      continue;
    }
    meta.isAlive = false;
    try {
      ws.ping();
    } catch (_) {}
  }
}, 15000);

wss.on('close', () => {
  clearInterval(pingInterval);
});

wss.on('connection', (ws) => {
  const meta = { userId: '', roomId: '', isAlive: true };
  clientMeta.set(ws, meta);
  homeSubscribers.add(ws);

  // Immediately push live rooms list on connect
  safeSend(ws, { type: 'LIVE_ROOMS_UPDATE', rooms: getLiveRoomsSummary() });

  ws.on('pong', () => {
    meta.isAlive = true;
  });

  ws.on('message', (rawBuffer) => {
    meta.isAlive = true;
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
        meta.userId = profile.userId;
        const existing = users.get(profile.userId) || {};
        const merged = {
          ...existing,
          ...profile,
          user8DigitId: profile.userId,
          updatedAt: Date.now(),
        };
        users.set(profile.userId, merged);
        safeSend(ws, { type: 'USER_REGISTERED', profile: merged });
        break;
      }

      case 'GET_USERS': {
        safeSend(ws, {
          type: 'USERS_LIST',
          users: Array.from(users.values()).slice(0, 30),
        });
        break;
      }

      case 'SUBSCRIBE_HOME': {
        homeSubscribers.add(ws);
        safeSend(ws, { type: 'LIVE_ROOMS_UPDATE', rooms: getLiveRoomsSummary() });
        if (msg.userId) {
          meta.userId = msg.userId;
          const ownRoom = rooms.get(msg.userId);
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

        // If user was in a different room on this socket, clean up old room immediately
        if (meta.roomId && meta.roomId !== roomId) {
          removeUserFromRoom(ws, meta.roomId, meta.userId || userId);
        }

        meta.userId = userId;
        meta.roomId = roomId;

        const room = getOrCreateRoom(roomId, {
          title,
          announcement,
          hostId: isHost ? userId : roomId,
          hostName,
          hostAvatarUri,
        });

        // Clean any stale room membership of this user across ALL rooms
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

        // Clear any stale seat of this joining user inside this room
        room.seats = room.seats.map((seat) => {
          if (seat.occupiedUserId === userId) {
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

        room.activeClients.set(userId, ws);
        room.updatedAt = Date.now();

        broadcastToRoom(room, buildRoomSnapshot(room));
        broadcastLiveRoomsToHome();
        break;
      }

      case 'LEAVE_ROOM': {
        const targetRoomId = msg.roomId || meta.roomId;
        const targetUserId = msg.userId || meta.userId;
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
        // Smart Fallback Relay: forwards compressed 3.2KB µ-Law packet ONLY to peers
        // listed in targetPeerIds (those without direct P2P WebRTC UDP), or all other listeners if omitted
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

  ws.on('close', () => {
    homeSubscribers.delete(ws);
    if (meta.roomId && meta.userId) {
      removeUserFromRoom(ws, meta.roomId, meta.userId);
    }
  });

  ws.on('error', () => {
    homeSubscribers.delete(ws);
    if (meta.roomId && meta.userId) {
      removeUserFromRoom(ws, meta.roomId, meta.userId);
    }
  });
});

server.listen(PORT, () => {
  console.log(`JustFun Real-Time Voice Backend running on port ${PORT}`);
});
