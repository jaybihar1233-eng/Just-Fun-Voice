/**
 * Global Rocket 10-Second Broadcast ("Tassels Countdown") Module
 * Isolated module for broadcasting live room rocket >=95% warnings and launch alerts
 * across all connected live rooms with server-synced 10-second countdowns.
 */

const BROADCAST_DURATION_MS = 10_000;
const ABOUT_TO_LAUNCH_THRESHOLD = 0.95;
const ABOUT_TO_LAUNCH_DEBOUNCE_MS = 120_000;

function initGlobalRocketBroadcast({
  wss,
  rooms,
  users,
  resolveLiveUserAvatar,
  safeSend,
}) {
  const activeBroadcasts = new Map(); // broadcastId -> payload
  const recent95Alerts = new Map(); // `${roomId}:${level}` -> timestamp

  function pruneExpired(now = Date.now()) {
    for (const [id, ev] of activeBroadcasts.entries()) {
      if (!ev || now >= Number(ev.expiresAtMs || 0)) {
        activeBroadcasts.delete(id);
      }
    }
    for (const [key, ts] of recent95Alerts.entries()) {
      if (now - ts > ABOUT_TO_LAUNCH_DEBOUNCE_MS) {
        recent95Alerts.delete(key);
      }
    }
  }

  function broadcastToAllClients(payload) {
    if (!wss || !wss.clients) return;
    for (const client of wss.clients) {
      if (client && client.readyState === 1) {
        safeSend(client, payload);
      }
    }
  }

  function resolveRoomMeta(room) {
    const roomId = String(room?.id || '').trim();
    const hostId = String(room?.hostId || roomId || '').trim();
    const hostUser = hostId ? users?.get(hostId) : null;
    const roomTitle = String(room?.title || 'Live Voice Room').trim() || 'Live Voice Room';
    const hostName = String(hostUser?.nickname || room?.hostName || 'Host').trim() || 'Host';
    const hostAvatarUri = typeof resolveLiveUserAvatar === 'function'
      ? resolveLiveUserAvatar(hostId, room?.hostAvatarUri || '')
      : String(room?.hostAvatarUri || '');
    return { roomId, roomTitle, hostId, hostName, hostAvatarUri };
  }

  function createBroadcastPayload({
    broadcastId,
    room,
    level,
    phase,
    actor,
    totalPoolCoins = 0,
  }) {
    const now = Date.now();
    const lvl = Math.min(3, Math.max(1, Number(level || 1)));
    const { roomId, roomTitle, hostId, hostName, hostAvatarUri } = resolveRoomMeta(room);

    const actorId = String(actor?.userId || hostId || '').trim();
    const actorUser = actorId ? users?.get(actorId) : null;
    const actorName = String(actorUser?.nickname || actor?.nickname || hostName || 'VIP').trim() || 'VIP';
    const actorAvatar = typeof resolveLiveUserAvatar === 'function'
      ? resolveLiveUserAvatar(actorId, actor?.avatarUri || hostAvatarUri || '')
      : String(actor?.avatarUri || hostAvatarUri || '');

    const titleText = phase === 'LAUNCHED'
      ? actorName
      : roomTitle;

    const message = phase === 'LAUNCHED'
      ? `Launched Lv.${lvl} Rocket in ${roomTitle}! Tap GO to join`
      : `Lv.${lvl} Rocket 95%+ in ${roomTitle} — Launching soon! Tap GO`;

    return {
      broadcastId: String(broadcastId),
      roomId,
      roomTitle,
      hostId,
      hostName,
      hostAvatarUri,
      topGifterId: actorId,
      topGifterName: actorName,
      avatarUri: actorAvatar || hostAvatarUri || '',
      level: lvl,
      phase,
      totalPoolCoins: Math.max(0, Math.floor(Number(totalPoolCoins || 0))),
      titleText,
      message,
      startedAtMs: now,
      expiresAtMs: now + BROADCAST_DURATION_MS,
      durationMs: BROADCAST_DURATION_MS,
      serverNowMs: now,
    };
  }

  function emitGlobalBroadcast(ev) {
    if (!ev || !ev.roomId) return;
    const now = Date.now();
    pruneExpired(now);
    activeBroadcasts.set(ev.broadcastId, ev);
    broadcastToAllClients({
      type: 'GLOBAL_ROCKET_BROADCAST',
      serverNowMs: now,
      broadcast: ev,
    });
  }

  function onRocketProgress({ room, level, currentExp, targetExp, contributor }) {
    if (!room || !room.id) return;
    if (room.rocketState?.allLaunched) return;
    const target = Number(targetExp || 0);
    const exp = Number(currentExp || 0);
    if (target <= 0 || exp >= target) return;
    if ((exp / target) < ABOUT_TO_LAUNCH_THRESHOLD) return;

    const lvl = Math.min(3, Math.max(1, Number(level || 1)));
    const key = `${room.id}:${lvl}`;
    const now = Date.now();
    const prevAlertAt = recent95Alerts.get(key) || 0;
    if (now - prevAlertAt < ABOUT_TO_LAUNCH_DEBOUNCE_MS) return;

    recent95Alerts.set(key, now);
    const ev = createBroadcastPayload({
      broadcastId: `rocket_95_${room.id}_L${lvl}_${now}`,
      room,
      level: lvl,
      phase: 'ABOUT_TO_LAUNCH',
      actor: contributor,
      totalPoolCoins: 0,
    });
    emitGlobalBroadcast(ev);
  }

  function onRocketLaunched({ room, level, topGifter, totalPoolCoins, launchId }) {
    if (!room || !room.id) return;
    const lvl = Math.min(3, Math.max(1, Number(level || 1)));
    const now = Date.now();
    const id = launchId ? `rocket_launch_${launchId}` : `rocket_launch_${room.id}_L${lvl}_${now}`;
    const ev = createBroadcastPayload({
      broadcastId: id,
      room,
      level: lvl,
      phase: 'LAUNCHED',
      actor: topGifter,
      totalPoolCoins,
    });
    emitGlobalBroadcast(ev);
  }

  function pushActiveBroadcasts(ws) {
    if (!ws || ws.readyState !== 1) return;
    const now = Date.now();
    pruneExpired(now);
    if (activeBroadcasts.size === 0) return;

    const list = Array.from(activeBroadcasts.values())
      .filter((b) => Number(b.expiresAtMs || 0) > now)
      .sort((a, b) => Number(a.startedAtMs || 0) - Number(b.startedAtMs || 0))
      .map((b) => ({
        ...b,
        serverNowMs: now,
        remainingMs: Math.max(0, Number(b.expiresAtMs || 0) - now),
      }));

    if (list.length > 0) {
      safeSend(ws, {
        type: 'GLOBAL_ROCKET_BROADCAST_SYNC',
        serverNowMs: now,
        broadcasts: list,
      });
    }
  }

  return {
    onRocketProgress,
    onRocketLaunched,
    pushActiveBroadcasts,
  };
}

module.exports = {
  initGlobalRocketBroadcast,
};
