'use strict';

/**
 * Modular Profile & Gift-Linked Medals Engine
 * Handles:
 * 1. Gift-Linked 4-Star Medals (1★, 2★, 3★, 4★ coin thresholds linked to Total Gift Coins or Specific Gift)
 * 2. Real-time Gift Wall (receivedGifts with Cloudinary icon URL & xCount)
 * 3. 10-Slot Equipped Medals (equippedMedalIds) displayed in horizontal scroll on Own & Public Profile
 * 4. Live Profile Album Photos (max 4) & Custom Cover Background (bgUrl)
 */

const DEFAULT_GIFT_LINKED_MEDALS = [
  {
    id: 'ach_multi_millionaire',
    name: 'Multi-Millionaire',
    category: 'ACHIEVEMENT',
    starCount: 4,
    description: 'Send total gift coins in voice rooms to upgrade 1★ to 4★.',
    unlockCondition: '1★: 10L | 2★: 50L | 3★: 1Cr | 4★: 5Cr Coins',
    iconUrl: '',
    linkType: 'ALL_GIFTS_COINS',
    linkedGiftId: '',
    linkedGiftName: '',
    starThresholds: [1000000, 5000000, 10000000, 50000000],
    targetProgress: 1000000,
    active: true,
  },
  {
    id: 'g_rose_star',
    name: 'Romantic Lover',
    category: 'GIFT',
    starCount: 4,
    description: 'Send Rose gifts in voice chatrooms to upgrade star level.',
    unlockCondition: '1★: 1L | 2★: 5L | 3★: 20L | 4★: 1Cr Coins',
    iconUrl: '',
    linkType: 'SPECIFIC_GIFT',
    linkedGiftId: 'g_rose',
    linkedGiftName: 'Rose',
    starThresholds: [100000, 500000, 2000000, 10000000],
    targetProgress: 100000,
    active: true,
  },
  {
    id: 'g_ferris_star',
    name: 'Ferris Wheel Star',
    category: 'GIFT',
    starCount: 4,
    description: 'Send Ferris Wheel gifts to unlock & upgrade 1★ to 4★.',
    unlockCondition: '1★: 10L | 2★: 50L | 3★: 1Cr | 4★: 5Cr Coins',
    iconUrl: '',
    linkType: 'SPECIFIC_GIFT',
    linkedGiftId: 'g_ferris',
    linkedGiftName: 'Ferris Wheel',
    starThresholds: [1000000, 5000000, 10000000, 50000000],
    targetProgress: 1000000,
    active: true,
  },
  {
    id: 'g_castle_star',
    name: 'Dream Castle Star',
    category: 'GIFT',
    starCount: 4,
    description: 'Send Dream Castle gifts to unlock & upgrade 1★ to 4★.',
    unlockCondition: '1★: 10L | 2★: 50L | 3★: 2Cr | 4★: 10Cr Coins',
    iconUrl: '',
    linkType: 'SPECIFIC_GIFT',
    linkedGiftId: 'g_castle',
    linkedGiftName: 'Dream Castle',
    starThresholds: [1000000, 5000000, 20000000, 100000000],
    targetProgress: 1000000,
    active: true,
  },
  {
    id: 'g_yacht_star',
    name: 'Luxury Yacht Star',
    category: 'GIFT',
    starCount: 4,
    description: 'Send Luxury Yacht gifts to unlock & upgrade 1★ to 4★.',
    unlockCondition: '1★: 10L | 2★: 50L | 3★: 2Cr | 4★: 10Cr Coins',
    iconUrl: '',
    linkType: 'SPECIFIC_GIFT',
    linkedGiftId: 'g_yacht',
    linkedGiftName: 'Luxury Yacht',
    starThresholds: [1000000, 5000000, 20000000, 100000000],
    targetProgress: 1000000,
    active: true,
  },
  {
    id: 'g_dragon_star',
    name: 'Dragon Master',
    category: 'GIFT',
    starCount: 4,
    description: 'Send Golden Dragon gifts to unlock & upgrade 1★ to 4★.',
    unlockCondition: '1★: 25L | 2★: 1Cr | 3★: 5Cr | 4★: 20Cr Coins',
    iconUrl: '',
    linkType: 'SPECIFIC_GIFT',
    linkedGiftId: 'g_dragon',
    linkedGiftName: 'Golden Dragon',
    starThresholds: [2500000, 10000000, 50000000, 200000000],
    targetProgress: 2500000,
    active: true,
  },
];

function normalizeStarThresholds(rawThresholds, fallbackTarget = 1000000) {
  const base = Math.max(1, Math.floor(Number(fallbackTarget || 1000000)));
  if (Array.isArray(rawThresholds) && rawThresholds.length >= 4) {
    const t1 = Math.max(1, Math.floor(Number(rawThresholds[0] || base)));
    const t2 = Math.max(t1, Math.floor(Number(rawThresholds[1] || t1 * 5)));
    const t3 = Math.max(t2, Math.floor(Number(rawThresholds[2] || t2 * 2)));
    const t4 = Math.max(t3, Math.floor(Number(rawThresholds[3] || t3 * 5)));
    return [t1, t2, t3, t4];
  }
  return [base, base * 5, base * 10, base * 50];
}

function calculateStarsForProgress(progress, thresholds) {
  const p = Number(progress || 0);
  const [t1, t2, t3, t4] = normalizeStarThresholds(thresholds);
  if (p >= t4) return 4;
  if (p >= t3) return 3;
  if (p >= t2) return 2;
  if (p >= t1) return 1;
  return 0;
}

function getNextThresholdForProgress(progress, thresholds) {
  const p = Number(progress || 0);
  const [t1, t2, t3, t4] = normalizeStarThresholds(thresholds);
  if (p < t1) return t1;
  if (p < t2) return t2;
  if (p < t3) return t3;
  return t4;
}

function initProfileAndMedalsModule(ctx) {
  const {
    app,
    users,
    DYNAMIC_GIFTS_CATALOG,
    DYNAMIC_MEDALS_CATALOG,
    firestoreStore,
    sendToUser,
    pushSocialSyncToUser,
    pushSystemNotificationToUser,
    broadcastDynamicCatalog,
    requireAdminAuth,
  } = ctx;

  function ensureDefaultMedalsSeeded() {
    for (const def of DEFAULT_GIFT_LINKED_MEDALS) {
      if (!DYNAMIC_MEDALS_CATALOG.has(def.id)) {
        DYNAMIC_MEDALS_CATALOG.set(def.id, { ...def });
      } else {
        const existing = DYNAMIC_MEDALS_CATALOG.get(def.id);
        DYNAMIC_MEDALS_CATALOG.set(def.id, {
          ...def,
          ...existing,
          starThresholds: normalizeStarThresholds(existing.starThresholds, existing.targetProgress || def.targetProgress),
        });
      }
    }
  }

  ensureDefaultMedalsSeeded();

  /**
   * Called whenever a gift transaction completes in a room (`SEND_GIFT_TX`).
   * 1. Ensures receiver's `receivedGifts` stores the real Cloudinary icon URL from `DYNAMIC_GIFTS_CATALOG`
   * 2. Updates sender's gift-linked medal progress & unlocks/upgrades 1★..4★ tiers
   */
  function onGiftTransactionCompleted({
    fromUserId,
    uniqueTargets = [],
    coinsSpent = 0,
    perTargetCharm = 0,
    giftId = '',
    giftName = '',
    giftIcon = '',
    giftCount = 1,
  }) {
    ensureDefaultMedalsSeeded();

    // 1. Resolve authoritative gift icon URL from DYNAMIC_GIFTS_CATALOG if available
    const catalogGift =
      (giftId && DYNAMIC_GIFTS_CATALOG.get(String(giftId))) ||
      Array.from(DYNAMIC_GIFTS_CATALOG.values()).find(
        (g) => g && String(g.name || '').toLowerCase() === String(giftName || '').toLowerCase()
      );
    const resolvedIcon =
      (catalogGift && catalogGift.icon) ||
      giftIcon ||
      '🎁';
    const resolvedGiftId = (catalogGift && catalogGift.id) || giftId || `g_${Date.now()}`;
    const resolvedGiftName = (catalogGift && catalogGift.name) || giftName || 'Gift';

    // Patch each receiver's receivedGifts entry so it always has the real Cloudinary icon URL
    for (const tUid of uniqueTargets) {
      if (!tUid) continue;
      const receiver = users.get(String(tUid));
      if (!receiver || !Array.isArray(receiver.receivedGifts)) continue;
      const idx = receiver.receivedGifts.findIndex(
        (g) =>
          (g.id && g.id === resolvedGiftId) ||
          String(g.name || '').toLowerCase() === String(resolvedGiftName).toLowerCase()
      );
      if (idx >= 0) {
        receiver.receivedGifts[idx] = {
          ...receiver.receivedGifts[idx],
          id: resolvedGiftId,
          name: resolvedGiftName,
          icon: resolvedIcon,
        };
        users.set(String(tUid), receiver);
        firestoreStore.markUserDirty(String(tUid));
      }
    }

    // 2. Update sender's gift-linked medals
    if (!fromUserId || coinsSpent <= 0) return;
    const uid = String(fromUserId);
    const sender = users.get(uid) || { userId: uid, user8DigitId: uid };
    const medalProgress =
      sender.medalProgress && typeof sender.medalProgress === 'object'
        ? { ...sender.medalProgress }
        : {};
    const unlockedSet = new Set(Array.isArray(sender.unlockedMedalIds) ? sender.unlockedMedalIds : []);
    const equippedList = Array.isArray(sender.equippedMedalIds)
      ? sender.equippedMedalIds.filter(Boolean).slice(0, 10)
      : [];

    let anyMedalUpdated = false;
    const newlyUnlockedOrUpgraded = [];

    for (const medal of DYNAMIC_MEDALS_CATALOG.values()) {
      if (!medal || medal.active === false) continue;
      const linkType = String(medal.linkType || '').toUpperCase();
      const linkedId = String(medal.linkedGiftId || '').trim().toLowerCase();
      const linkedName = String(medal.linkedGiftName || '').trim().toLowerCase();
      const sentId = String(resolvedGiftId || '').trim().toLowerCase();
      const sentName = String(resolvedGiftName || '').trim().toLowerCase();

      let matches = false;
      if (linkType === 'ALL_GIFTS_COINS' || medal.id === 'ach_multi_millionaire') {
        matches = true;
      } else if (
        linkType === 'SPECIFIC_GIFT' ||
        medal.category === 'GIFT' ||
        linkedId.length > 0 ||
        linkedName.length > 0
      ) {
        if (linkedId && sentId && (linkedId === sentId || sentId.includes(linkedId))) {
          matches = true;
        } else if (linkedName && sentName && (linkedName === sentName || sentName.includes(linkedName))) {
          matches = true;
        }
      }

      if (!matches) continue;

      const prevEntry = medalProgress[medal.id] || { progress: 0, stars: 0 };
      const prevStars = Number(prevEntry.stars || 0);
      const newProgress = Number(prevEntry.progress || 0) + Number(coinsSpent || 0);
      const thresholds = normalizeStarThresholds(medal.starThresholds, medal.targetProgress || 1000000);
      const newStars = calculateStarsForProgress(newProgress, thresholds);
      const nextTarget = getNextThresholdForProgress(newProgress, thresholds);

      medalProgress[medal.id] = {
        progress: newProgress,
        stars: newStars,
        nextTarget,
        updatedAtMs: Date.now(),
      };
      anyMedalUpdated = true;

      if (newStars >= 1) {
        unlockedSet.add(medal.id);
        if (!equippedList.includes(medal.id) && equippedList.length < 10) {
          equippedList.push(medal.id);
        }
      }

      if (newStars > prevStars) {
        newlyUnlockedOrUpgraded.push({
          medalId: medal.id,
          medalName: medal.name,
          stars: newStars,
        });
      }
    }

    if (anyMedalUpdated) {
      sender.medalProgress = medalProgress;
      sender.unlockedMedalIds = Array.from(unlockedSet);
      sender.equippedMedalIds = equippedList.slice(0, 10);
      users.set(uid, sender);
      firestoreStore.markUserDirty(uid);

      sendToUser(uid, {
        type: 'MEDAL_PROGRESS_SYNC',
        userId: uid,
        medalProgress: sender.medalProgress,
        unlockedMedalIds: sender.unlockedMedalIds,
        equippedMedalIds: sender.equippedMedalIds,
      });
      sendToUser(uid, {
        type: 'USER_REGISTERED',
        profile: sender,
      });
      if (typeof pushSocialSyncToUser === 'function') {
        pushSocialSyncToUser(uid);
      }

      for (const upg of newlyUnlockedOrUpgraded) {
        if (typeof pushSystemNotificationToUser === 'function') {
          pushSystemNotificationToUser(
            uid,
            `🏅 Medal Upgraded to ${upg.stars}★!`,
            `Congratulations! Your "${upg.medalName}" medal reached ${upg.stars} Star${upg.stars > 1 ? 's' : ''}!`
          );
        }
      }
    }
  }

  /**
   * Handle WebSocket messages for equipping medals (`EQUIP_MEDALS_SYNC`)
   */
  function handleSocketMessage(ws, msg, meta) {
    if (!msg || typeof msg.type !== 'string') return false;

    if (msg.type === 'EQUIP_MEDALS_SYNC') {
      const uid = String(msg.userId || meta?.userId || '').trim();
      if (!uid) return true;
      const existing = users.get(uid) || { userId: uid, user8DigitId: uid };
      const unlockedSet = new Set(Array.isArray(existing.unlockedMedalIds) ? existing.unlockedMedalIds : []);
      const rawEquipped = Array.isArray(msg.equippedMedalIds) ? msg.equippedMedalIds : [];
      const sanitized = [];
      for (const mid of rawEquipped) {
        const cleanId = String(mid || '').trim();
        if (!cleanId || sanitized.includes(cleanId)) continue;
        if (unlockedSet.has(cleanId) || cleanId.startsWith('m_') || cleanId.startsWith('g_') || cleanId.startsWith('ach_') || cleanId.startsWith('act_')) {
          sanitized.push(cleanId);
        }
        if (sanitized.length >= 10) break;
      }
      existing.equippedMedalIds = sanitized;
      existing.updatedAt = Date.now();
      users.set(uid, existing);
      firestoreStore.markUserDirty(uid);

      sendToUser(uid, {
        type: 'MEDAL_PROGRESS_SYNC',
        userId: uid,
        medalProgress: existing.medalProgress || {},
        unlockedMedalIds: Array.from(unlockedSet),
        equippedMedalIds: existing.equippedMedalIds,
      });
      if (typeof pushSocialSyncToUser === 'function') {
        pushSocialSyncToUser(uid);
      }
      return true;
    }

    return false;
  }

  // Enhanced Admin Medal Save Route (supports Gift Linking + 1★..4★ thresholds)
  app.post('/api/admin/medals', requireAdminAuth, (req, res) => {
    ensureDefaultMedalsSeeded();
    const id = String(req.body.id || `m_${Date.now()}`).trim();
    const name = String(req.body.name || '').trim();
    const category = String(req.body.category || 'GIFT').trim().toUpperCase();
    const starCount = Math.max(1, Math.min(4, Math.floor(Number(req.body.starCount || 4))));
    const description = String(req.body.description || 'Official Gift-Linked Medal').trim();
    const iconUrl = String(req.body.iconUrl || '').trim();
    const linkType = String(
      req.body.linkType || (req.body.linkedGiftId || req.body.linkedGiftName ? 'SPECIFIC_GIFT' : 'ALL_GIFTS_COINS')
    )
      .trim()
      .toUpperCase();
    const linkedGiftId = String(req.body.linkedGiftId || '').trim();
    const linkedGiftName = String(req.body.linkedGiftName || '').trim();

    const rawThresholds = Array.isArray(req.body.starThresholds)
      ? req.body.starThresholds
      : [
          Number(req.body.star1Coins || req.body.targetProgress || 1000000),
          Number(req.body.star2Coins || 5000000),
          Number(req.body.star3Coins || 10000000),
          Number(req.body.star4Coins || 50000000),
        ];
    const starThresholds = normalizeStarThresholds(rawThresholds, req.body.targetProgress || 1000000);
    const targetProgress = starThresholds[0];
    const unlockCondition =
      String(req.body.unlockCondition || '').trim() ||
      `1★: ${starThresholds[0].toLocaleString()} | 2★: ${starThresholds[1].toLocaleString()} | 3★: ${starThresholds[2].toLocaleString()} | 4★: ${starThresholds[3].toLocaleString()} Coins`;
    const active = req.body.active !== false;

    if (!name) return res.status(400).json({ error: 'Medal name required' });

    const medal = {
      id,
      name,
      category,
      starCount,
      description,
      unlockCondition,
      iconUrl,
      linkType,
      linkedGiftId,
      linkedGiftName,
      starThresholds,
      targetProgress,
      active,
    };

    DYNAMIC_MEDALS_CATALOG.set(id, medal);
    broadcastDynamicCatalog();
    firestoreStore.markGlobalDirty();
    res.json({ ok: true, medal });
  });

  // Track user safety strikes for NSFW / harmful photo attempts (3 strikes = auto photo lock)
  const userSafetyStrikes = new Map();

  function isGifPayload(str) {
    const clean = String(str || '').trim();
    if (!clean) return false;
    const lower = clean.toLowerCase();
    if (lower.startsWith('data:image/gif') || lower.endsWith('.gif') || lower.includes('.gif?')) {
      return true;
    }
    const rawB64 = clean
      .replace(/^base64:\/\//i, '')
      .replace(/^data:image\/[a-zA-Z0-9.+-]+;base64,/i, '')
      .trim();
    // "R0lGODlh" or "R0lGODdh" is Base64 for "GIF89a" / "GIF87a"
    if (rawB64.startsWith('R0lGOD')) {
      return true;
    }
    return false;
  }

  // Live Image Moderation & 3-Strike Auto-Protection Endpoint (Profile DP, Album, Cover & Room DP)
  app.post('/api/profile/moderate-image', (req, res) => {
    const userId = String(req.body.userId || '').trim();
    const targetKind = String(req.body.targetKind || 'PROFILE_DP').trim().toUpperCase();
    const mimeType = String(req.body.mimeType || 'image/jpeg').trim().toLowerCase();
    const base64Data = String(req.body.base64Data || '').trim();
    const clientDetectedUnsafe = req.body.clientDetectedUnsafe === true;

    // 1. Strictly block GIF format
    if (mimeType.includes('gif') || isGifPayload(base64Data)) {
      return res.status(400).json({
        ok: false,
        isSafe: false,
        reason: 'GIF_NOT_ALLOWED',
        message: 'GIF support nahi hai! Sirf JPG ya PNG photo lagayein.',
      });
    }

    // 2. Check if user is already photo-banned due to 3 NSFW strikes
    const existingStrikes = Number(userSafetyStrikes.get(userId) || 0);
    if (userId && existingStrikes >= 3) {
      return res.status(403).json({
        ok: false,
        isSafe: false,
        strikes: existingStrikes,
        reason: 'PHOTO_UPLOAD_LOCKED_3_STRIKES',
        message: '3 baar harmful/sexual photo try karne par aapka photo upload block kar diya gaya hai.',
      });
    }

    // 3. Record strike if client heuristic / Gemini flagged image as NSFW/Harmful
    if (clientDetectedUnsafe) {
      const nextStrikes = existingStrikes + 1;
      if (userId) {
        userSafetyStrikes.set(userId, nextStrikes);
        const u = users.get(userId);
        if (u) {
          u.safetyStrikes = nextStrikes;
          if (nextStrikes >= 3) {
            u.avatarUri = '';
            u.bgUrl = '';
            u.photos = [];
          }
          users.set(userId, u);
          firestoreStore.markUserDirty(userId);
        }
      }
      return res.status(400).json({
        ok: false,
        isSafe: false,
        strikes: nextStrikes,
        targetKind,
        reason: 'NSFW_OR_HARMFUL_DETECTED',
        message: 'Harmful ya sexual photo lagana strictly mana hai!',
      });
    }

    return res.json({
      ok: true,
      isSafe: true,
      strikes: existingStrikes,
      targetKind,
    });
  });

  // Admin 1-Click Reset Inappropriate Profile DP / Album / Cover / Strikes
  app.post('/api/admin/users/reset-photo', requireAdminAuth, (req, res) => {
    const userId = String(req.body.userId || '').trim();
    if (!userId) return res.status(400).json({ error: 'userId is required' });
    const existing = users.get(userId);
    if (!existing) return res.status(404).json({ error: 'User not found' });

    existing.avatarUri = '';
    existing.bgUrl = '';
    existing.photos = [];
    existing.safetyStrikes = 0;
    existing.updatedAt = Date.now();
    userSafetyStrikes.set(userId, 0);
    users.set(userId, existing);
    firestoreStore.markUserDirty(userId);

    sendToUser(userId, { type: 'USER_REGISTERED', profile: existing });
    if (typeof pushSocialSyncToUser === 'function') {
      pushSocialSyncToUser(userId);
    }
    return res.json({ ok: true, userId, message: 'User profile photos & strikes reset to clean default.' });
  });

  // Upload Profile Media (Avatar / Background Cover / 4 Album Photos / Room DP) to Cloudinary or Live Server
  app.post('/api/profile/media-upload', async (req, res) => {
    try {
      const userId = String(req.body.userId || '').trim();
      const kind = String(req.body.kind || 'album').trim().toLowerCase(); // 'avatar' | 'cover' | 'album' | 'room_dp'
      const dataUrlOrBase64 = String(req.body.base64Data || req.body.imageUrl || '').trim();
      if (!userId || !dataUrlOrBase64) {
        return res.status(400).json({ error: 'userId and base64Data/imageUrl are required' });
      }

      // Strictly reject GIF uploads on backend
      if (isGifPayload(dataUrlOrBase64)) {
        return res.status(400).json({
          error: 'GIF support nahi hai! Sirf JPG ya PNG photo lagayein.',
          reason: 'GIF_NOT_ALLOWED',
        });
      }

      // Check 3-strike lock
      const strikes = Number(userSafetyStrikes.get(userId) || 0);
      if (strikes >= 3) {
        return res.status(403).json({
          error: '3 safety violations ke kaaran photo upload locked hai.',
          reason: 'PHOTO_UPLOAD_LOCKED_3_STRIKES',
        });
      }

      let finalUrl = dataUrlOrBase64;
      if (!dataUrlOrBase64.startsWith('http://') && !dataUrlOrBase64.startsWith('https://')) {
        const normalizedDataUri = dataUrlOrBase64.startsWith('data:')
          ? dataUrlOrBase64
          : `data:image/jpeg;base64,${dataUrlOrBase64}`;
        if (firestoreStore && typeof firestoreStore.isCloudinaryConfigured === 'function' && firestoreStore.isCloudinaryConfigured()) {
          const uploaded = await firestoreStore.uploadMediaToCloudinary(normalizedDataUri, {
            folder: `justfun/profiles/${userId}`,
            resourceType: 'image',
          });
          if (uploaded && uploaded.url) {
            finalUrl = uploaded.url;
          } else {
            finalUrl = normalizedDataUri;
          }
        } else {
          finalUrl = normalizedDataUri;
        }
      }

      const existing = users.get(userId) || { userId, user8DigitId: userId };
      if (kind === 'avatar') {
        existing.avatarUri = finalUrl;
      } else if (kind === 'cover' || kind === 'bg') {
        existing.bgUrl = finalUrl;
      } else if (kind === 'room_dp') {
        existing.roomCoverUrl = finalUrl;
      } else {
        const photos = Array.isArray(existing.photos) ? [...existing.photos] : [];
        if (photos.length < 4 && !photos.includes(finalUrl)) {
          photos.push(finalUrl);
        }
        existing.photos = photos.slice(0, 4);
      }
      existing.updatedAt = Date.now();
      users.set(userId, existing);
      firestoreStore.markUserDirty(userId);

      sendToUser(userId, { type: 'USER_REGISTERED', profile: existing });
      if (typeof pushSocialSyncToUser === 'function') {
        pushSocialSyncToUser(userId);
      }
      return res.json({
        ok: true,
        url: finalUrl,
        avatarUri: existing.avatarUri || '',
        bgUrl: existing.bgUrl || '',
        photos: existing.photos || [],
      });
    } catch (err) {
      return res.status(500).json({ error: err.message || 'Profile media upload failed' });
    }
  });

  // Equip up to 10 Medals via REST
  app.post('/api/profile/medals/equip', (req, res) => {
    const userId = String(req.body.userId || '').trim();
    if (!userId) return res.status(400).json({ error: 'userId is required' });
    const existing = users.get(userId) || { userId, user8DigitId: userId };
    const rawEquipped = Array.isArray(req.body.equippedMedalIds) ? req.body.equippedMedalIds : [];
    existing.equippedMedalIds = rawEquipped
      .map((id) => String(id || '').trim())
      .filter(Boolean)
      .slice(0, 10);
    existing.updatedAt = Date.now();
    users.set(userId, existing);
    firestoreStore.markUserDirty(userId);
    sendToUser(userId, {
      type: 'MEDAL_PROGRESS_SYNC',
      userId,
      medalProgress: existing.medalProgress || {},
      unlockedMedalIds: Array.isArray(existing.unlockedMedalIds) ? existing.unlockedMedalIds : [],
      equippedMedalIds: existing.equippedMedalIds,
    });
    if (typeof pushSocialSyncToUser === 'function') {
      pushSocialSyncToUser(userId);
    }
    return res.json({ ok: true, equippedMedalIds: existing.equippedMedalIds });
  });

  return {
    ensureDefaultMedalsSeeded,
    onGiftTransactionCompleted,
    handleSocketMessage,
  };
}

module.exports = {
  initProfileAndMedalsModule,
  DEFAULT_GIFT_LINKED_MEDALS,
};
