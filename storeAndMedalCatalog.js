'use strict';

/**
 * Modular Store & Medal Catalog + Upload + Logic Engine
 * Kept isolated in /backend/catalog_manager/ so server.js stays clean and lightweight.
 *
 * Responsibilities:
 * 1. Zero-default Store & Medal management (Admin Panel is single source of truth).
 * 2. Full Admin CRUD for Store Items across VEHICLE (Entry Effect), THEME (Room Theme),
 *    FRAME, BUBBLE, MIC_WAVE, SPECIAL_ID (including unlockMode & minLevelRequired).
 * 3. Direct Media Upload endpoint supporting Cloudinary or Data-URI image fallback.
 * 4. Full Admin CRUD + Dynamic Unlock Logic Engine for Medals across:
 *    ALL_GIFTS_COINS, SPECIFIC_GIFT, RECHARGE_COINS, WEALTH_LEVEL, GAME_WINS,
 *    INVITES_COUNT, MANUAL_ADMIN (with 1★..4★ thresholds & validityDays).
 */

const VALID_STORE_CATEGORIES = new Set([
  'VEHICLE',
  'THEME',
  'FRAME',
  'BUBBLE',
  'MIC_WAVE',
  'SPECIAL_ID',
]);

const VALID_UNLOCK_MODES = new Set(['PURCHASE', 'MIN_LEVEL', 'ADMIN_ONLY']);

const VALID_MEDAL_LINK_TYPES = new Set([
  'ALL_GIFTS_COINS',
  'SPECIFIC_GIFT',
  'RECHARGE_COINS',
  'WEALTH_LEVEL',
  'GAME_WINS',
  'INVITES_COUNT',
  'MANUAL_ADMIN',
]);

function normalizeThresholds(rawThresholds, fallbackTarget = 1) {
  const base = Math.max(1, Math.floor(Number(fallbackTarget || 1)));
  if (Array.isArray(rawThresholds) && rawThresholds.length >= 4) {
    const t1 = Math.max(1, Math.floor(Number(rawThresholds[0] || base)));
    const t2 = Math.max(t1, Math.floor(Number(rawThresholds[1] || t1)));
    const t3 = Math.max(t2, Math.floor(Number(rawThresholds[2] || t2)));
    const t4 = Math.max(t3, Math.floor(Number(rawThresholds[3] || t3)));
    return [t1, t2, t3, t4];
  }
  return [base, base, base, base];
}

function calcStars(progress, thresholds) {
  const p = Number(progress || 0);
  const [t1, t2, t3, t4] = normalizeThresholds(thresholds);
  if (p >= t4) return 4;
  if (p >= t3) return 3;
  if (p >= t2) return 2;
  if (p >= t1) return 1;
  return 0;
}

function nextTargetFor(progress, thresholds) {
  const p = Number(progress || 0);
  const [t1, t2, t3, t4] = normalizeThresholds(thresholds);
  if (p < t1) return t1;
  if (p < t2) return t2;
  if (p < t3) return t3;
  return t4;
}

function parseHexColor(rawHex) {
  if (typeof rawHex === 'number' && Number.isFinite(rawHex)) return rawHex;
  const s = String(rawHex || '').trim();
  if (!s) return null;
  if (/^0x[0-9a-fA-F]{6,8}$/.test(s)) {
    return Number.parseInt(s, 16);
  }
  const clean = s.replace(/^#/, '');
  if (/^[0-9a-fA-F]{6}$/.test(clean)) {
    return Number.parseInt(`FF${clean}`, 16);
  }
  if (/^[0-9a-fA-F]{8}$/.test(clean)) {
    return Number.parseInt(clean, 16);
  }
  return null;
}

function buildDefaultConditionText(linkType, thresholds, linkedGiftName) {
  const [t1, t2, t3, t4] = thresholds;
  const fmt = (n) => Number(n || 0).toLocaleString('en-US');
  const suffix =
    linkType === 'WEALTH_LEVEL'
      ? 'Lv.'
      : linkType === 'INVITES_COUNT'
        ? 'Invites'
        : linkType === 'MANUAL_ADMIN'
          ? 'Admin Grant'
          : 'Coins';
  if (linkType === 'MANUAL_ADMIN') return 'Exclusive Official Admin Grant';
  if (linkType === 'WEALTH_LEVEL') {
    return `1★: Lv.${t1} | 2★: Lv.${t2} | 3★: Lv.${t3} | 4★: Lv.${t4}`;
  }
  if (linkType === 'SPECIFIC_GIFT' && linkedGiftName) {
    return `${linkedGiftName}: 1★ ${fmt(t1)} | 2★ ${fmt(t2)} | 3★ ${fmt(t3)} | 4★ ${fmt(t4)}`;
  }
  return `1★: ${fmt(t1)} | 2★: ${fmt(t2)} | 3★: ${fmt(t3)} | 4★: ${fmt(t4)} ${suffix}`;
}

function initStoreAndMedalCatalog(ctx) {
  const {
    app,
    users,
    userLevels,
    STORE_CATALOG,
    DYNAMIC_MEDALS_CATALOG,
    firestoreStore,
    sendToUser,
    pushSocialSyncToUser,
    pushSystemNotificationToUser,
    broadcastDynamicCatalog,
    requireAdminAuth,
    uploadMediaToCloudinaryLocal,
    markStoreCustomized,
  } = ctx;

  // =========================================================================
  // 1. DIRECT MEDIA UPLOAD (Cloudinary or Validated Image Data-URI Fallback)
  // =========================================================================
  app.post('/api/admin/catalog/upload-media', requireAdminAuth, async (req, res) => {
    try {
      const kindRaw = String(req.body?.kind || 'icon').toLowerCase();
      const kind = kindRaw === 'video' ? 'video' : 'icon';
      const dataUrl = String(req.body?.dataUrl || '').trim();
      const contentType = String(req.body?.contentType || (kind === 'video' ? 'video/mp4' : 'image/png'));
      if (!dataUrl) {
        return res.status(400).json({ error: 'dataUrl is required' });
      }
      if (dataUrl.startsWith('http://') || dataUrl.startsWith('https://')) {
        return res.json({ ok: true, url: dataUrl, storage: 'external_url' });
      }

      const match = dataUrl.match(/^data:([^;]+);base64,(.+)$/);
      if (match && typeof uploadMediaToCloudinaryLocal === 'function') {
        try {
          const buf = Buffer.from(match[2], 'base64');
          const uploaded = await uploadMediaToCloudinaryLocal({
            buffer: buf,
            kind,
            contentType: match[1] || contentType,
          });
          if (uploaded && uploaded.url) {
            return res.json({ ok: true, url: uploaded.url, storage: 'cloudinary' });
          }
        } catch (cloudErr) {
          // Fall back gracefully if image is small enough to inline when Cloudinary is not configured
          if (kind === 'icon' && dataUrl.length <= 650000) {
            return res.json({
              ok: true,
              url: dataUrl,
              storage: 'inline_fallback',
              warning: cloudErr.message,
            });
          }
          return res.status(500).json({
            error: `Cloudinary upload failed (${cloudErr.message}). Configure Cloudinary env keys or use an external HTTPS URL.`,
          });
        }
      }

      if (kind === 'icon' && dataUrl.length <= 650000) {
        return res.json({ ok: true, url: dataUrl, storage: 'inline_fallback' });
      }
      return res.status(400).json({ error: 'Invalid upload payload or file too large without Cloudinary.' });
    } catch (err) {
      return res.status(500).json({ error: err.message || 'Catalog upload failed' });
    }
  });

  // =========================================================================
  // 2. STORE CATALOG CRUD (FRAME / VEHICLE / THEME / BUBBLE / MIC_WAVE / SPECIAL_ID)
  // =========================================================================
  app.post('/api/admin/store/catalog', requireAdminAuth, (req, res) => {
    const id = String(req.body.id || `st_${Date.now()}`).trim();
    const name = String(req.body.name || '').trim();
    if (!name) return res.status(400).json({ error: 'Item name is required' });

    const prev = STORE_CATALOG[id] || {};
    const rawCat = String(req.body.category || prev.category || 'FRAME').trim().toUpperCase();
    const category = VALID_STORE_CATEGORIES.has(rawCat) ? rawCat : 'FRAME';

    const subtitle = String(
      req.body.subtitle || prev.subtitle || `Exclusive ${category.replace('_', ' ')} Item`
    ).trim();
    const basePrice7Days = Math.max(0, Math.floor(Number(req.body.basePrice7Days ?? prev.basePrice7Days ?? 10000)));
    const rarityStars = Math.max(1, Math.min(3, Math.floor(Number(req.body.rarityStars || prev.rarityStars || 2))));
    const rawUnlockMode = String(req.body.unlockMode || prev.unlockMode || 'PURCHASE').trim().toUpperCase();
    const unlockMode = VALID_UNLOCK_MODES.has(rawUnlockMode) ? rawUnlockMode : 'PURCHASE';
    const minLevelRequired = Math.max(0, Math.min(200, Math.floor(Number(req.body.minLevelRequired ?? prev.minLevelRequired ?? 0))));

    const iconUrl = req.body.iconUrl !== undefined ? String(req.body.iconUrl || '').trim() : (prev.iconUrl || '');
    const animationUrl = req.body.animationUrl !== undefined ? String(req.body.animationUrl || '').trim() : (prev.animationUrl || '');
    const hasAnimationPlay = req.body.hasAnimationPlay !== undefined
      ? Boolean(req.body.hasAnimationPlay)
      : (Boolean(animationUrl) || category === 'VEHICLE' || category === 'MIC_WAVE' || Boolean(prev.hasAnimationPlay));
    const specialIdNumber = req.body.specialIdNumber !== undefined
      ? String(req.body.specialIdNumber || '').trim()
      : (prev.specialIdNumber || '');
    const sampleBubbleText = req.body.sampleBubbleText !== undefined
      ? String(req.body.sampleBubbleText || '').trim()
      : (prev.sampleBubbleText || 'Welcome to Just Fun Voice Party! 🎤');
    const parsedColor = parseHexColor(req.body.micWaveColorHex !== undefined ? req.body.micWaveColorHex : prev.micWaveColorHex);
    const active = req.body.active !== false;

    STORE_CATALOG[id] = {
      ...prev,
      id,
      name,
      subtitle,
      category,
      basePrice7Days: unlockMode === 'ADMIN_ONLY' ? basePrice7Days : Math.max(100, basePrice7Days),
      rarityStars,
      unlockMode,
      minLevelRequired,
      hasAnimationPlay,
      iconUrl,
      animationUrl,
      specialIdNumber,
      sampleBubbleText,
      micWaveColorHex: parsedColor !== null ? parsedColor : (category === 'MIC_WAVE' ? 0xFF19CC9D : undefined),
      active,
      updatedAtMs: Date.now(),
    };

    if (typeof markStoreCustomized === 'function') markStoreCustomized(true);
    broadcastDynamicCatalog();
    firestoreStore.markGlobalDirty();
    firestoreStore.flushDirtyState().catch(() => {});
    res.json({ ok: true, item: STORE_CATALOG[id] });
  });

  app.post('/api/admin/store/toggle', requireAdminAuth, (req, res) => {
    const id = String(req.body.id || '').trim();
    const item = STORE_CATALOG[id];
    if (!item) return res.status(404).json({ error: 'Store item not found' });
    item.active = Boolean(req.body.active);
    STORE_CATALOG[id] = item;
    if (typeof markStoreCustomized === 'function') markStoreCustomized(true);
    broadcastDynamicCatalog();
    firestoreStore.markGlobalDirty();
    firestoreStore.flushDirtyState().catch(() => {});
    res.json({ ok: true, item });
  });

  app.delete('/api/admin/store/catalog', requireAdminAuth, (req, res) => {
    Object.keys(STORE_CATALOG).forEach((k) => delete STORE_CATALOG[k]);
    if (typeof markStoreCustomized === 'function') markStoreCustomized(true);
    broadcastDynamicCatalog();
    firestoreStore.markGlobalDirty();
    firestoreStore.flushDirtyState().catch(() => {});
    res.json({ ok: true, cleared: true });
  });

  app.delete('/api/admin/store/catalog/:id', requireAdminAuth, (req, res) => {
    const id = String(req.params.id || '').trim();
    delete STORE_CATALOG[id];
    if (typeof markStoreCustomized === 'function') markStoreCustomized(true);
    broadcastDynamicCatalog();
    firestoreStore.markGlobalDirty();
    firestoreStore.flushDirtyState().catch(() => {});
    res.json({ ok: true, deletedId: id });
  });

  // =========================================================================
  // 3. MEDAL CATALOG CRUD + GRANT/REVOKE + LOGIC ENGINE
  // =========================================================================
  app.post('/api/admin/medals', requireAdminAuth, (req, res) => {
    const id = String(req.body.id || `m_${Date.now()}`).trim();
    const name = String(req.body.name || '').trim();
    if (!name) return res.status(400).json({ error: 'Medal name required' });

    const prev = DYNAMIC_MEDALS_CATALOG.get(id) || {};
    const rawCat = String(req.body.category || prev.category || 'ACHIEVEMENT').trim().toUpperCase();
    const category = ['ACHIEVEMENT', 'GIFT', 'ACTIVITY'].includes(rawCat) ? rawCat : 'ACHIEVEMENT';
    const rawLink = String(
      req.body.linkType ||
        prev.linkType ||
        (req.body.linkedGiftId || req.body.linkedGiftName ? 'SPECIFIC_GIFT' : 'ALL_GIFTS_COINS')
    )
      .trim()
      .toUpperCase();
    const linkType = VALID_MEDAL_LINK_TYPES.has(rawLink) ? rawLink : 'ALL_GIFTS_COINS';
    const linkedGiftId = String(req.body.linkedGiftId ?? prev.linkedGiftId ?? '').trim();
    const linkedGiftName = String(req.body.linkedGiftName ?? prev.linkedGiftName ?? '').trim();

    const rawThresholds = Array.isArray(req.body.starThresholds)
      ? req.body.starThresholds
      : [
          Number(req.body.star1Coins || req.body.targetProgress || 1),
          Number(req.body.star2Coins || req.body.star1Coins || 1),
          Number(req.body.star3Coins || req.body.star2Coins || 1),
          Number(req.body.star4Coins || req.body.star3Coins || 1),
        ];
    const starThresholds = linkType === 'MANUAL_ADMIN'
      ? [1, 1, 1, 1]
      : normalizeThresholds(rawThresholds, req.body.targetProgress || 1);

    const starCount = Math.max(1, Math.min(4, Math.floor(Number(req.body.starCount || prev.starCount || 4))));
    const validityDays = Math.max(0, Math.floor(Number(req.body.validityDays ?? prev.validityDays ?? 0)));
    const iconUrl = req.body.iconUrl !== undefined ? String(req.body.iconUrl || '').trim() : (prev.iconUrl || '');
    const description = String(req.body.description || prev.description || 'Official Medal').trim();
    const unlockCondition = String(req.body.unlockCondition || '').trim()
      || buildDefaultConditionText(linkType, starThresholds, linkedGiftName);
    const active = req.body.active !== false;

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
      targetProgress: starThresholds[0],
      validityDays,
      active,
      updatedAtMs: Date.now(),
    };

    DYNAMIC_MEDALS_CATALOG.set(id, medal);
    broadcastDynamicCatalog();
    firestoreStore.markGlobalDirty();
    firestoreStore.flushDirtyState().catch(() => {});
    res.json({ ok: true, medal });
  });

  app.post('/api/admin/medals/toggle', requireAdminAuth, (req, res) => {
    const id = String(req.body.id || '').trim();
    const medal = DYNAMIC_MEDALS_CATALOG.get(id);
    if (!medal) return res.status(404).json({ error: 'Medal not found' });
    medal.active = Boolean(req.body.active);
    DYNAMIC_MEDALS_CATALOG.set(id, medal);
    broadcastDynamicCatalog();
    firestoreStore.markGlobalDirty();
    firestoreStore.flushDirtyState().catch(() => {});
    res.json({ ok: true, medal });
  });

  app.delete('/api/admin/medals', requireAdminAuth, (req, res) => {
    DYNAMIC_MEDALS_CATALOG.clear();
    broadcastDynamicCatalog();
    firestoreStore.markGlobalDirty();
    firestoreStore.flushDirtyState().catch(() => {});
    res.json({ ok: true, cleared: true });
  });

  app.delete('/api/admin/medals/:id', requireAdminAuth, (req, res) => {
    const id = String(req.params.id || '').trim();
    DYNAMIC_MEDALS_CATALOG.delete(id);
    broadcastDynamicCatalog();
    firestoreStore.markGlobalDirty();
    firestoreStore.flushDirtyState().catch(() => {});
    res.json({ ok: true, deletedId: id });
  });

  app.post('/api/admin/medals/revoke', requireAdminAuth, (req, res) => {
    const userId = String(req.body.userId || '').trim();
    const medalId = String(req.body.medalId || '').trim();
    if (!userId || !medalId) {
      return res.status(400).json({ error: 'userId and medalId are required' });
    }
    const prof = users.get(userId);
    if (!prof) return res.status(404).json({ error: 'User not found' });
    prof.unlockedMedalIds = (Array.isArray(prof.unlockedMedalIds) ? prof.unlockedMedalIds : []).filter((m) => m !== medalId);
    prof.equippedMedalIds = (Array.isArray(prof.equippedMedalIds) ? prof.equippedMedalIds : []).filter((m) => m !== medalId);
    if (prof.medalProgress && typeof prof.medalProgress === 'object') {
      delete prof.medalProgress[medalId];
    }
    users.set(userId, prof);
    firestoreStore.markUserDirty(userId);
    syncUserMedalsLive(userId, prof);
    res.json({ ok: true, userId, revokedMedalId: medalId });
  });

  // =========================================================================
  // 4. MEDAL PROGRESS EVALUATION HOOKS (Recharge / Level / Game Wins / Invites)
  // =========================================================================
  function syncUserMedalsLive(uid, userProf) {
    sendToUser(uid, {
      type: 'MEDAL_PROGRESS_SYNC',
      userId: uid,
      medalProgress: userProf.medalProgress || {},
      unlockedMedalIds: Array.isArray(userProf.unlockedMedalIds) ? userProf.unlockedMedalIds : [],
      equippedMedalIds: Array.isArray(userProf.equippedMedalIds) ? userProf.equippedMedalIds : [],
    });
    sendToUser(uid, { type: 'USER_REGISTERED', profile: userProf });
    if (typeof pushSocialSyncToUser === 'function') {
      pushSocialSyncToUser(uid);
    }
  }

  function evaluateMedalsForUser(uid, matcherFn, valueOrDelta, { isAdditive = true } = {}) {
    if (!uid) return;
    const cleanUid = String(uid).trim();
    const prof = users.get(cleanUid);
    if (!prof) return;

    const medalProgress =
      prof.medalProgress && typeof prof.medalProgress === 'object' ? { ...prof.medalProgress } : {};
    const unlockedSet = new Set(Array.isArray(prof.unlockedMedalIds) ? prof.unlockedMedalIds : []);
    const equippedList = Array.isArray(prof.equippedMedalIds)
      ? prof.equippedMedalIds.filter(Boolean).slice(0, 10)
      : [];

    let updated = false;
    const upgrades = [];

    for (const medal of DYNAMIC_MEDALS_CATALOG.values()) {
      if (!medal || medal.active === false) continue;
      if (!matcherFn(medal)) continue;

      const prev = medalProgress[medal.id] || { progress: 0, stars: 0 };
      const prevStars = Number(prev.stars || 0);
      const nextProg = isAdditive
        ? Number(prev.progress || 0) + Number(valueOrDelta || 0)
        : Math.max(Number(prev.progress || 0), Number(valueOrDelta || 0));
      const thresholds = normalizeThresholds(medal.starThresholds, medal.targetProgress || 1);
      const newStars = calcStars(nextProg, thresholds);
      const nextTarget = nextTargetFor(nextProg, thresholds);

      medalProgress[medal.id] = {
        progress: nextProg,
        stars: newStars,
        nextTarget,
        updatedAtMs: Date.now(),
      };
      updated = true;

      if (newStars >= 1) {
        unlockedSet.add(medal.id);
        if (!equippedList.includes(medal.id) && equippedList.length < 10) {
          equippedList.push(medal.id);
        }
      }
      if (newStars > prevStars) {
        upgrades.push({ medalName: medal.name, stars: newStars });
      }
    }

    if (!updated) return;
    prof.medalProgress = medalProgress;
    prof.unlockedMedalIds = Array.from(unlockedSet);
    prof.equippedMedalIds = equippedList.slice(0, 10);
    users.set(cleanUid, prof);
    firestoreStore.markUserDirty(cleanUid);
    syncUserMedalsLive(cleanUid, prof);

    for (const up of upgrades) {
      if (typeof pushSystemNotificationToUser === 'function') {
        pushSystemNotificationToUser(
          cleanUid,
          `🏅 Medal Unlocked (${up.stars}★)!`,
          `Congratulations! Your "${up.medalName}" medal reached ${up.stars}★!`
        );
      }
    }
  }

  function onRechargeCoins(uid, coinsAdded) {
    if (coinsAdded <= 0) return;
    evaluateMedalsForUser(
      uid,
      (m) => String(m.linkType || '').toUpperCase() === 'RECHARGE_COINS',
      coinsAdded,
      { isAdditive: true }
    );
  }

  function onLevelUpdated(uid, level) {
    if (level <= 0) return;
    evaluateMedalsForUser(
      uid,
      (m) => String(m.linkType || '').toUpperCase() === 'WEALTH_LEVEL',
      level,
      { isAdditive: false }
    );
  }

  function onGameCoinsWon(uid, coinsWon) {
    if (coinsWon <= 0) return;
    evaluateMedalsForUser(
      uid,
      (m) => String(m.linkType || '').toUpperCase() === 'GAME_WINS',
      coinsWon,
      { isAdditive: true }
    );
  }

  function onFriendInvited(uid, totalInvites) {
    if (totalInvites <= 0) return;
    evaluateMedalsForUser(
      uid,
      (m) => String(m.linkType || '').toUpperCase() === 'INVITES_COUNT',
      totalInvites,
      { isAdditive: false }
    );
  }

  function validateStorePurchase(uid, itemSpec) {
    if (!itemSpec || itemSpec.active === false) {
      return { ok: false, error: 'Store item is unavailable or discontinued.' };
    }
    const mode = String(itemSpec.unlockMode || 'PURCHASE').toUpperCase();
    if (mode === 'ADMIN_ONLY') {
      return { ok: false, error: `"${itemSpec.name}" is VIP / Official Admin Grant only!` };
    }
    const minLv = Math.max(0, Number(itemSpec.minLevelRequired || 0));
    if (minLv > 0) {
      const userLv = Number(userLevels?.get(uid)?.level ?? users.get(uid)?.level ?? 0);
      if (userLv < minLv) {
        return {
          ok: false,
          error: `Requires Wealth Level Lv.${minLv} to unlock "${itemSpec.name}"! (Your Level: Lv.${userLv})`,
        };
      }
    }
    return { ok: true };
  }

  const LEGACY_DEFAULT_STORE_IDS = new Set([
    'v_phantom_car', 'v_silver_roadster', 'v_royal_crown_ride', 'v_starlight_cruiser', 'v_supersonic_jet', 'v_cupid_heart',
    'f_3rd_anniversary', 'f_charm_crescent', 'f_cp_lover', 'f_ornate_gold',
    'b_purple_panther', 'b_starlight_galaxy', 'b_golden_combo', 'b_cp_romance',
    'w_emerald_pulse', 'w_golden_crown_wave', 'w_cyber_violet_wave', 'w_rose_lover_wave',
    'sid_888888', 'sid_999999', 'sid_777777', 'sid_5201314', 'sid_100001', 'sid_112233',
  ]);
  const LEGACY_DEFAULT_MEDAL_IDS = new Set([
    'ach_multi_millionaire', 'g_rose_star', 'g_ferris_star', 'g_castle_star', 'g_yacht_star', 'g_dragon_star',
  ]);

  function purgeLegacyDefaults() {
    let changed = false;
    Object.keys(STORE_CATALOG).forEach((id) => {
      const item = STORE_CATALOG[id];
      if (LEGACY_DEFAULT_STORE_IDS.has(id) && (!item || !item.updatedAtMs)) {
        delete STORE_CATALOG[id];
        changed = true;
      }
    });
    for (const [id, m] of DYNAMIC_MEDALS_CATALOG.entries()) {
      if (LEGACY_DEFAULT_MEDAL_IDS.has(id) && (!m || !m.updatedAtMs)) {
        DYNAMIC_MEDALS_CATALOG.delete(id);
        changed = true;
      }
    }
    if (changed) {
      if (typeof markStoreCustomized === 'function') markStoreCustomized(true);
      firestoreStore.markGlobalDirty();
      firestoreStore.flushDirtyState().catch(() => {});
    }
  }

  return {
    onRechargeCoins,
    onLevelUpdated,
    onGameCoinsWon,
    onFriendInvited,
    validateStorePurchase,
    purgeLegacyDefaults,
  };
}

module.exports = {
  initStoreAndMedalCatalog,
};
