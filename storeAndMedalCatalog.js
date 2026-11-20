'use strict';

const crypto = require('crypto');
const express = require('express');

/**
 * Modular Store & Medal Catalog + Upload + A-to-Z Logic Engine
 * Kept isolated in /backend/catalog_manager/ so server.js stays clean and lightweight.
 *
 * Responsibilities:
 * 1. Zero-default Store & Medal management (Admin Panel is single source of truth).
 * 2. Full Admin CRUD for Store Items across VEHICLE (Entry Effect), THEME (Room Theme),
 *    FRAME, BUBBLE, MIC_WAVE, SPECIAL_ID (including unlockMode & minLevelRequired).
 * 3. Direct Cloudinary Storage linking for Store & Medals (scoped folders + raw fallback).
 * 4. Complete A-to-Z Medal Logic Engine supporting:
 *    - Gift Sent (All Coins / All Count / Specific Gift Coins or Count) & Gift Received
 *    - Game Play Count, Game Win Coins, Game Win Count
 *    - Daily Login Days & Daily Check-In Claims
 *    - Event Participation (Any Event or Specific Event)
 *    - Recharge Coins, Wealth Level, Charm Points, Invites Count, Room Joins, Admin Grant
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
  'ALL_GIFTS_COUNT',
  'SPECIFIC_GIFT',
  'GIFTS_RECEIVED_COINS',
  'GAME_PLAYS_COUNT',
  'GAME_WINS',
  'GAME_WINS_COUNT',
  'LOGIN_DAYS',
  'DAILY_CHECKIN_COUNT',
  'EVENT_PARTICIPATION',
  'RECHARGE_COINS',
  'WEALTH_LEVEL',
  'CHARM_POINTS',
  'INVITES_COUNT',
  'ROOM_JOIN_COUNT',
  'MANUAL_ADMIN',
]);

function getTodayDateKeyIst() {
  return new Date(Date.now() + 19800000).toISOString().slice(0, 10);
}

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

function buildDefaultConditionText(linkType, thresholds, { linkedGiftName = '', linkedEventTitle = '', metricMode = 'COINS' } = {}) {
  const [t1, t2, t3, t4] = thresholds;
  const fmt = (n) => Number(n || 0).toLocaleString('en-US');

  switch (linkType) {
    case 'MANUAL_ADMIN':
      return 'Exclusive Official Admin Grant';
    case 'WEALTH_LEVEL':
      return `Reach Wealth Lv.${t1} / Lv.${t2} / Lv.${t3} / Lv.${t4}`;
    case 'SPECIFIC_GIFT': {
      const unit = metricMode === 'COUNT' ? 'Gifts' : 'Coins';
      const gName = linkedGiftName || 'Specific Gift';
      return `Send ${gName}: 1★ ${fmt(t1)} | 2★ ${fmt(t2)} | 3★ ${fmt(t3)} | 4★ ${fmt(t4)} ${unit}`;
    }
    case 'ALL_GIFTS_COUNT':
      return `Send Gifts: 1★ ${fmt(t1)} | 2★ ${fmt(t2)} | 3★ ${fmt(t3)} | 4★ ${fmt(t4)} Gifts`;
    case 'GIFTS_RECEIVED_COINS':
      return `Receive Gifts: 1★ ${fmt(t1)} | 2★ ${fmt(t2)} | 3★ ${fmt(t3)} | 4★ ${fmt(t4)} Coins`;
    case 'GAME_PLAYS_COUNT':
      return `Play Games: 1★ ${fmt(t1)} | 2★ ${fmt(t2)} | 3★ ${fmt(t3)} | 4★ ${fmt(t4)} Rounds`;
    case 'GAME_WINS_COUNT':
      return `Win Games: 1★ ${fmt(t1)} | 2★ ${fmt(t2)} | 3★ ${fmt(t3)} | 4★ ${fmt(t4)} Wins`;
    case 'GAME_WINS':
      return `Win Game Coins: 1★ ${fmt(t1)} | 2★ ${fmt(t2)} | 3★ ${fmt(t3)} | 4★ ${fmt(t4)} Coins`;
    case 'LOGIN_DAYS':
      return `Login Days: 1★ ${fmt(t1)} | 2★ ${fmt(t2)} | 3★ ${fmt(t3)} | 4★ ${fmt(t4)} Days`;
    case 'DAILY_CHECKIN_COUNT':
      return `Daily Check-In: 1★ ${fmt(t1)} | 2★ ${fmt(t2)} | 3★ ${fmt(t3)} | 4★ ${fmt(t4)} Days`;
    case 'EVENT_PARTICIPATION':
      return linkedEventTitle
        ? `Join Event "${linkedEventTitle}": ${fmt(t1)}★..${fmt(t4)}★`
        : `Join Live Events: 1★ ${fmt(t1)} | 2★ ${fmt(t2)} | 3★ ${fmt(t3)} | 4★ ${fmt(t4)}`;
    case 'ROOM_JOIN_COUNT':
      return `Join Voice Rooms: 1★ ${fmt(t1)} | 2★ ${fmt(t2)} | 3★ ${fmt(t3)} | 4★ ${fmt(t4)}`;
    case 'CHARM_POINTS':
      return `Earn Charm: 1★ ${fmt(t1)} | 2★ ${fmt(t2)} | 3★ ${fmt(t3)} | 4★ ${fmt(t4)}`;
    case 'INVITES_COUNT':
      return `Invite Friends: 1★ ${fmt(t1)} | 2★ ${fmt(t2)} | 3★ ${fmt(t3)} | 4★ ${fmt(t4)}`;
    default:
      return `1★: ${fmt(t1)} | 2★: ${fmt(t2)} | 3★: ${fmt(t3)} | 4★: ${fmt(t4)} Coins`;
  }
}

function getCatalogCloudinaryConfig() {
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
  const rootRaw = cleanVal(process.env.CLOUDINARY_UPLOAD_FOLDER || 'justfun').replace(/^\/+|\/+$/g, '') || 'justfun';
  const rootBase = rootRaw.replace(/\/gifts$/i, '') || 'justfun';
  return {
    configured: Boolean(cloudName && apiKey && apiSecret),
    cloudName,
    apiKey,
    apiSecret,
    rootBase,
  };
}

function signScopedCloudinaryUpload({ kind = 'icon', scope = 'store' } = {}) {
  const cfg = getCatalogCloudinaryConfig();
  if (!cfg.configured) {
    throw new Error(
      'Cloudinary is not configured on the server. Set CLOUDINARY_URL (or CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET) in Render Environment Variables.'
    );
  }
  const safeKind = String(kind).toLowerCase() === 'video' ? 'video' : 'icon';
  const safeScope = String(scope).toLowerCase() === 'medals' ? 'medals' : 'store';
  const resourceType = safeKind === 'video' ? 'video' : 'image';
  const folder = `${cfg.rootBase}/${safeScope}/${safeKind === 'video' ? 'videos' : 'icons'}`;
  const publicId = `${safeScope}_${crypto.randomUUID()}`;
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

async function uploadBufferToCloudinaryScoped({ buffer, kind = 'icon', scope = 'store', contentType }) {
  const sign = signScopedCloudinaryUpload({ kind, scope });
  const form = new FormData();
  const isVideo = sign.resourceType === 'video';
  let ext = isVideo ? 'mp4' : 'png';
  if (contentType) {
    if (contentType.includes('webp')) ext = 'webp';
    else if (contentType.includes('gif')) ext = 'gif';
    else if (contentType.includes('jpeg') || contentType.includes('jpg')) ext = 'jpg';
    else if (contentType.includes('webm')) ext = 'webm';
  }
  const mime = contentType || (isVideo ? 'video/mp4' : 'image/png');
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
  const url = data.secure_url || data.url || '';
  if (!url) {
    throw new Error('Cloudinary did not return a media URL.');
  }
  return {
    url,
    downloadUrl: url,
    publicId: data.public_id,
    folder: sign.folder,
    resourceType: sign.resourceType,
  };
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
  // 1. CLOUDINARY STORAGE STATUS, SCOPED SIGNING & DIRECT MEDIA UPLOAD
  // =========================================================================
  app.get('/api/admin/catalog/storage-status', requireAdminAuth, (_req, res) => {
    const cfg = getCatalogCloudinaryConfig();
    res.json({
      ok: true,
      cloudinaryConfigured: cfg.configured,
      cloudName: cfg.configured ? cfg.cloudName : '',
      storeFolder: `${cfg.rootBase}/store`,
      medalsFolder: `${cfg.rootBase}/medals`,
    });
  });

  app.post('/api/admin/catalog/cloudinary-sign', requireAdminAuth, (req, res) => {
    try {
      const kind = String(req.body?.kind || req.query?.kind || 'icon').toLowerCase() === 'video' ? 'video' : 'icon';
      const scope = String(req.body?.scope || req.query?.scope || 'store').toLowerCase() === 'medals' ? 'medals' : 'store';
      const signed = signScopedCloudinaryUpload({ kind, scope });
      return res.json({ ok: true, ...signed });
    } catch (err) {
      return res.status(500).json({ error: err.message || 'Cloudinary signing failed' });
    }
  });

  app.put(
    '/api/admin/catalog/upload-raw',
    requireAdminAuth,
    express.raw({ type: ['video/*', 'image/*', 'application/octet-stream'], limit: '65mb' }),
    async (req, res) => {
      try {
        const kind = String(req.query?.kind || 'icon').toLowerCase() === 'video' ? 'video' : 'icon';
        const scope = String(req.query?.scope || 'store').toLowerCase() === 'medals' ? 'medals' : 'store';
        const buffer = req.body;
        if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
          return res.status(400).json({ error: 'Empty media file upload body' });
        }
        const contentType = req.headers['content-type'] || (kind === 'video' ? 'video/mp4' : 'image/png');
        const uploaded = await uploadBufferToCloudinaryScoped({
          buffer,
          kind,
          scope,
          contentType,
        });
        return res.json({
          ok: true,
          url: uploaded.url,
          downloadUrl: uploaded.downloadUrl,
          folder: uploaded.folder,
          storage: 'cloudinary',
        });
      } catch (err) {
        return res.status(500).json({ error: err.message || 'Cloudinary raw upload failed' });
      }
    }
  );

  app.post('/api/admin/catalog/upload-media', requireAdminAuth, async (req, res) => {
    try {
      const kindRaw = String(req.body?.kind || 'icon').toLowerCase();
      const kind = kindRaw === 'video' ? 'video' : 'icon';
      const scope = String(req.body?.scope || 'store').toLowerCase() === 'medals' ? 'medals' : 'store';
      const dataUrl = String(req.body?.dataUrl || '').trim();
      const contentType = String(req.body?.contentType || (kind === 'video' ? 'video/mp4' : 'image/png'));
      if (!dataUrl) {
        return res.status(400).json({ error: 'dataUrl is required' });
      }
      if (dataUrl.startsWith('http://') || dataUrl.startsWith('https://')) {
        return res.json({ ok: true, url: dataUrl, storage: 'external_url' });
      }

      const match = dataUrl.match(/^data:([^;]+);base64,(.+)$/);
      if (match) {
        const buf = Buffer.from(match[2], 'base64');
        const cfg = getCatalogCloudinaryConfig();
        if (cfg.configured) {
          const uploaded = await uploadBufferToCloudinaryScoped({
            buffer: buf,
            kind,
            scope,
            contentType: match[1] || contentType,
          });
          return res.json({ ok: true, url: uploaded.url, storage: 'cloudinary', folder: uploaded.folder });
        }
        if (typeof uploadMediaToCloudinaryLocal === 'function') {
          try {
            const uploaded = await uploadMediaToCloudinaryLocal({
              buffer: buf,
              kind,
              contentType: match[1] || contentType,
            });
            const resolvedUrl = uploaded?.url || uploaded?.downloadUrl || '';
            if (resolvedUrl) {
              return res.json({ ok: true, url: resolvedUrl, storage: 'cloudinary' });
            }
          } catch (cloudErr) {
            if (kind === 'icon' && dataUrl.length <= 650000) {
              return res.json({
                ok: true,
                url: dataUrl,
                storage: 'inline_fallback',
                warning: cloudErr.message,
              });
            }
            return res.status(500).json({
              error: `Cloudinary upload failed (${cloudErr.message}). Set CLOUDINARY_URL / keys on Render.`,
            });
          }
        }
      }

      if (kind === 'icon' && dataUrl.length <= 650000) {
        return res.json({ ok: true, url: dataUrl, storage: 'inline_fallback' });
      }
      return res.status(400).json({ error: 'Cloudinary not configured or file too large for inline fallback.' });
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
  // 3. MEDAL CATALOG CRUD + GRANT/REVOKE + A-TO-Z LOGIC ENGINE
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
    const linkedEventId = String(req.body.linkedEventId ?? prev.linkedEventId ?? '').trim();
    const linkedEventTitle = String(req.body.linkedEventTitle ?? prev.linkedEventTitle ?? '').trim();
    const metricMode = String(req.body.metricMode ?? prev.metricMode ?? 'COINS').trim().toUpperCase() === 'COUNT'
      ? 'COUNT'
      : 'COINS';

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
      || buildDefaultConditionText(linkType, starThresholds, { linkedGiftName, linkedEventTitle, metricMode });
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
      metricMode,
      linkedGiftId,
      linkedGiftName,
      linkedEventId,
      linkedEventTitle,
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
  // 4. A-TO-Z MEDAL EVALUATION ENGINE
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

  function onUserLogin(uid) {
    const cleanUid = String(uid || '').trim();
    if (!cleanUid) return;
    const prof = users.get(cleanUid);
    if (!prof) return;
    const todayKey = getTodayDateKeyIst();
    const keys = Array.isArray(prof.loginDateKeys) ? [...prof.loginDateKeys] : [];
    if (!keys.includes(todayKey)) {
      keys.push(todayKey);
      prof.loginDateKeys = keys.slice(-365);
      users.set(cleanUid, prof);
      firestoreStore.markUserDirty(cleanUid);
      evaluateMedalsForUser(
        cleanUid,
        (m) => String(m.linkType || '').toUpperCase() === 'LOGIN_DAYS',
        prof.loginDateKeys.length,
        { isAdditive: false }
      );
    } else {
      // Ensure newly added LOGIN_DAYS medals (e.g. 1-day First Login) also evaluate against existing login days
      evaluateMedalsForUser(
        cleanUid,
        (m) => String(m.linkType || '').toUpperCase() === 'LOGIN_DAYS',
        Math.max(1, keys.length),
        { isAdditive: false }
      );
    }
  }

  function onDailyCheckIn(uid) {
    evaluateMedalsForUser(
      uid,
      (m) => String(m.linkType || '').toUpperCase() === 'DAILY_CHECKIN_COUNT',
      1,
      { isAdditive: true }
    );
  }

  function onGamePlayed(uid, roundNumber) {
    const cleanUid = String(uid || '').trim();
    if (!cleanUid) return;
    const prof = users.get(cleanUid);
    if (!prof) return;
    const r = Math.floor(Number(roundNumber || 0));
    if (r > 0 && Number(prof.lastGreedyPlayedRound || 0) === r) {
      return; // Already counted this game round for this user
    }
    if (r > 0) {
      prof.lastGreedyPlayedRound = r;
      users.set(cleanUid, prof);
      firestoreStore.markUserDirty(cleanUid);
    }
    evaluateMedalsForUser(
      cleanUid,
      (m) => String(m.linkType || '').toUpperCase() === 'GAME_PLAYS_COUNT',
      1,
      { isAdditive: true }
    );
  }

  function onGameRoundWon(uid) {
    evaluateMedalsForUser(
      uid,
      (m) => String(m.linkType || '').toUpperCase() === 'GAME_WINS_COUNT',
      1,
      { isAdditive: true }
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

  function onEventParticipated(uid, eventId = '', eventTitle = '') {
    const cleanUid = String(uid || '').trim();
    if (!cleanUid) return;
    const prof = users.get(cleanUid);
    if (!prof) return;
    const evId = String(eventId || '').trim();
    const evTitle = String(eventTitle || '').trim();
    const todayKey = getTodayDateKeyIst();
    const dedupKey = `${evId || evTitle || 'any'}_${todayKey}`;
    const seen = Array.isArray(prof.eventParticipatedKeys) ? [...prof.eventParticipatedKeys] : [];
    if (seen.includes(dedupKey)) return;
    seen.push(dedupKey);
    prof.eventParticipatedKeys = seen.slice(-200);
    users.set(cleanUid, prof);
    firestoreStore.markUserDirty(cleanUid);

    evaluateMedalsForUser(
      cleanUid,
      (m) => {
        if (String(m.linkType || '').toUpperCase() !== 'EVENT_PARTICIPATION') return false;
        const mEvId = String(m.linkedEventId || '').trim().toLowerCase();
        const mEvTitle = String(m.linkedEventTitle || '').trim().toLowerCase();
        if (!mEvId && !mEvTitle) return true; // Any event
        if (mEvId && evId && mEvId === evId.toLowerCase()) return true;
        if (mEvTitle && evTitle && evTitle.toLowerCase().includes(mEvTitle)) return true;
        return false;
      },
      1,
      { isAdditive: true }
    );
  }

  function onRoomJoined(uid, roomId) {
    const cleanUid = String(uid || '').trim();
    if (!cleanUid || !roomId) return;
    const prof = users.get(cleanUid);
    if (!prof) return;
    const key = `${String(roomId).trim()}_${getTodayDateKeyIst()}`;
    const seen = Array.isArray(prof.roomJoinKeys) ? [...prof.roomJoinKeys] : [];
    if (seen.includes(key)) return;
    seen.push(key);
    prof.roomJoinKeys = seen.slice(-200);
    users.set(cleanUid, prof);
    firestoreStore.markUserDirty(cleanUid);

    evaluateMedalsForUser(
      cleanUid,
      (m) => String(m.linkType || '').toUpperCase() === 'ROOM_JOIN_COUNT',
      1,
      { isAdditive: true }
    );
  }

  function onCharmUpdated(uid, totalCharmPoints) {
    if (totalCharmPoints <= 0) return;
    evaluateMedalsForUser(
      uid,
      (m) => String(m.linkType || '').toUpperCase() === 'CHARM_POINTS',
      totalCharmPoints,
      { isAdditive: false }
    );
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
    onUserLogin,
    onDailyCheckIn,
    onGamePlayed,
    onGameRoundWon,
    onGameCoinsWon,
    onEventParticipated,
    onRoomJoined,
    onCharmUpdated,
    onRechargeCoins,
    onLevelUpdated,
    onFriendInvited,
    validateStorePurchase,
    purgeLegacyDefaults,
  };
}

module.exports = {
  initStoreAndMedalCatalog,
};
