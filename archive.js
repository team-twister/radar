/* Archive: reads BoM radar volumes straight from the AURA archive on NCI's THREDDS server.
   Each radar-day is one big zip; we read only its index, then download single scans with byte ranges. */
const NCI = 'https://thredds.nci.org.au/thredds';
const Archive = (() => {
  const r32 = (b, o) => (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16)) + b[o + 3] * 16777216;
  const r16 = (b, o) => b[o] | (b[o + 1] << 8);
  const r64 = (b, o) => r32(b, o) + r32(b, o + 4) * 4294967296;

  async function range(url, start, end, signal) {   // inclusive end; exact "bytes=a-b" form needs no CORS preflight
    const res = await fetch(url, { headers: { Range: `bytes=${start}-${end}` }, signal, cache: 'no-store' });
    if (res.status === 404) throw new Error('not-found');
    if (res.status !== 206) { await res.body?.cancel(); throw new Error(`The archive didn't return part of the file (status ${res.status}).`); }
    return new Uint8Array(await res.arrayBuffer());
  }
  async function rangeParallel(url, start, end, onProgress, signal) {
    const total = end - start + 1, parts = total > 4e6 ? 4 : total > 1.5e6 ? 2 : 1, size = Math.ceil(total / parts);
    let done = 0;
    const jobs = [];
    for (let p = 0; p < parts; p++) {
      const a = start + p * size, b = Math.min(end, a + size - 1);
      jobs.push(range(url, a, b, signal).then(buf => { done += buf.length; onProgress && onProgress(done / total); return buf; }));
    }
    const bufs = await Promise.all(jobs);
    if (bufs.length === 1) return bufs[0];
    const out = new Uint8Array(bufs.reduce((n, b) => n + b.length, 0)); let o = 0;
    for (const b of bufs) { out.set(b, o); o += b.length; }
    return out;
  }

  /* ---- radar site list ---- */
  function parseCSV(text) {
    const lines = text.replace(/\r/g, '').split('\n').filter(Boolean), head = lines.shift().split(',').map(s => s.trim());
    return lines.map(l => { const c = l.split(','); const o = {}; head.forEach((h, i) => o[h] = (c[i] || '').trim()); return o; });
  }
  const pdate = s => { const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(s || ''); return m ? new Date(Date.UTC(+m[3], +m[2] - 1, +m[1])) : null; };
  async function sites() {
    const res = await fetch(`${NCI}/fileServer/rq0/radar_site_list.csv`);
    if (!res.ok) throw new Error(`Couldn't load the radar list (status ${res.status}).`);
    const rows = parseCSV(await res.text());
    const byId = new Map();
    for (const r of rows) {
      const id = parseInt(r.id, 10), lat = parseFloat(r.site_lat), lon = parseFloat(r.site_lon);
      if (!id || !isFinite(lat) || !isFinite(lon)) continue;
      const ep = { start: pdate(r.postchange_start), end: pdate(r.prechange_end), lat, lon, type: r.radar_type, band: r.band,
        doppler: /yes/i.test(r.doppler), dp: /yes/i.test(r.dp), status: r.status };
      if (!byId.has(id)) byId.set(id, { id, name: r.location || r.short_name || `Radar ${id}`, short: r.short_name, state: r.state, epochs: [] });
      byId.get(id).epochs.push(ep);
    }
    const list = [...byId.values()];
    for (const s of list) {
      s.epochs.sort((a, b) => (a.start || 0) - (b.start || 0));
      const last = s.epochs.at(-1); s.lat = last.lat; s.lon = last.lon; s.current = !last.end;
      s.first = s.epochs[0].start;
    }
    return list.sort((a, b) => (a.state || '').localeCompare(b.state || '') || a.name.localeCompare(b.name));
  }
  function epochOn(site, date) {   // equipment in use on a given day (for dual-pol / Doppler)
    return site.epochs.find(e => (!e.start || e.start <= date) && (!e.end || e.end >= date)) || site.epochs.at(-1);
  }

  /* ---- one radar-day: find the zip's size and read its table of contents ---- */
  const dayURL = (id, d) => `${NCI}/fileServer/rq0/${id}/${d.slice(0, 4)}/vol/${id}_${d}.pvol.zip`;
  async function approxSize(id, d) {
    const res = await fetch(`${NCI}/catalog/rq0/${id}/${d.slice(0, 4)}/vol/catalog.xml`);
    if (!res.ok) return null;
    const xml = await res.text(), i = xml.indexOf(`${id}_${d}.pvol.zip`);
    if (i < 0) throw new Error('not-found');
    const m = /<dataSize[^>]*units="([^"]+)"[^>]*>\s*([\d.]+)/.exec(xml.slice(i, i + 1500)) || /<dataSize[^>]*>\s*([\d.]+)/.exec(xml.slice(i, i + 1500));
    if (!m) return null;
    const unit = m.length === 3 ? m[1] : 'bytes', val = parseFloat(m.length === 3 ? m[2] : m[1]);
    const mult = /gbyte/i.test(unit) ? 1e9 : /mbyte/i.test(unit) ? 1e6 : /kbyte/i.test(unit) ? 1e3 : 1;
    return { size: val * mult, exact: mult === 1 };
  }
  async function openDay(id, d) {     // d = 'YYYYMMDD'
    const url = dayURL(id, d);
    let size = null;
    try { const h = await fetch(url, { method: 'HEAD' }); if (h.status === 404) throw new Error('not-found'); const n = +h.headers.get('Content-Length'); if (n > 0) size = n; }
    catch (e) { if (e.message === 'not-found') throw e; }
    let tail, tailStart;
    if (size) { tailStart = Math.max(0, size - 70000); tail = await range(url, tailStart, size - 1); }
    else {
      const ap = await approxSize(id, d);
      if (!ap) throw new Error("Couldn't work out the archive file size.");
      const slack = ap.exact ? 70000 : ap.size * 0.004 + 70000;
      tailStart = Math.max(0, Math.floor(ap.size - slack)); tail = await range(url, tailStart, Math.ceil(ap.size + slack));
      size = tailStart + tail.length;               // the server stops at the end of the file
    }
    // end-of-central-directory record
    let e = -1; for (let i = tail.length - 22; i >= 0; i--) if (tail[i] === 0x50 && tail[i + 1] === 0x4b && tail[i + 2] === 5 && tail[i + 3] === 6) { e = i; break; }
    if (e < 0) throw new Error("Couldn't find the index of this day's file.");
    let cdCount = r16(tail, e + 10), cdSize = r32(tail, e + 12), cdOff = r32(tail, e + 16);
    if (cdOff === 0xffffffff || cdSize === 0xffffffff || cdCount === 0xffff) {     // ZIP64
      const l = e - 20;
      if (l >= 0 && r32(tail, l) === 0x07064b50) {
        const z64 = r64(tail, l + 8);
        const rec = z64 >= tailStart ? tail.subarray(z64 - tailStart) : await range(url, z64, z64 + 55);
        cdCount = r64(rec, 32); cdSize = r64(rec, 40); cdOff = r64(rec, 48);
      }
    }
    const cd = cdOff >= tailStart ? tail.subarray(cdOff - tailStart, cdOff - tailStart + cdSize) : await range(url, cdOff, cdOff + cdSize - 1);
    const entries = []; let p = 0;
    while (p + 46 <= cd.length && r32(cd, p) === 0x02014b50) {
      const method = r16(cd, p + 10); let csize = r32(cd, p + 20), usize = r32(cd, p + 24);
      const nlen = r16(cd, p + 28), xlen = r16(cd, p + 30), clen = r16(cd, p + 32); let lho = r32(cd, p + 42);
      const name = new TextDecoder().decode(cd.subarray(p + 46, p + 46 + nlen));
      let x = p + 46 + nlen; const xend = x + xlen;
      while (x + 4 <= xend) {                       // ZIP64 extra field
        const hid = r16(cd, x), hl = r16(cd, x + 2); let q = x + 4;
        if (hid === 1) { if (usize === 0xffffffff) { usize = r64(cd, q); q += 8; } if (csize === 0xffffffff) { csize = r64(cd, q); q += 8; } if (lho === 0xffffffff) { lho = r64(cd, q); q += 8; } }
        x += 4 + hl;
      }
      const m = /_(\d{8})_(\d{6})\.pvol\.h5$/.exec(name);
      if (m) {
        const t = Date.UTC(+m[1].slice(0, 4), +m[1].slice(4, 6) - 1, +m[1].slice(6, 8), +m[2].slice(0, 2), +m[2].slice(2, 4), +m[2].slice(4, 6));
        entries.push({ name, time: t, method, csize, usize, lho, nlen });
      }
      p = xend + clen;
    }
    entries.sort((a, b) => a.time - b.time);
    return { url, size, entries };
  }
  async function getScan(day, entry, onProgress, signal) {
    const end = Math.min(day.size - 1, entry.lho + 30 + entry.nlen + entry.csize + 2048);
    const buf = await rangeParallel(day.url, entry.lho, end, onProgress, signal);
    if (r32(buf, 0) !== 0x04034b50) throw new Error('This scan in the archive looks damaged.');
    const s = 30 + r16(buf, 26) + r16(buf, 28);
    return { bytes: buf.slice(s, s + entry.csize), method: entry.method };
  }
  return { sites, epochOn, openDay, getScan, dayURL };
})();
if (typeof self !== 'undefined') self.Archive = Archive;
