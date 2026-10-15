/* Processing worker: decompresses one archive scan, reads it with h5wasm and builds the 3D data. */
importScripts('https://cdn.jsdelivr.net/npm/h5wasm@0.10.3/dist/iife/h5wasm.js', 'radarcore.js');
let Mod = null;
const ready = h5wasm.ready.then(m => (Mod = m));
self.onmessage = async (e) => {
  const { id, buf, method, opt } = e.data;
  try {
    await ready;
    let bytes = new Uint8Array(buf);
    if (method === 8) bytes = new Uint8Array(await new Response(new Blob([bytes]).stream().pipeThrough(new DecompressionStream('deflate-raw'))).arrayBuffer());
    else if (method !== 0) throw new Error('This scan uses a compression type the app can\'t read (' + method + ').');
    const path = '/scan' + id + '.h5';
    Mod.FS.writeFile(path, bytes);
    let vol;
    try { const f = new h5wasm.File(path, 'r'); vol = RadarCore.readVolume(f); f.close(); }
    finally { try { Mod.FS.unlink(path); } catch (_) {} }
    if (!vol.sweeps.length) throw new Error('No reflectivity in this scan.');
    const out = RadarCore.process(vol, opt);
    const tr = [];
    for (const t of out.tilts) tr.push(t.z.buffer, t.v.buffer, t.c.buffer, t.h.buffer);
    tr.push(out.vol.z.buffer, out.vol.v.buffer, out.vol.c.buffer);
    for (const m of Object.values(out.meshes)) tr.push(m.pos.buffer, m.idx.buffer);
    for (const k of ['z', 'v', 'c']) if (out.polar[k]) tr.push(out.polar[k].q.buffer);
    self.postMessage({ id, ok: true, out, meta: vol.meta }, tr);
  } catch (err) {
    self.postMessage({ id, ok: false, error: String(err && err.message || err) });
  }
};
