// Firestore -> JSON 스냅샷 내보내기 (GitHub Actions 에서 약 10분마다 실행)
// - 외부 패키지 없이 Node 20 기본 기능 + Firestore REST API 만 사용 (무료)
// - 읽기(Read) 비용 절약: 평소에는 '변경된 문서만' 가져오고, 문서 수가 달라지면 그 컬렉션만 전체 재동기화,
//   하루에 한 번은 전체 재동기화 (Firestore 무료 한도: 하루 읽기 50,000건)
// - transferMemos(비밀번호 포함)는 내보내지 않음
import fs from "node:fs";
import path from "node:path";

const DATA_DIR = process.env.DATA_DIR || "data";
const CONFIG_JS = process.env.CONFIG_JS || "config.js";
const API_BASE = process.env.FIRESTORE_API_BASE || "https://firestore.googleapis.com/v1";
const INCREMENTAL = ["schedules", "custom_events"]; // 변경분만 가져올 컬렉션 (updatedAt 기준)
const ALWAYS_FULL = ["settings", "notice"];          // 문서가 몇 개 안 되어 매번 전체
const FULL_EVERY_MS = 26 * 60 * 60 * 1000;           // 마지막 전체 동기화 후 26시간 지나면 전체 재동기화
const OVERLAP_MS = 2 * 60 * 60 * 1000;               // 변경분 조회 시 2시간 겹쳐서 조회 (PC 시계 오차 대비)

function loadConfig() {
  let projectId = process.env.FIREBASE_PROJECT_ID;
  let apiKey = process.env.FIREBASE_API_KEY;
  if (!projectId || !apiKey) {
    const text = fs.readFileSync(CONFIG_JS, "utf8");
    const win = {};
    const cfg = new Function("window", text + '\n;return (typeof firebaseConfig!=="undefined")?firebaseConfig:window.firebaseConfig;')(win);
    projectId = projectId || (cfg && cfg.projectId);
    apiKey = apiKey || (cfg && cfg.apiKey);
  }
  if (!projectId || !apiKey) throw new Error("projectId / apiKey 를 찾을 수 없습니다. config.js 또는 시크릿(FIREBASE_PROJECT_ID, FIREBASE_API_KEY)을 확인하세요.");
  return { projectId, apiKey };
}
const { projectId, apiKey } = loadConfig();
const ROOT = `${API_BASE}/projects/${projectId}/databases/(default)/documents`;

async function api(url, body) {
  const headers = { "content-type": "application/json" };
  if (process.env.FIREBASE_REFERER) headers.Referer = process.env.FIREBASE_REFERER; // API 키에 웹사이트 제한을 걸어둔 경우
  let lastErr;
  for (let attempt = 1; attempt <= 4; attempt++) {
    try {
      const res = await fetch(url, body ? { method: "POST", headers, body: JSON.stringify(body) } : { headers });
      if (res.ok) return await res.json();
      const msg = `${res.status} ${(await res.text()).slice(0, 300)}`;
      if (res.status < 500 && res.status !== 429) throw Object.assign(new Error(msg), { fatal: true });
      lastErr = new Error(msg);
    } catch (e) {
      if (e.fatal) throw e;
      lastErr = e;
    }
    await new Promise(r => setTimeout(r, 1000 * attempt));
  }
  throw lastErr;
}

// Firestore REST 값 -> 일반 JSON (Timestamp 는 {"__ts": 밀리초} 로 저장, 화면 코드에서 다시 복원)
function dec(v) {
  if ("stringValue" in v) return v.stringValue;
  if ("integerValue" in v) return Number(v.integerValue);
  if ("doubleValue" in v) return typeof v.doubleValue === "number" ? v.doubleValue : Number(v.doubleValue);
  if ("booleanValue" in v) return v.booleanValue;
  if ("nullValue" in v) return null;
  if ("timestampValue" in v) return { __ts: Date.parse(v.timestampValue) };
  if ("arrayValue" in v) return (v.arrayValue.values || []).map(dec);
  if ("mapValue" in v) return decFields(v.mapValue.fields || {});
  if ("geoPointValue" in v) return v.geoPointValue;
  if ("referenceValue" in v) return v.referenceValue;
  if ("bytesValue" in v) return v.bytesValue;
  return null;
}
function decFields(f) { const o = {}; for (const k of Object.keys(f)) o[k] = dec(f[k]); return o; }
const idOf = d => d.name.split("/").pop();

async function fetchAll(col) {
  const docs = {};
  let token = "";
  do {
    const j = await api(`${ROOT}/${col}?pageSize=300&key=${apiKey}` + (token ? `&pageToken=${encodeURIComponent(token)}` : ""));
    for (const d of j.documents || []) docs[idOf(d)] = decFields(d.fields || {});
    token = j.nextPageToken || "";
  } while (token);
  return docs;
}
async function countDocs(col) {
  const r = await api(`${ROOT}:runAggregationQuery?key=${apiKey}`, {
    structuredAggregationQuery: { structuredQuery: { from: [{ collectionId: col }] }, aggregations: [{ alias: "n", count: {} }] }
  });
  return Number(r[0].result.aggregateFields.n.integerValue);
}
async function fetchChanged(col, sinceISO) {
  const r = await api(`${ROOT}:runQuery?key=${apiKey}`, {
    structuredQuery: {
      from: [{ collectionId: col }],
      where: { fieldFilter: { field: { fieldPath: "updatedAt" }, op: "GREATER_THAN_OR_EQUAL", value: { timestampValue: sinceISO } } }
    }
  });
  const docs = {};
  for (const x of r) if (x.document) docs[idOf(x.document)] = decFields(x.document.fields || {});
  return docs;
}

function serialize(docs) {
  const ids = Object.keys(docs).sort();
  return '{"docs":{\n' + ids.map(id => JSON.stringify(id) + ":" + JSON.stringify(docs[id])).join(",\n") + "\n}}\n";
}
function readText(f) { try { return fs.readFileSync(f, "utf8"); } catch { return null; } }
function readDocs(col) { const t = readText(path.join(DATA_DIR, col + ".json")); try { return t ? JSON.parse(t).docs || {} : null; } catch { return null; } }

fs.mkdirSync(DATA_DIR, { recursive: true });
const now = new Date();
const metaText = readText(path.join(DATA_DIR, "meta.json"));
let prevMeta = {};
try { prevMeta = metaText ? JSON.parse(metaText) : {}; } catch {}
const lastFull = prevMeta.lastFullAt ? Date.parse(prevMeta.lastFullAt) : 0;
const lastSync = prevMeta.syncedAt ? Date.parse(prevMeta.syncedAt) : 0;
const forceFull = process.env.FULL_SYNC === "1" || now.getTime() - lastFull > FULL_EVERY_MS || !lastSync;
const sinceISO = new Date(lastSync - OVERLAP_MS).toISOString();
console.log(`모드: ${forceFull ? "전체 동기화" : "변경분 동기화(since " + sinceISO + ")"}`);

let changedAny = false;
const counts = {};
async function save(col, docs) {
  const old = readDocs(col);
  if (old && Object.keys(old).length > 5 && Object.keys(docs).length === 0 && process.env.FULL_SYNC !== "1")
    throw new Error(`${col}: 기존 ${Object.keys(old).length}건인데 결과가 0건입니다. 권한/설정 문제로 판단해 덮어쓰지 않습니다. (정말 비운 거라면 수동 실행에서 '전체 재동기화' 체크)`);
  const text = serialize(docs);
  const file = path.join(DATA_DIR, col + ".json");
  if (readText(file) !== text) { fs.writeFileSync(file, text); changedAny = true; console.log(`  ${col}: 변경됨 (${Object.keys(docs).length}건)`); }
  else console.log(`  ${col}: 변경 없음 (${Object.keys(docs).length}건)`);
  counts[col] = Object.keys(docs).length;
}

for (const col of ALWAYS_FULL) await save(col, await fetchAll(col));
for (const col of INCREMENTAL) {
  const old = readDocs(col);
  let docs;
  if (forceFull || !old) {
    docs = await fetchAll(col);
  } else {
    const serverCount = await countDocs(col);
    docs = { ...old, ...(await fetchChanged(col, sinceISO)) };
    if (Object.keys(docs).length !== serverCount) {
      console.log(`  ${col}: 문서 수 불일치(JSON ${Object.keys(docs).length} / 서버 ${serverCount}) → 전체 재동기화`);
      docs = await fetchAll(col);
    }
  }
  await save(col, docs);
}

// meta.json: 데이터가 바뀌었거나, 전체 동기화이거나, 마지막 기록 후 1시간 이상 지났을 때만 갱신 (불필요한 커밋 방지)
const hourOf = iso => (iso || "").slice(0, 13);
if (changedAny || forceFull || hourOf(prevMeta.syncedAt) !== hourOf(now.toISOString())) {
  const meta = { syncedAt: now.toISOString(), lastFullAt: forceFull ? now.toISOString() : prevMeta.lastFullAt, counts };
  const text = JSON.stringify(meta, null, 2) + "\n";
  if (text !== metaText) fs.writeFileSync(path.join(DATA_DIR, "meta.json"), text);
}
console.log("완료");
