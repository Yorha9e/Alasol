/**
 * alasol 最小 TypeScript 消费端：逐行读取 NDJSON 流，按 contracts/schema 做跨语言契约校验。
 *
 * 运行方式：
 *   cd tools/ts-consumer && npm ci && npx tsx reader.ts
 *   （事件流样本：npx tsx reader.ts sample-events.ndjson；坏样本：npx tsx reader.ts broken-events.ndjson）
 *   （传输消息样本：npx tsx reader.ts sample-transport.ndjson；坏样本：npx tsx reader.ts broken-transport.ndjson）
 *
 * 退出码：所有行都通过校验 → 0；任一行解析/校验失败 → 1。
 *
 * 设计要点：
 * - contracts/schema 下的 JSON Schema（2020-12，$defs）各自带 $id（见各文件；命名空间前缀见
 *   contracts/INVENTORY.md §3）。ajv（2020-12 类）注册后，跨文件相对 $ref 按各文件 $id 命名空间自解析，
 *   无需消费端注入 $id。schema 目录相对本文件定位（../../contracts/schema），只读不写。
 * - **契约只有一处定义**：消费端不硬编码任何 kind、payload 类型名或 $id 前缀 ——
 *   - kind→payload 映射来自 schema 的 x- 注记（机器可读约定）：事件档案读事件 schema 的
 *     $defs.Event["x-kind-payload"]，传输档案读传输 schema 的 $defs.TransportMessage["x-kind-payload"]；
 *     映射的值可以是**纯 $defs 名**（在全部已注册 schema 的 $defs 里查找，命中唯一才用，
 *     命中多处或零命中 → 明确报错点名），也可以是**显式 ref**（如 "approval.json#/$defs/ApprovalRequest"）；
 *   - 命名空间 base 来自已注册 schema 的实际 $id（反推其目录部分）；
 *   - envelope = 档案对应 schema 的 $defs；payload = 映射指向的 $defs。
 *     schema 改了（新增 kind / 换 $id / 换 payload 形状），消费端不用改。
 * - envelope 必填字段与 ULID 正则由 schema 自己声明（消费端不重复写规则）。
 * - 未知 kind 宽容跳过 payload 校验（D7：未知 kind 忽略但持久化），envelope 合法即算该行通过。
 * - tool_call_id 是 provider 不透明字符串（common.json#/$defs/ProviderId），与 Event.id（ULID）永不共字段。
 * - **两个档案**（profile）：事件流（events）与传输消息（transport）。默认按**文件名**推断
 *   （含 "transport" → 传输档案，否则事件档案），也可显式 `--profile events|transport`。
 *   传输消息的 id/reply_to 是**客户端关联 id**（任意不透明字符串；服务端只回指、不校验）——
 *   所以 transport.json 的 id 不套 ULID 正则，样本里就故意用了非 ULID 的 id。
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Ajv from "ajv/dist/2020";
import type { ErrorObject, ValidateFunction } from "ajv";
import addFormats from "ajv-formats";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SCHEMA_DIR = path.resolve(HERE, "..", "..", "contracts", "schema");

// ---------------------------------------------------------------------------
// 1. 读取并注册全部 schema：$id 由 schema 自带（绝对命名空间），跨文件 $ref 自解析
// ---------------------------------------------------------------------------
interface LoadedSchema {
  /** 文件名（schema 目录内） */
  file: string;
  /** schema 自带的 $id（绝对命名空间） */
  id: string;
  /** 解析后的 schema 文档 */
  doc: Record<string, unknown>;
}

function loadSchemas(dir: string): LoadedSchema[] {
  const files = fs
    .readdirSync(dir)
    .filter((name) => name.endsWith(".json"))
    .sort();
  if (files.length === 0) throw new Error(`no schema files found in ${dir}`);
  return files.map((file) => {
    const doc = JSON.parse(fs.readFileSync(path.join(dir, file), "utf8")) as Record<string, unknown>;
    const id = typeof doc.$id === "string" ? doc.$id : "";
    if (id === "") throw new Error(`schema ${file} has no $id: the namespace cannot be derived`);
    return { file, id, doc };
  });
}

/**
 * namespaceBase 从已注册 schema 的实际 $id 反推命名空间 base（$id 的目录部分）。
 * 不硬编码任何 URL 前缀：schema 换域名时消费端不用改。
 */
function namespaceBase(schemas: LoadedSchema[]): string {
  const bases = new Set<string>();
  for (const s of schemas) {
    const cut = s.id.lastIndexOf("/");
    if (cut < 0) throw new Error(`schema ${s.file}: $id "${s.id}" has no namespace base`);
    bases.add(s.id.slice(0, cut + 1));
  }
  if (bases.size !== 1) {
    throw new Error(`the schemas do not share one namespace: ${[...bases].sort().join(", ")}`);
  }
  return [...bases][0];
}

const schemas = loadSchemas(SCHEMA_DIR);
const BASE = namespaceBase(schemas);

const ajv = new Ajv({ allErrors: true, strict: false }); // strict=false：容忍 x-go-type/x-id-kind 等自定义关键字
addFormats(ajv); // 提供 format: "date-time"（ts 字段）

for (const s of schemas) {
  ajv.addSchema(s.doc);
}

// ---------------------------------------------------------------------------
// 2. 校验档案（profile）：事件流 vs 传输消息
// ---------------------------------------------------------------------------

/**
 * Profile 是一条流的校验档案：envelope 的 $defs + kind→payload 映射。
 *
 * 两个档案由 schema 自己声明（x-kind-payload 注记）：
 *   - events   ：$defs.Event            （事件流）
 *   - transport：$defs.TransportMessage  （WS+NDJSON 传输消息）
 */
interface Profile {
  /** 档案名（events / transport） */
  name: string;
  /** schema 文件名（envelope 所在处） */
  file: string;
  /** envelope 的 $defs 名 */
  def: string;
  /** kind → payload $defs 名 */
  kindPayload: Record<string, string>;
}

/**
 * loadProfile 找出带 $defs.<def>["x-kind-payload"] 的那一份 schema（唯一定义处）。
 * 找不到 / 找到多份 → 明确报错点名（不猜）。
 */
function loadProfile(candidates: LoadedSchema[], def: string, name: string): Profile {
  const found = candidates.filter((s) => {
    const defs = (s.doc.$defs ?? {}) as Record<string, unknown>;
    const d = defs[def];
    return typeof d === "object" && d !== null && "x-kind-payload" in d;
  });
  if (found.length !== 1) {
    throw new Error(
      `expected exactly one schema with $defs.${def}["x-kind-payload"], found ${found.length}: ` +
        found.map((s) => s.file).join(", "),
    );
  }
  const schema = found[0];
  const defs = (schema.doc.$defs ?? {}) as Record<string, unknown>;
  const d = defs[def] as Record<string, unknown>;
  const kindPayload = (d["x-kind-payload"] ?? {}) as Record<string, string>;
  return { name, file: schema.file, def, kindPayload };
}

const eventProfile = loadProfile(schemas, "Event", "events");
const transportProfile = loadProfile(schemas, "TransportMessage", "transport");

/**
 * 全部已注册 schema 的 $defs 名字索引：名字 → 位置（同名可能多处）。
 *
 * x-kind-payload 的值有两种写法：
 *   - 纯名字（如 "ToolCallIntentPayload"）：在**所有**已注册 schema 的 $defs 里查找
 *     （payload 的形状不一定定义在事件 schema 里）；
 *   - 显式 ref（如 "policy.json#/$defs/Effect"）：直接按 ref 解析。
 */
function indexDefs(candidates: LoadedSchema[]): Map<string, DefLocation[]> {
  const index = new Map<string, DefLocation[]>();
  for (const s of candidates) {
    const defs = (s.doc.$defs ?? {}) as Record<string, unknown>;
    for (const name of Object.keys(defs)) {
      const loc: DefLocation = { file: s.file, ref: `${s.file}#/$defs/${name}` };
      const hits = index.get(name);
      if (hits) hits.push(loc);
      else index.set(name, [loc]);
    }
  }
  return index;
}

/** 一个 $defs 条目的位置：它在哪个文件里。 */
interface DefLocation {
  /** 文件名（schema 目录内） */
  file: string;
  /** 绝对 ref（相对命名空间 base 解析） */
  ref: string;
}

/**
 * payload 的 $defs 名 → 校验用的 ref。
 *
 * ① 带 "#" 或 "/" 的是显式 ref（如 "policy.json#/$defs/Effect"），直接用；
 * ② 纯名字在所有已注册 schema 的 $defs 里查找：唯一命中就用它，
 *    命中多处或零命中 → 明确报错（点名），不猜。
 */
function payloadRef(defName: string, defIndex: Map<string, DefLocation[]>): string {
  if (defName.includes("/") || defName.includes("#")) return defName; // 显式 ref
  const hits = defIndex.get(defName);
  if (!hits || hits.length === 0) {
    throw new Error(
      `x-kind-payload points to "${defName}", which is not a $defs entry of any registered schema ` +
        `(use an explicit ref like "policy.json#/$defs/${defName}" to point at another file)`,
    );
  }
  if (hits.length > 1) {
    throw new Error(
      `x-kind-payload points to "${defName}", which is defined in several schemas: ` +
        hits.map((h) => h.file).join(", ") +
        ` (use an explicit ref to pick one)`,
    );
  }
  return hits[0].ref;
}

// ---------------------------------------------------------------------------
// 3. 校验器：envelope + 已知 kind 的 payload
// ---------------------------------------------------------------------------
function mustGet(ref: string): ValidateFunction {
  const key = new URL(ref, BASE).href; // "event.json#/$defs/Event" → 绝对 key
  const validate = ajv.getSchema(key);
  if (!validate) throw new Error(`schema not found (check $id registration): ${key}`);
  return validate;
}

const defIndex = indexDefs(schemas);

/** 一个档案的校验器集合：envelope + kind→payload。 */
interface ProfileValidators {
  envelope: ValidateFunction;
  payloads: Map<string, ValidateFunction>;
}

function profileValidators(p: Profile): ProfileValidators {
  const envelope = mustGet(`${p.file}#/$defs/${p.def}`);
  const payloads = new Map<string, ValidateFunction>();
  for (const [kind, defName] of Object.entries(p.kindPayload)) {
    payloads.set(kind, mustGet(payloadRef(defName, defIndex)));
  }
  return { envelope, payloads };
}

const validatorsByProfile = new Map<string, ProfileValidators>([
  [eventProfile.name, profileValidators(eventProfile)],
  [transportProfile.name, profileValidators(transportProfile)],
]);

function formatErrors(errors: ErrorObject[] | null | undefined): string {
  if (!errors || errors.length === 0) return "unknown validation error";
  return errors
    .map((e) => `${e.instancePath === "" ? "<root>" : e.instancePath} ${e.message ?? "invalid"}`)
    .join("; ");
}

function kindOf(value: unknown): string | undefined {
  if (typeof value === "object" && value !== null && "kind" in value) {
    const kind = (value as { kind?: unknown }).kind;
    return typeof kind === "string" ? kind : undefined;
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// 4. 逐行处理
// ---------------------------------------------------------------------------
interface LineResult {
  line: number;
  ok: boolean;
  kind?: string;
  detail: string;
}

function processFile(file: string, profile: Profile): LineResult[] {
  const { envelope, payloads } = validatorsByProfile.get(profile.name)!;
  const text = fs.readFileSync(file, "utf8");
  const results: LineResult[] = [];
  text.split(/\r?\n/).forEach((raw, index) => {
    if (raw.trim() === "") return; // 容忍末尾/中间空行
    const line = index + 1;

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch (err) {
      results.push({ line, ok: false, detail: `invalid JSON: ${(err as Error).message}` });
      return;
    }

    const kind = kindOf(parsed);

    if (!envelope(parsed)) {
      results.push({ line, ok: false, kind, detail: `envelope FAIL: ${formatErrors(envelope.errors)}` });
      return;
    }

    const payloadValidator = kind !== undefined ? payloads.get(kind) : undefined;
    if (!payloadValidator) {
      // 未知 kind 宽容跳过 payload 校验（D7）
      results.push({ line, ok: true, kind, detail: "envelope OK; payload skipped (unknown kind)" });
      return;
    }

    const payload = (parsed as { payload?: unknown }).payload;
    if (!payloadValidator(payload)) {
      results.push({
        line,
        ok: false,
        kind,
        detail: `envelope OK; payload FAIL: ${formatErrors(payloadValidator.errors)}`,
      });
      return;
    }
    results.push({ line, ok: true, kind, detail: "envelope OK; payload OK" });
  });
  return results;
}

// ---------------------------------------------------------------------------
// 5. 参数与 main：逐行结果 + 汇总，按结果设置退出码
// ---------------------------------------------------------------------------

/** 档案选择：显式 --profile 优先，否则按文件名推断（含 "transport" → 传输档案）。 */
function pickProfile(explicit: string, file: string): Profile {
  if (explicit !== "") {
    if (explicit === eventProfile.name) return eventProfile;
    if (explicit === transportProfile.name) return transportProfile;
    throw new Error(`unknown profile "${explicit}" (want ${eventProfile.name} | ${transportProfile.name})`);
  }
  return path.basename(file).includes("transport") ? transportProfile : eventProfile;
}

function parseArgs(argv: string[]): { file: string; profile: string } {
  let profile = "";
  const files: string[] = [];
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--profile" || arg === "-p") {
      profile = argv[++i] ?? "";
    } else if (arg.startsWith("--profile=")) {
      profile = arg.slice("--profile=".length);
    } else {
      files.push(arg);
    }
  }
  if (files.length > 1) throw new Error(`expected at most one file argument, got ${files.length}`);
  const file = files.length === 1 ? path.resolve(process.cwd(), files[0]) : path.join(HERE, "sample-events.ndjson");
  return { file, profile };
}

function main(): number {
  const { file, profile: profileName } = parseArgs(process.argv.slice(2));
  const profile = pickProfile(profileName, file);

  console.log("alasol ts-consumer — NDJSON stream reader");
  console.log(`schemas: ${SCHEMA_DIR} (${schemas.length} registered, $id from schema)`);
  console.log(`namespace: ${BASE} (derived from $id)`);
  console.log(
    `profile: ${profile.name} (envelope ${profile.file}#/$defs/${profile.def})`,
  );
  console.log(
    `kind→payload: ${Object.entries(profile.kindPayload)
      .map(([kind, def]) => `${kind} → ${def}`)
      .join(", ")}`,
  );
  console.log(`file:    ${path.relative(process.cwd(), file) || file}`);
  console.log("");

  if (!fs.existsSync(file)) {
    console.error(`ERROR: file not found: ${file}`);
    return 1;
  }

  const results = processFile(file, profile);
  for (const r of results) {
    const status = r.ok ? "PASS" : "FAIL";
    const kind = (r.kind ?? "<no kind>").padEnd(20);
    console.log(`[${status}] line ${String(r.line).padStart(2)} kind=${kind} ${r.detail}`);
  }

  const passed = results.filter((r) => r.ok).length;
  const failed = results.length - passed;
  console.log("");
  console.log(`Summary: ${results.length} line(s), ${passed} passed, ${failed} failed`);
  const exitCode = failed === 0 ? 0 : 1;
  console.log(`RESULT: ${exitCode === 0 ? "PASS" : "FAIL"} — exit ${exitCode}`);
  return exitCode;
}

process.exit(main());
