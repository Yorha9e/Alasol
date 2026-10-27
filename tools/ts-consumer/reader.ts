/**
 * alasol 最小 TypeScript 消费端：逐行读取 NDJSON 事件流，按 contracts/schema 做跨语言契约校验。
 *
 * 运行方式：
 *   cd tools/ts-consumer && npm install && npx tsx reader.ts
 *   （坏样本：npx tsx reader.ts broken-events.ndjson；默认读 sample-events.ndjson）
 *
 * 退出码：所有行都通过校验 → 0；任一行解析/校验失败 → 1。
 *
 * 设计要点：
 * - 12 份 JSON Schema（2020-12，$defs）自带 $id：https://github.com/Yorha9e/alasol/contracts/schema/<file>.json。
 *   ajv（2020-12 类）注册后，跨文件相对 $ref（common.json#/$defs/Ulid 等）按各文件 $id 命名空间自解析，
 *   无需消费端注入 $id。schema 目录相对本文件定位（../../contracts/schema），只读不写。
 * - envelope 一律用 event.json#/$defs/Event 校验（v/id/kind/ts/session_id/agent_id/durable 必填，
 *   id/session_id/agent_id 走 common.json#/$defs/Ulid 的 26 字符 Crockford base32 正则）。
 * - 按 kind 分发 payload 校验（映射与 event.json 的 x-kind-payload 一致）：
 *     tool.call.intent     → event.json#/$defs/ToolCallIntentPayload
 *     tool.call.completed  → event.json#/$defs/ToolCallCompletedPayload（result 跨文件 ref tool-result.json）
 *     tool.call.failed     → event.json#/$defs/ToolCallFailedPayload
 *   其他 kind（如 model.delta、context.assembled）宽容跳过 payload 校验（D7：未知 kind 忽略但持久化），
 *   envelope 合法即算该行通过。
 * - tool_call_id 是 provider 不透明字符串（common.json#/$defs/ProviderId），与 Event.id（ULID）永不共字段。
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Ajv from "ajv/dist/2020";
import type { ErrorObject, ValidateFunction } from "ajv";
import addFormats from "ajv-formats";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SCHEMA_DIR = path.resolve(HERE, "..", "..", "contracts", "schema");
const NS = "https://github.com/Yorha9e/alasol/contracts/schema/";

// ---------------------------------------------------------------------------
// 1. 注册全部 schema：$id 由 schema 自带（绝对命名空间），跨文件 $ref 自解析
// ---------------------------------------------------------------------------
const ajv = new Ajv({ allErrors: true, strict: false }); // strict=false：容忍 x-go-type/x-id-kind 等自定义关键字
addFormats(ajv); // 提供 format: "date-time"（ts 字段）

const schemaFiles = fs
  .readdirSync(SCHEMA_DIR)
  .filter((name) => name.endsWith(".json"))
  .sort();
for (const name of schemaFiles) {
  const schema = JSON.parse(fs.readFileSync(path.join(SCHEMA_DIR, name), "utf8")) as Record<string, unknown>;
  ajv.addSchema(schema);
}

// ---------------------------------------------------------------------------
// 2. 校验器：envelope + 三种已知 tool.call payload
// ---------------------------------------------------------------------------
function mustGet(ref: string): ValidateFunction {
  const key = new URL(ref, NS).href; // "event.json#/$defs/Event" → 绝对 key
  const validate = ajv.getSchema(key);
  if (!validate) throw new Error(`schema not found (check $id registration): ${key}`);
  return validate;
}

const validateEnvelope = mustGet("event.json#/$defs/Event");
const payloadValidators = new Map<string, ValidateFunction>([
  ["tool.call.intent", mustGet("event.json#/$defs/ToolCallIntentPayload")],
  ["tool.call.completed", mustGet("event.json#/$defs/ToolCallCompletedPayload")],
  ["tool.call.failed", mustGet("event.json#/$defs/ToolCallFailedPayload")],
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
// 3. 逐行处理
// ---------------------------------------------------------------------------
interface LineResult {
  line: number;
  ok: boolean;
  kind?: string;
  detail: string;
}

function processFile(file: string): LineResult[] {
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

    if (!validateEnvelope(parsed)) {
      results.push({ line, ok: false, kind, detail: `envelope FAIL: ${formatErrors(validateEnvelope.errors)}` });
      return;
    }

    const payloadValidator = kind !== undefined ? payloadValidators.get(kind) : undefined;
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
// 4. main：参数指定文件（默认 sample），打印逐行结果 + 汇总，按结果设置退出码
// ---------------------------------------------------------------------------
function main(): number {
  const arg = process.argv[2];
  const file = arg !== undefined ? path.resolve(process.cwd(), arg) : path.join(HERE, "sample-events.ndjson");

  console.log("alasol ts-consumer — NDJSON event stream reader");
  console.log(`schemas: ${SCHEMA_DIR} (${schemaFiles.length} registered, $id from schema)`);
  console.log(`file:    ${path.relative(process.cwd(), file) || file}`);
  console.log("");

  if (!fs.existsSync(file)) {
    console.error(`ERROR: file not found: ${file}`);
    return 1;
  }

  const results = processFile(file);
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
