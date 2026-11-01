#!/usr/bin/env node
// Teste de carga: cria N jobs disparando a cada segundo e mede, no receptor de webhooks,
// quantas execuções por segundo chegam e com quanto atraso em relação ao horário lógico
// (header X-Cronos-Scheduled-For). Repete para cada valor de N em STEPS.
//
// Uso (Cronos rodando em CRONOS_URL):
//   node scripts/load.mjs
//
// Variáveis de ambiente:
//   CRONOS_URL     URL da API                                (padrão: http://localhost:8080)
//   STEPS          quantidades de jobs, separadas por vírgula (padrão: 100,250,500,1000)
//   WARMUP_S       aquecimento antes de medir                (padrão: 10)
//   MEASURE_S      janela de medição                         (padrão: 30)
//   WEBHOOK_PORT   porta do receptor                         (padrão: 4200)
//   CRONOS_API_KEY chave da API                              (padrão: dev-key)

import http from "node:http";
import os from "node:os";

const CRONOS_URL = process.env.CRONOS_URL ?? "http://localhost:8080";
const STEPS = (process.env.STEPS ?? "100,250,500,1000").split(",").map(Number);
const WARMUP_S = Number(process.env.WARMUP_S ?? 10);
const MEASURE_S = Number(process.env.MEASURE_S ?? 30);
const WEBHOOK_PORT = Number(process.env.WEBHOOK_PORT ?? 4200);
const API_KEY = process.env.CRONOS_API_KEY ?? "dev-key";

const c = (code) => (s) => (process.stdout.isTTY ? `\x1b[${code}m${s}\x1b[0m` : String(s));
const gray = c(90), green = c(32), red = c(31), bold = c(1);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// Receptor: grava (chegada, horário lógico) de cada primeira tentativa
// ---------------------------------------------------------------------------
let samples = null; // [{ at, scheduledFor }] enquanto uma medição está aberta
const receiver = http.createServer({ keepAliveTimeout: 60_000 }, (req, res) => {
    const at = Date.now();
    req.resume();
    req.on("end", () => {
        const sf = req.headers["x-cronos-scheduled-for"];
        if (samples && sf && req.headers["x-cronos-attempt"] === "1") samples.push({ at, scheduledFor: Date.parse(sf) });
        res.writeHead(200).end();
    });
});

async function api(method, path, body) {
    const res = await fetch(`${CRONOS_URL}${path}`, {
        method,
        headers: { ...(body ? { "content-type": "application/json" } : {}), authorization: `Bearer ${API_KEY}` },
        body: body ? JSON.stringify(body) : undefined,
    });
    const text = await res.text();
    if (!res.ok) throw new Error(`${method} ${path} -> ${res.status}: ${text}`);
    return text ? JSON.parse(text) : null;
}

/** Executa fn para cada item com no máximo `limit` em paralelo. */
async function pool(items, limit, fn) {
    const out = new Array(items.length);
    let next = 0;
    await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
        while (next < items.length) {
            const i = next++;
            out[i] = await fn(items[i], i);
        }
    }));
    return out;
}

const pct = (sorted, p) => sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))] : NaN;
const ms = (v) => (Number.isNaN(v) ? "-" : `${Math.round(v)} ms`);

async function step(n, runId) {
    console.log(bold(`\n=== ${n} jobs (esperado: ${n} execuções/s) ===`));

    // Criação dos jobs, medindo a latência da API.
    const createLatencies = [];
    const t0 = Date.now();
    const ids = await pool([...Array(n).keys()], 20, async (i) => {
        const start = performance.now();
        const job = await api("POST", "/jobs", {
            name: `load-${runId}-${n}-${i}`, scheduleType: "INTERVAL", scheduleExpr: "PT1S", allowOverlap: true,
            payload: { url: `http://localhost:${WEBHOOK_PORT}/hit`, method: "POST" },
        });
        createLatencies.push(performance.now() - start);
        return job.id;
    });
    const createSecs = (Date.now() - t0) / 1000;
    createLatencies.sort((a, b) => a - b);
    console.log(gray(`  ${n} jobs criados em ${createSecs.toFixed(1)}s (${Math.round(n / createSecs)}/s), ` +
        `POST /jobs p50=${ms(pct(createLatencies, 0.5))} p95=${ms(pct(createLatencies, 0.95))}`));

    console.log(gray(`  aquecendo ${WARMUP_S}s...`));
    await sleep(WARMUP_S * 1000);

    console.log(gray(`  medindo ${MEASURE_S}s...`));
    samples = [];
    const windowStart = Date.now();
    await sleep(MEASURE_S * 1000);
    const windowEnd = Date.now();
    const got = samples;
    samples = null;

    await pool(ids, 20, (id) => api("DELETE", `/jobs/${id}`));

    const delays = got.map((s) => s.at - s.scheduledFor).sort((a, b) => a - b);
    const rate = got.length / ((windowEnd - windowStart) / 1000);
    const result = {
        jobs: n,
        "esperado/s": n,
        "entregue/s": Math.round(rate),
        "atraso p50": ms(pct(delays, 0.5)),
        "atraso p95": ms(pct(delays, 0.95)),
        "atraso p99": ms(pct(delays, 0.99)),
        "atraso máx": ms(delays.at(-1) ?? NaN),
        "POST /jobs p95": ms(pct(createLatencies, 0.95)),
    };
    const ok = rate >= n * 0.95;
    console.log(`  ${ok ? green("✔") : red("✘")} ${Math.round(rate)}/s entregues, atraso p50 ${result["atraso p50"]}, p99 ${result["atraso p99"]}`);

    // Deixa a fila esvaziar antes do próximo degrau.
    await sleep(5000);
    return result;
}

async function main() {
    await api("GET", "/actuator/health").catch(() => {
        console.error(red(`Cronos não respondeu em ${CRONOS_URL}`));
        process.exit(2);
    });
    await new Promise((r) => receiver.listen(WEBHOOK_PORT, r));
    const runId = Date.now().toString(36);

    const results = [];
    for (const n of STEPS) results.push(await step(n, runId));

    console.log(bold("\n==================== RESULTADO ===================="));
    console.table(results);

    // Tabela pronta para o README.
    const cols = Object.keys(results[0]);
    console.log(bold("Markdown:\n"));
    console.log(`| ${cols.join(" | ")} |`);
    console.log(`|${cols.map(() => "---").join("|")}|`);
    for (const r of results) console.log(`| ${cols.map((k) => r[k]).join(" | ")} |`);
    console.log(gray(`\nMáquina: ${os.cpus()[0].model.trim()}, ${os.cpus().length} threads, ` +
        `${Math.round(os.totalmem() / 2 ** 30)} GB RAM, ${os.type()} ${os.release()}`));

    receiver.close();
}

main().catch((e) => {
    console.error(red(e.stack ?? e));
    process.exit(1);
});
