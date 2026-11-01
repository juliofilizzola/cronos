#!/usr/bin/env node
// Teste de caos: sobe várias instâncias do Cronos sobre o mesmo PostgreSQL, mata instâncias
// aleatoriamente com SIGKILL (sem desligamento limpo) enquanto jobs disparam a cada segundo,
// e no fim verifica que nenhuma ocorrência foi duplicada nem perdida.
//
// Pré-requisitos: PostgreSQL no ar (docker compose up -d) e o jar gerado (./gradlew bootJar).
// Uso: node scripts/chaos.mjs
//
// Variáveis de ambiente:
//   INSTANCES     instâncias simultâneas              (padrão: 3)
//   JOBS          jobs criados, um disparo/s cada     (padrão: 20)
//   DURATION_S    duração do caos em segundos         (padrão: 60)
//   KILL_EVERY_S  intervalo entre mortes              (padrão: 5)
//   DRAIN_S       espera máxima para a fila esvaziar  (padrão: 90)
//   BASE_PORT     porta da 1ª instância               (padrão: 8081)
//   WEBHOOK_PORT  porta do receptor de webhooks       (padrão: 4100)
//   JAVA          executável do Java                  (padrão: java)

import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const INSTANCES = Number(process.env.INSTANCES ?? 3);
const JOBS = Number(process.env.JOBS ?? 20);
const DURATION_S = Number(process.env.DURATION_S ?? 60);
const KILL_EVERY_S = Number(process.env.KILL_EVERY_S ?? 5);
const DRAIN_S = Number(process.env.DRAIN_S ?? 90);
const BASE_PORT = Number(process.env.BASE_PORT ?? 8081);
const WEBHOOK_PORT = Number(process.env.WEBHOOK_PORT ?? 4100);
const JAVA = process.env.JAVA ?? "java";
const LOG_DIR = path.join(ROOT, "build", "chaos-logs");
const API_KEY = "chaos-key";

const c = (code) => (s) => (process.stdout.isTTY ? `\x1b[${code}m${s}\x1b[0m` : String(s));
const gray = c(90), green = c(32), red = c(31), yellow = c(33), bold = c(1);
const t0 = Date.now();
const log = (...a) => console.log(gray(`[+${((Date.now() - t0) / 1000).toFixed(1).padStart(5)}s]`), ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// Receptor de webhooks: responde 200 depois de 0-300 ms, para haver chamadas em voo na hora da morte
// ---------------------------------------------------------------------------
const deliveries = new Map(); // execId -> nº de chamadas recebidas
const receiver = http.createServer((req, res) => {
    req.resume();
    req.on("end", () => {
        const id = req.headers["idempotency-key"];
        if (id) deliveries.set(id, (deliveries.get(id) ?? 0) + 1);
        setTimeout(() => res.writeHead(200).end("ok"), Math.random() * 300);
    });
});

// ---------------------------------------------------------------------------
// Instâncias
// ---------------------------------------------------------------------------
function findJar() {
    const dir = path.join(ROOT, "build", "libs");
    const jar = fs.existsSync(dir) && fs.readdirSync(dir).find((f) => f.endsWith(".jar") && !f.endsWith("-plain.jar"));
    if (!jar) {
        console.error(red("Jar não encontrado em build/libs. Rode: ./gradlew bootJar"));
        process.exit(2);
    }
    return path.join(dir, jar);
}

const JAR = findJar();
const instances = Array.from({ length: INSTANCES }, (_, i) => ({ n: i + 1, port: BASE_PORT + i, proc: null, starts: 0 }));
let kills = 0;

function start(inst) {
    fs.mkdirSync(LOG_DIR, { recursive: true });
    const out = fs.openSync(path.join(LOG_DIR, `instance-${inst.n}.log`), "a");
    inst.proc = spawn(JAVA, [
        "-jar", JAR,
        `--server.port=${inst.port}`,
        // Lease curto para o reaper agir rápido durante o teste.
        "--cronos.worker.lease=5s",
        "--cronos.worker.heartbeat-interval-ms=1000",
        "--cronos.worker.reaper-interval-ms=1000",
        "--cronos.worker.shutdown-grace=1s",
        `--cronos.security.api-keys=${API_KEY}`,
        "--cronos.webhook.allow-private-networks=true", // o receptor roda em localhost
    ], { stdio: ["ignore", out, out] });
    inst.starts++;
    const proc = inst.proc;
    proc.on("exit", () => { if (inst.proc === proc) inst.proc = null; });
}

function kill(inst) {
    if (!inst.proc) return;
    inst.proc.kill("SIGKILL"); // no Windows vira TerminateProcess: morte sem aviso
    inst.proc = null;
    kills++;
}

const alive = () => instances.filter((i) => i.proc);

async function healthy(inst) {
    try {
        const r = await fetch(`http://localhost:${inst.port}/actuator/health`, { signal: AbortSignal.timeout(1000) });
        return r.ok;
    } catch {
        return false;
    }
}

async function waitHealthy(inst, timeoutMs = 120_000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        if (await healthy(inst)) return true;
        if (!inst.proc) return false;
        await sleep(500);
    }
    return false;
}

// Faz a chamada em qualquer instância que responda.
async function api(method, p, body) {
    for (let attempt = 0; attempt < 40; attempt++) {
        for (const inst of [...alive()].sort(() => Math.random() - 0.5)) {
            try {
                const r = await fetch(`http://localhost:${inst.port}${p}`, {
                    method, signal: AbortSignal.timeout(5000),
                    headers: { ...(body ? { "content-type": "application/json" } : {}), authorization: `Bearer ${API_KEY}` },
                    body: body ? JSON.stringify(body) : undefined,
                });
                const text = await r.text();
                if (r.status >= 500) continue;
                return { status: r.status, data: text ? JSON.parse(text) : null };
            } catch { /* instância morreu no meio: tenta outra */ }
        }
        await sleep(500);
    }
    throw new Error(`nenhuma instância respondeu a ${method} ${p}`);
}

async function allExecutions(jobId) {
    const all = [];
    for (let page = 0; ; page++) {
        const { data } = await api("GET", `/jobs/${jobId}/executions?size=100&page=${page}`);
        all.push(...data);
        if (data.length < 100) return all;
    }
}

function shutdownAll() {
    for (const inst of instances) kill(inst);
    receiver.close();
}
process.on("exit", shutdownAll);
process.on("SIGINT", () => process.exit(130));

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
    await new Promise((r) => receiver.listen(WEBHOOK_PORT, r));
    log(bold(`Subindo ${INSTANCES} instâncias`), gray(`(${path.relative(ROOT, JAR)}, logs em ${path.relative(ROOT, LOG_DIR)})`));
    instances.forEach(start);
    const ups = await Promise.all(instances.map((i) => waitHealthy(i)));
    if (!ups.every(Boolean)) throw new Error("alguma instância não subiu; veja os logs");
    log(green("todas no ar"));

    const runId = Date.now().toString(36);
    const jobIds = [];
    for (let i = 0; i < JOBS; i++) {
        const { status, data } = await api("POST", "/jobs", {
            name: `chaos-${runId}-${i}`, scheduleType: "INTERVAL", scheduleExpr: "PT1S",
            allowOverlap: true, misfirePolicy: "FIRE_ALL", misfireThresholdSeconds: 0, maxAttempts: 20,
            payload: { url: `http://localhost:${WEBHOOK_PORT}/ok`, method: "POST" },
        });
        if (status !== 201) throw new Error(`falha ao criar job: ${status} ${JSON.stringify(data)}`);
        jobIds.push(data.id);
    }
    log(bold(`${JOBS} jobs criados`), gray("(INTERVAL PT1S, FIRE_ALL: cada segundo perdido deve ser recuperado)"));

    // Caos: mata uma instância aleatória a cada KILL_EVERY_S e a reinicia 2-6 s depois.
    const chaosUntil = Date.now() + DURATION_S * 1000;
    while (Date.now() < chaosUntil) {
        await sleep(KILL_EVERY_S * 1000);
        const candidates = alive();
        if (candidates.length === 0) continue;
        const victim = candidates[Math.floor(Math.random() * candidates.length)];
        kill(victim);
        log(red(`☠ instância ${victim.n} morta`), gray(`(vivas: ${alive().map((i) => i.n).join(",") || "nenhuma"})`));
        const delay = 2000 + Math.random() * 4000;
        setTimeout(() => {
            start(victim);
            log(yellow(`↻ instância ${victim.n} reiniciando`));
        }, delay);
    }

    log(bold("Fim do caos: pausando os jobs e reiniciando quem estiver fora"));
    await sleep(6500); // deixa terminar os reinícios agendados
    for (const inst of instances) if (!inst.proc) start(inst);
    await Promise.all(instances.map((i) => waitHealthy(i)));
    for (const id of jobIds) await api("POST", `/jobs/${id}/pause`);

    // Espera a fila esvaziar (inclui execuções presas em workers mortos, liberadas pelo reaper).
    const drainUntil = Date.now() + DRAIN_S * 1000;
    let execs = [];
    while (true) {
        execs = (await Promise.all(jobIds.map(allExecutions))).flat();
        const open = execs.filter((e) => e.status === "PENDING" || e.status === "RUNNING").length;
        log(gray(`aguardando fila: ${open} PENDING/RUNNING de ${execs.length}`));
        if (open === 0 || Date.now() > drainUntil) break;
        await sleep(3000);
    }

    // ---------------- Verificações ----------------
    const checks = [];
    const check = (ok, msg, detail = "") => checks.push({ ok, msg, detail });
    const byJob = Object.groupBy(execs, (e) => e.jobId);

    const keys = execs.map((e) => `${e.jobId}|${e.scheduledFor}`);
    check(new Set(keys).size === keys.length, "Nenhuma ocorrência duplicada", `${keys.length - new Set(keys).size} duplicadas`);

    let gaps = 0;
    for (const id of jobIds) {
        const times = (byJob[id] ?? []).map((e) => Date.parse(e.scheduledFor)).sort((a, b) => a - b);
        for (let i = 1; i < times.length; i++) if (times[i] - times[i - 1] !== 1000) gaps += (times[i] - times[i - 1]) / 1000 - 1;
    }
    check(gaps === 0, "Nenhuma ocorrência perdida (segundos contíguos em todos os jobs)", `${gaps} segundos faltando`);

    const byStatus = Object.groupBy(execs, (e) => e.status);
    const count = (s) => byStatus[s]?.length ?? 0;
    check(count("PENDING") + count("RUNNING") === 0, "Nenhuma execução presa em PENDING/RUNNING",
        `${count("PENDING")} PENDING, ${count("RUNNING")} RUNNING`);
    check(count("SUCCEEDED") === execs.length, "Todas as execuções terminaram SUCCEEDED",
        Object.entries(byStatus).map(([s, l]) => `${s}=${l.length}`).join(" "));

    const undelivered = execs.filter((e) => e.status === "SUCCEEDED" && !deliveries.has(e.id)).length;
    check(undelivered === 0, "Toda execução SUCCEEDED chegou ao webhook", `${undelivered} sem entrega`);

    const redelivered = [...deliveries.values()].filter((n) => n > 1).length;
    const retried = execs.filter((e) => e.attempt > 1).length;

    console.log("\n" + bold("==================== RESUMO ===================="));
    console.table({
        instâncias: INSTANCES, jobs: JOBS, "duração (s)": DURATION_S, mortes: kills,
        execuções: execs.length, "com >1 tentativa": retried,
        "entregas repetidas (at-least-once)": redelivered,
    });
    for (const { ok, msg, detail } of checks) {
        console.log(` ${ok ? green("✔") : red("✘")} ${msg}${detail ? gray(`  (${detail})`) : ""}`);
    }
    console.log(gray(`\nEntregas repetidas são esperadas: um worker morto depois de chamar o webhook e antes de\n` +
        `gravar o resultado faz a execução rodar de novo. O destino deduplica pelo Idempotency-Key.`));

    const failed = checks.filter((x) => !x.ok).length;
    console.log(failed === 0 ? green(bold(`\nTudo certo: ${checks.length} verificações passaram.`))
        : red(bold(`\n${failed} de ${checks.length} verificações falharam.`)));
    process.exit(failed === 0 ? 0 : 1);
}

main().catch((e) => {
    console.error(red(e.stack ?? e));
    process.exit(1);
});
