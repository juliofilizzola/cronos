#!/usr/bin/env node
// Teste ponta a ponta do Cronos: cria jobs pela API, sobe um receptor de webhooks local
// e acompanha em tempo real as execuções (PENDING -> RUNNING -> SUCCEEDED/FAILED/DEAD)
// e as chamadas que chegam no receptor.
//
// Uso (Node 18+, sem dependências):
//   node scripts/e2e-workers.mjs
//
// Variáveis de ambiente:
//   CRONOS_URL     URL da API                       (padrão: http://localhost:8080)
//   WEBHOOK_PORT   Porta do receptor de webhooks    (padrão: 4000)
//   WEBHOOK_HOST   Host que o worker usa p/ chegar no receptor (padrão: localhost;
//                  use host.docker.internal se o Cronos rodar em container)
//   DURATION_S     Duração do teste em segundos     (padrão: 30)

import http from "node:http";

const CRONOS_URL = process.env.CRONOS_URL ?? "http://localhost:8080";
const WEBHOOK_PORT = Number(process.env.WEBHOOK_PORT ?? 4000);
const WEBHOOK_HOST = process.env.WEBHOOK_HOST ?? "localhost";
const DURATION_S = Number(process.env.DURATION_S ?? 30);
const POLL_MS = 1000;

const RUN_ID = Date.now().toString(36);
const hook = (path) => `http://${WEBHOOK_HOST}:${WEBHOOK_PORT}${path}`;

// ---------------------------------------------------------------------------
// Saída colorida
// ---------------------------------------------------------------------------
const c = (code) => (s) => (process.stdout.isTTY ? `\x1b[${code}m${s}\x1b[0m` : String(s));
const gray = c(90), green = c(32), red = c(31), yellow = c(33), cyan = c(36), magenta = c(35), bold = c(1);

const STATUS_COLOR = {
    PENDING: yellow, RUNNING: cyan, SUCCEEDED: green, FAILED: red, DEAD: magenta,
};
const paint = (status) => (STATUS_COLOR[status] ?? ((s) => s))(status);

const t0 = Date.now();
const elapsed = () => gray(`[+${((Date.now() - t0) / 1000).toFixed(1).padStart(5)}s]`);
const log = (...args) => console.log(elapsed(), ...args);

// ---------------------------------------------------------------------------
// Cenários: cada job aponta para uma rota diferente do receptor
// ---------------------------------------------------------------------------
const scenarios = [
    {
        key: "ok",
        description: "INTERVAL 2s, permite sobreposição, webhook responde 200",
        body: {
            scheduleType: "INTERVAL", scheduleExpr: "PT2S", allowOverlap: true,
            payload: { url: hook("/ok"), method: "POST", body: { hello: "cronos" } },
        },
    },
    {
        key: "no-overlap",
        description: "INTERVAL 2s, SEM sobreposição: só cria a próxima depois que a anterior terminar",
        body: {
            scheduleType: "INTERVAL", scheduleExpr: "PT2S", allowOverlap: false,
            payload: { url: hook("/ok"), method: "POST" },
        },
    },
    {
        key: "fail",
        description: "INTERVAL 5s, webhook responde 500: deve tentar 3x com backoff e virar DEAD",
        body: {
            scheduleType: "INTERVAL", scheduleExpr: "PT5S", allowOverlap: true, maxAttempts: 3,
            payload: { url: hook("/fail"), method: "POST" },
        },
    },
    {
        key: "slow",
        description: "INTERVAL 10s, webhook demora 5s com timeout de 2s: deve falhar por timeout",
        body: {
            scheduleType: "INTERVAL", scheduleExpr: "PT10S", allowOverlap: true, timeoutSeconds: 2,
            payload: { url: hook("/slow?ms=5000"), method: "POST" },
        },
    },
    {
        key: "once",
        description: "ONCE daqui a 5s: exatamente uma execução, job vira COMPLETED",
        body: {
            scheduleType: "ONCE",
            scheduleExpr: new Date(Date.now() + 5000).toISOString().replace(/\.\d+Z$/, "Z"),
            payload: { url: hook("/ok"), method: "POST" },
        },
    },
];

// ---------------------------------------------------------------------------
// Receptor de webhooks
// ---------------------------------------------------------------------------
const hits = []; // { at, path, method, headers, body, scenario }
const failCount = new Map(); // execução -> nº de chamadas em /fail

function executionIdFrom(req, body) {
    const h = req.headers;
    return h["idempotency-key"] ?? h["x-idempotency-key"] ?? h["x-cronos-execution-id"]
        ?? body?.executionId ?? body?.execution_id ?? null;
}

const server = http.createServer((req, res) => {
    let raw = "";
    req.on("data", (chunk) => (raw += chunk));
    req.on("end", async () => {
        let body = null;
        try { body = raw ? JSON.parse(raw) : null; } catch { body = raw; }

        const url = new URL(req.url, `http://${req.headers.host}`);
        const execId = executionIdFrom(req, body);
        hits.push({ at: Date.now(), path: url.pathname, method: req.method, headers: req.headers, body, execId });

        const tag = execId ? gray(` exec=${String(execId).slice(0, 8)}`) : yellow(" (sem chave de idempotência)");
        if (url.pathname === "/fail") {
            failCount.set(execId, (failCount.get(execId) ?? 0) + 1);
            log(red("<- webhook"), `${req.method} ${url.pathname}${tag}`, red("-> 500"));
            res.writeHead(500, { "content-type": "application/json" }).end('{"error":"falha simulada"}');
        } else if (url.pathname === "/slow") {
            const ms = Number(url.searchParams.get("ms") ?? 5000);
            log(cyan("<- webhook"), `${req.method} ${url.pathname}${tag}`, gray(`(segurando ${ms}ms)`));
            await new Promise((r) => setTimeout(r, ms));
            if (!res.destroyed) res.writeHead(200).end("ok");
        } else {
            log(green("<- webhook"), `${req.method} ${url.pathname}${tag}`, green("-> 200"));
            res.writeHead(200, { "content-type": "application/json" }).end('{"ok":true}');
        }
    });
});

// ---------------------------------------------------------------------------
// Cliente da API
// ---------------------------------------------------------------------------
async function api(method, path, body) {
    const res = await fetch(`${CRONOS_URL}${path}`, {
        method,
        headers: body ? { "content-type": "application/json" } : {},
        body: body ? JSON.stringify(body) : undefined,
    });
    const text = await res.text();
    const data = text ? JSON.parse(text) : null;
    if (!res.ok) throw new Error(`${method} ${path} -> ${res.status}: ${text}`);
    return data;
}

// ---------------------------------------------------------------------------
// Acompanhamento das execuções
// ---------------------------------------------------------------------------
const jobs = []; // { scenario, id }
const seen = new Map(); // execId -> { status, attempt, jobKey, scheduledFor }

async function poll() {
    for (const job of jobs) {
        const execs = await api("GET", `/jobs/${job.id}/executions?size=100`);
        for (const e of execs) {
            const prev = seen.get(e.id);
            const label = `${bold(job.scenario.key.padEnd(10))} exec=${e.id.slice(0, 8)} ` +
                gray(`(scheduledFor ${e.scheduledFor.slice(11, 19)})`);
            if (!prev) {
                log(gray("•"), label, "criada", paint(e.status));
            } else if (prev.status !== e.status || prev.attempt !== e.attempt) {
                const err = e.lastError ? red(` erro="${e.lastError.slice(0, 60)}"`) : "";
                log(gray("•"), label, `${paint(prev.status)} -> ${paint(e.status)}`,
                    gray(`tentativa ${e.attempt}`) + err);
            }
            seen.set(e.id, { ...e, jobKey: job.scenario.key });
        }
    }
}

function summarize() {
    console.log("\n" + bold("==================== RESUMO ===================="));

    const all = [...seen.values()];
    const byJob = Object.groupBy(all, (e) => e.jobKey);
    const statuses = ["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "DEAD"];

    console.log(bold("\nExecuções por job"));
    console.table(Object.fromEntries(jobs.map(({ scenario }) => {
        const list = byJob[scenario.key] ?? [];
        const row = { total: list.length };
        for (const s of statuses) row[s] = list.filter((e) => e.status === s).length;
        return [scenario.key, row];
    })));

    const webhookHits = hits.filter((h) => h.path !== "/favicon.ico");
    console.log(bold("Chamadas recebidas no webhook:"), webhookHits.length);

    // ---- Verificações ----
    const checks = [];
    const check = (ok, msg, detail = "") => checks.push({ ok, msg, detail });

    // 1. Nenhuma ocorrência duplicada (UNIQUE job_id + scheduled_for)
    const dupOcc = all.length - new Set(all.map((e) => `${e.jobId}|${e.scheduledFor}`)).size;
    check(dupOcc === 0, "Nenhuma ocorrência duplicada (job_id, scheduled_for)", `${dupOcc} duplicadas`);

    // 2. O worker consumiu alguma coisa?
    const consumed = all.filter((e) => e.status !== "PENDING").length;
    check(consumed > 0, "Worker consumiu execuções (saíram de PENDING)",
        consumed === 0 ? "todas continuam PENDING: worker não implementado ou parado" : `${consumed} consumidas`);

    // 3. Webhook foi chamado
    check(webhookHits.length > 0, "Webhook foi chamado pelo worker",
        webhookHits.length === 0 ? `nenhuma chamada em ${hook("")}` : "");

    // 4. Chave de idempotência enviada
    const withKey = webhookHits.filter((h) => h.execId).length;
    check(webhookHits.length > 0 && withKey === webhookHits.length,
        "Toda chamada traz o id da execução (Idempotency-Key)", `${withKey}/${webhookHits.length}`);

    // 5. Sucesso não é entregue mais de uma vez
    const okHits = webhookHits.filter((h) => h.path === "/ok" && h.execId);
    const okDup = okHits.length - new Set(okHits.map((h) => h.execId)).size;
    check(okDup === 0, "Execuções bem-sucedidas entregues uma única vez", `${okDup} entregas repetidas`);

    // 6. Sem sobreposição: nunca 2 ativas ao mesmo tempo
    const noOverlap = byJob["no-overlap"] ?? [];
    const activeNow = noOverlap.filter((e) => e.status === "PENDING" || e.status === "RUNNING").length;
    check(activeNow <= 1, "allowOverlap=false mantém no máximo 1 execução ativa", `${activeNow} ativas`);
    check(noOverlap.length > 1, "allowOverlap=false continua disparando após concluir a anterior",
        `${noOverlap.length} execuções (se 1 e PENDING, o job está travado esperando o worker)`);

    // 7. Retentativas
    const failExecs = byJob["fail"] ?? [];
    const maxAttempt = Math.max(0, ...failExecs.map((e) => e.attempt));
    check(maxAttempt > 1, "Webhook com 500 é retentado", `maior tentativa = ${maxAttempt}`);
    check(failExecs.some((e) => e.status === "DEAD"), "Após maxAttempts a execução vira DEAD");

    // 8. Timeout
    const slow = byJob["slow"] ?? [];
    check(slow.some((e) => e.lastError && /timeout|timed out/i.test(e.lastError)),
        "Webhook lento falha por timeout", slow.map((e) => e.lastError).filter(Boolean)[0] ?? "");

    // 9. ONCE
    const once = byJob["once"] ?? [];
    check(once.length === 1, "Job ONCE gerou exatamente 1 execução", `${once.length} execuções`);

    console.log(bold("\nVerificações"));
    for (const { ok, msg, detail } of checks) {
        console.log(` ${ok ? green("✔") : red("✘")} ${msg}${detail ? gray(`  (${detail})`) : ""}`);
    }
    const failed = checks.filter((x) => !x.ok).length;
    console.log(failed === 0
        ? green(bold(`\nTudo certo: ${checks.length} verificações passaram.`))
        : yellow(bold(`\n${failed} de ${checks.length} verificações falharam.`)));
    return failed;
}

async function cleanup() {
    // Não existe DELETE na API: pausa os jobs para não continuarem gerando execuções.
    for (const job of jobs) {
        try { await api("POST", `/jobs/${job.id}/pause`); } catch { /* COMPLETED ou já pausado */ }
    }
    server.close();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
    try {
        await api("GET", "/actuator/health");
    } catch (e) {
        console.error(red(`Cronos não respondeu em ${CRONOS_URL}. Suba com: docker compose up -d && ./gradlew bootRun`));
        console.error(gray(e.message));
        process.exit(2);
    }

    await new Promise((resolve) => server.listen(WEBHOOK_PORT, resolve));
    console.log(bold(`Receptor de webhooks em http://localhost:${WEBHOOK_PORT}`), gray(`(worker deve chamar ${hook("/...")})`));
    console.log(bold(`Cronos em ${CRONOS_URL}`), gray(`| duração ${DURATION_S}s | run ${RUN_ID}\n`));

    for (const scenario of scenarios) {
        const job = await api("POST", "/jobs", { name: `e2e-${RUN_ID}-${scenario.key}`, ...scenario.body });
        jobs.push({ scenario, id: job.id });
        log(bold("job criado"), scenario.key.padEnd(10), gray(job.id), "-", scenario.description);
    }
    console.log();

    let stopping = false;
    const stop = async () => {
        if (stopping) return;
        stopping = true;
        clearInterval(timer);
        await poll().catch(() => {});
        const failed = summarize();
        await cleanup();
        process.exit(failed === 0 ? 0 : 1);
    };

    const timer = setInterval(() => poll().catch((e) => log(red("erro no poll:"), e.message)), POLL_MS);
    setTimeout(stop, DURATION_S * 1000);
    process.on("SIGINT", stop);
}

main().catch(async (e) => {
    console.error(red(e.stack ?? e));
    await cleanup();
    process.exit(1);
});
