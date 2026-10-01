#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

/**
 * Target-host rehearsal runner for G-007.
 *
 * The rehearsal was previously a checklist a human walked through, which means
 * "we rehearsed" was a claim rather than an artifact. This executes the sequence
 * in DEPLOYMENT_ROLLBACK_RUNBOOK.md and writes a signed-shaped evidence record
 * of what actually happened, step by step, with each step's real exit status.
 *
 * It is deliberately not allowed to self-certify. The record it produces says
 * which steps passed on which host; it does not say the host is the intended
 * production host, because this script cannot know that. G-007 stays open until
 * a person confirms the target.
 *
 * Usage:
 *   node scripts/rehearse-deployment.mjs --env-file <path> [--keep] [--only <step>]
 */

const COMPOSE_FILE = 'docker-compose.production.yml';
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');

// Services this run depends on. Asserted against `compose config --services`
// before anything starts.
//
// The start-application step originally only checked that `compose ps` listed
// `api` and `economic-worker` as running. `compose ps` enumerates containers in
// the project, not only the services defined in the file it was handed, so a
// container left behind by an earlier stack satisfied the check. A rehearsal
// recorded on 2026-10-01 reported `economic-worker:running` and 10/10 passed
// even though no compose file in this repository defines `economic-worker` --
// the step certified a container the committed configuration does not specify.
// Requiring the service to be declared first turns that from a silent false pass
// into an immediate, explicit failure.
const REQUIRED_SERVICES = ['api', 'economic-worker', 'postgres'];

// Every step a complete run executes. A record missing any of these is partial by
// construction, whatever flags produced it.
const EXPECTED_STEPS = [
  'source-revision',
  'declared-services',
  'trading-services-startup-contract',
  'preflight-port',
  'render-compose-model',
  'build-images',
  'start-postgres',
  'apply-migrations',
  'start-application',
  'predeploy-backup',
  'production-paper-smoke',
  'teardown'
];

const PAPER_ONLY_FLAGS = {
  LIVE_TRADING: 'false',
  LIVE_TRADING_ENABLED: 'false',
  COINBASE_DRY_RUN: 'true',
  ALLOW_POLYMARKET_ORDER_SUBMISSION: 'false',
  ALLOW_LIVE_SETTLEMENT_REDEMPTION: 'false',
  REMOTE_LLM_EXECUTION_ENABLED: 'false'
};

function parse(argv) {
  const options = { keep: false, envFile: null, only: null };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--env-file') options.envFile = argv[++index];
    else if (arg === '--keep') options.keep = true;
    else if (arg === '--only') options.only = argv[++index];
  }
  return options;
}

const options = parse(process.argv.slice(2));
if (!options.envFile) {
  process.stderr.write(`${JSON.stringify({ ok: false, error: 'rehearsal_env_file_required' }, null, 2)}\n`);
  process.exit(1);
}
if (!fs.existsSync(options.envFile)) {
  process.stderr.write(`${JSON.stringify({ ok: false, error: 'rehearsal_env_file_missing', path: options.envFile }, null, 2)}\n`);
  process.exit(1);
}

const envFile = path.resolve(options.envFile);
const workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'portfolio-rehearsal-'));
const backupDir = path.join(workDir, 'backups');
fs.mkdirSync(backupDir, { recursive: true });

/**
 * Parse the rehearsal env file into a plain object. Compose reads it itself,
 * but steps that shell out to npm scripts do not, so the tokens the smoke test
 * needs have to be passed through explicitly. Values are copied, never
 * printed, and the record only ever carries a masked summary.
 */
function loadEnvFile(file) {
  const loaded = {};
  for (const rawLine of fs.readFileSync(file, 'utf8').split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const separator = line.indexOf('=');
    if (separator < 1) continue;
    const key = line.slice(0, separator).trim();
    let value = line.slice(separator + 1).trim();
    if ((value.startsWith("'") && value.endsWith("'")) || (value.startsWith('"') && value.endsWith('"'))) {
      value = value.slice(1, -1);
    }
    loaded[key] = value;
  }
  return loaded;
}

const fileEnv = loadEnvFile(envFile);
const rehearsalEnv = { ...process.env, ...fileEnv };
rehearsalEnv.PORTFOLIO_BASE_URL = `http://${fileEnv.API_BIND_ADDRESS || '127.0.0.1'}:${fileEnv.API_PORT || '3000'}`;

function run(command, args, { allowFailure = false, capture = true, withEnv = true } = {}) {
  const started = Date.now();
  const result = spawnSync(command, args, {
    encoding: 'utf8',
    env: withEnv ? rehearsalEnv : { ...process.env },
    maxBuffer: 64 * 1024 * 1024
  });
  const step = {
    command: `${command} ${args.join(' ')}`.replace(new RegExp(envFile, 'g'), '$ENV_FILE'),
    status: result.status,
    ok: result.status === 0,
    durationMs: Date.now() - started
  };
  if (capture) {
    const out = String(result.stdout || '');
    step.stdoutBytes = out.length;
    if (!step.ok) {
      step.stderr = String(result.stderr || '').slice(0, 4000);
      step.stdout = out.slice(0, 4000);
    } else {
      step.stdout = out.slice(0, 8000);
    }
  }
  if (!step.ok && !allowFailure) throw Object.assign(new Error(`rehearsal_step_failed:${step.command}`), { step });
  return step;
}

function compose(args, opts = {}) {
  return run('docker', ['compose', '--env-file', envFile, '-f', COMPOSE_FILE, ...args], opts);
}

const steps = [];
const failures = [];
let current = null;

function step(name, fn) {
  if (options.only && options.only !== name) return;
  current = { name, startedAt: new Date().toISOString(), commands: [] };
  try {
    const extra = fn(current.commands) ?? {};
    current.status = 'passed';
    current.detail = extra;
  } catch (error) {
    current.status = 'failed';
    current.error = error.message;
    if (error.step) current.commands.push(error.step);
    failures.push({ step: name, error: error.message });
  }
  current.durationMs = Date.now() - Date.parse(current.startedAt);
  steps.push(current);
  process.stdout.write(`  ${current.status === 'passed' ? 'PASS' : 'FAIL'}  ${name}\n`);
  for (const command of current.commands) {
    process.stdout.write(`        ${command.ok ? 'ok  ' : 'FAIL'} ${command.command} (${command.durationMs}ms)\n`);
  }
}

const releaseSha = spawnSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).stdout.trim();

step('source-revision', commands => {
  const head = run('git', ['rev-parse', 'HEAD']);
  commands.push(head);
  const status = run('git', ['status', '--short'], { allowFailure: true });
  commands.push(status);

  // A dirty tree invalidates the whole record. The SHA names a commit whose tree
  // was not what got deployed or tested, so every later step would be evidence
  // for something other than what the SHA points at. This is not a warning: the
  // first version of this runner recorded the SHA anyway, and the resulting
  // record cited a commit that could not have produced the result.
  if (status.stdout.trim() !== '') {
    throw Object.assign(new Error('rehearsal_working_tree_dirty'), {
      step: {
        command: 'git status --short',
        ok: false,
        status: 1,
        stdout: status.stdout,
        stderr: 'The working tree has uncommitted changes. The recorded SHA would not identify the tree that was tested, so the evidence would be worthless. Commit or stash first.',
        durationMs: status.durationMs
      }
    });
  }
  return { releaseSha: head.stdout.trim(), workingTreeClean: true };
});

step('declared-services', commands => {
  const declared = compose(['config', '--services']);
  commands.push(declared);
  const defined = new Set(String(declared.stdout || '').split(/\s+/).filter(Boolean));
  const missing = REQUIRED_SERVICES.filter(service => !defined.has(service));
  if (missing.length) {
    throw Object.assign(new Error(`rehearsal_services_not_declared:${missing.join(',')}`), {
      step: {
        command: `docker compose -f ${COMPOSE_FILE} config --services`,
        ok: false,
        status: 1,
        stdout: declared.stdout,
        stderr: `${COMPOSE_FILE} defines [${[...defined].join(', ')}] but this rehearsal requires [${REQUIRED_SERVICES.join(', ')}]. Missing: ${missing.join(', ')}. A step that waits for an undeclared service gets satisfied by any stale container in the project, so the record would certify something this repository does not define.`,
        durationMs: declared.durationMs
      }
    });
  }
  return { declared: [...defined] };
});

step('trading-services-startup-contract', commands => {
  // This rehearsal only ever starts compose services (api, postgres,
  // economic-worker). The services that actually trade are systemd-managed
  // Python: run_production.py supervising unified_market_daemon,
  // dashboard_server, run_trader_v4 and the hermes agent watcher. Nothing in the
  // compose stack starts them, so "10/10 steps passed" says nothing about
  // whether the trading system comes up.
  //
  // Rather than pretend otherwise, check the startup contract those services are
  // supervised under: the scripts exist, their argparse configuration is sound,
  // and the dashboard can resolve its operator token in a systemd-like
  // environment. That is the part which has bitten before -- a required token
  // turned into a dashboard crash-loop on every service restart, and a bare `%`
  // in an argparse help string broke run_trader_v4 -- both invisible to unit
  // tests because they only appear when the documented way to run is used.
  const venvPython = path.join(ROOT, '.venv', 'bin', 'python');
  const python = fs.existsSync(venvPython) ? venvPython : 'python3';
  const contract = run(python, ['-m', 'pytest', 'tests/test_supervisor_contract.py', '-q'], {
    cwd: ROOT,
    env: { ...process.env, PYTHONPATH: `${ROOT}${path.delimiter}${path.join(ROOT, 'trading_system')}` }
  });
  commands.push(contract);
  if (!contract.ok) {
    throw Object.assign(new Error('rehearsal_trading_services_startup_contract_failed'), { step: contract });
  }
  return {
    covered: ['run_production.py children exist', 'argparse configuration sound',
              'dashboard resolves an operator token under a systemd-like env'],
    notCovered: 'the compose rehearsal does not start, restart or health-check the systemd-managed trading services; see deploy/portfolio-trader.service'
  };
});

step('preflight-port', commands => {
  // A rehearsal that silently validates whatever else happens to be listening
  // is worse than no rehearsal: the smoke would report on a foreign service and
  // the record would look green. Check before starting anything.
  const port = Number(fileEnv.API_PORT || 3000);
  const probe = run('bash', ['-c',
    `node -e "const n=require('net');const s=n.createServer();` +
    `s.once('error',()=>process.exit(1));s.listen(${port},'127.0.0.1',()=>s.close(()=>process.exit(0)))"`
  ], { allowFailure: true });
  commands.push(probe);
  if (!probe.ok) {
    throw Object.assign(new Error(`rehearsal_api_port_occupied:${port}`), {
      step: {
        command: `bind probe 127.0.0.1:${port}`,
        ok: false,
        status: 1,
        stdout: `port ${port} is already in use; the portfolio API cannot bind and the smoke would test a foreign service`,
        durationMs: probe.durationMs
      }
    });
  }
  return { port, available: true };
});

step('render-compose-model', commands => {
  const rendered = compose(['config']);
  commands.push(rendered);
  const violations = [];
  for (const [key, expected] of Object.entries(PAPER_ONLY_FLAGS)) {
    const match = rendered.stdout.match(new RegExp(`^\\s*${key}:\\s*"?([^"\\n]+)"?\\s*$`, 'm'));
    if (!match) {
      violations.push(`${key}:absent`);
    } else if (match[1].trim() !== expected) {
      violations.push(`${key}:${match[1].trim()}`);
    }
  }
  if (violations.length) {
    throw Object.assign(new Error(`rehearsal_paper_only_flags_violated:${violations.join(',')}`), {
      step: { command: 'paper-only flag inspection', ok: false, status: 1, stdout: violations.join('\n'), durationMs: 0 }
    });
  }
  return { paperOnlyFlagsVerified: Object.keys(PAPER_ONLY_FLAGS).length };
});

step('build-images', commands => {
  const built = compose(['build', '--pull'], { allowFailure: true });
  commands.push(built);
  if (!built.ok) {
    // A network-restricted rehearsal host cannot pull. Building from the local
    // context is still a real rehearsal of the image build, so fall back and
    // record that the pull was skipped rather than pretending it succeeded.
    const local = compose(['build']);
    commands.push(local);
    if (!local.ok) throw Object.assign(new Error('rehearsal_build_failed'), { step: local });
    return { pulled: false, built: true };
  }
  return { pulled: true, built: true };
});

step('start-postgres', commands => {
  commands.push(compose(['up', '-d', 'postgres'], { allowFailure: true }));
  const ps = compose(['ps', 'postgres'], { allowFailure: true });
  commands.push(ps);
  return { state: ps.stdout.trim().split('\n').slice(1).join(' | ') };
});

step('apply-migrations', commands => {
  const first = compose(['run', '--rm', 'migrate'], { allowFailure: true });
  commands.push(first);
  if (!first.ok) throw Object.assign(new Error('rehearsal_migrate_failed'), { step: first });
  // Idempotency is the property that matters: a second run must be a no-op.
  const second = compose(['run', '--rm', 'migrate']);
  commands.push(second);
  const skipped = /skipped|no migrations|already/i.test(second.stdout);
  if (!skipped) {
    throw Object.assign(new Error('rehearsal_migrate_not_idempotent'), { step: second });
  }
  return { idempotentSecondRun: true };
});

step('start-application', commands => {
  const up = compose(['up', '-d', 'api', 'economic-worker'], { allowFailure: true });
  commands.push(up);
  if (!up.ok) throw Object.assign(new Error('rehearsal_application_start_failed'), { step: up });

  // Give the healthchecks a chance, then require both services to be running.
  // Recording this step as passed while the containers failed to start is the
  // exact failure this rehearsal exists to catch.
  const deadline = Date.now() + 90_000;
  let ps = null;
  let running = [];
  while (Date.now() < deadline) {
    ps = compose(['ps', '--format', '{{.Service}}:{{.State}}']);
    const text = ps.stdout;
    running = ['api', 'economic-worker'].filter(service => new RegExp(`^${service}:running`, 'm').test(text));
    if (running.length === 2) break;
    run('bash', ['-c', 'sleep 3'], { capture: false });
  }
  if (ps) commands.push(ps);
  if (running.length !== 2) {
    throw Object.assign(new Error(`rehearsal_services_not_running:${running.join(',') || 'none'}`), {
      step: ps ?? { command: 'compose ps', ok: false, status: 1, stdout: '', durationMs: 0 }
    });
  }
  return { services: running };
});

step('predeploy-backup', commands => {
  const stamp = new Date().toISOString().replace(/[-:]/g, '').replace(/\..+/, 'Z');
  const target = path.join(backupDir, `predeploy-${releaseSha.slice(0, 12)}-${stamp}.dump`);
  const user = process.env.POSTGRES_USER || 'portfolio';
  const database = process.env.POSTGRES_DB || 'portfolio';
  const dump = run('bash', ['-c',
    `docker compose --env-file ${envFile} -f ${COMPOSE_FILE} exec -T postgres ` +
    `pg_dump -U ${user} -d ${database} --format=custom --no-owner > ${target}`
  ], { allowFailure: true });
  commands.push(dump);
  if (!dump.ok) throw Object.assign(new Error('rehearsal_backup_failed'), { step: dump });
  const stat = fs.statSync(target);
  if (stat.size === 0) throw Object.assign(new Error('rehearsal_backup_empty'), { step: dump });
  return { bytes: stat.size, path: target };
});

step('production-paper-smoke', commands => {
  // The smoke authenticates against the running API, so it needs the operator
  // token and the bound port from the rehearsal env, not the operator shell.
  const port = fileEnv.API_PORT || '3000';
  const baseUrl = `http://${fileEnv.API_BIND_ADDRESS || '127.0.0.1'}:${port}`;
  const smoke = run('npm', ['run', 'smoke:production-paper'], { allowFailure: true });
  commands.push(smoke);
  if (!smoke.ok) throw Object.assign(new Error('rehearsal_smoke_failed'), { step: smoke });
  return { smoke: 'passed', baseUrl };
});

step('teardown', commands => {
  // `down` without -v on purpose: the runbook prohibits `down -v` as a
  // rollback, and the rehearsal should exercise the same rule.
  commands.push(compose(['down'], { allowFailure: true }));
  return { volumesRemoved: false };
});

const record = {
  schemaVersion: 1,
  kind: 'portfolio-deployment-rehearsal',
  ok: failures.length === 0,
  releaseSha,
  startedAt: steps[0]?.startedAt ?? new Date().toISOString(),
  finishedAt: new Date().toISOString(),
  host: {
    platform: process.platform,
    arch: process.arch,
    node: process.version,
    docker: spawnSync('docker', ['version', '--format', '{{.Server.Version}}'], { encoding: 'utf8' }).stdout.trim() || null,
    cpus: Number(spawnSync('nproc', [], { encoding: 'utf8' }).stdout.trim() || 0)
  },
  composeFile: COMPOSE_FILE,
  // Loaded so a human can see what the rehearsal actually ran against, without
  // the record becoming a secret store.
  loadedEnvKeys: Object.keys(fileEnv).sort(),
  paperOnlyFlags: PAPER_ONLY_FLAGS,
  steps,
  failures,
  // A `--only` run executes a subset, so it cannot speak for the whole
  // sequence. It must not be mistaken for a rehearsal of this head: this flag
  // was found writing a one-step record over a complete ten-step one, which then
  // matched the head and made `release-status` treat an unrehearsed revision as
  // rehearsed. Partial runs are labelled and never copied over canonical evidence.
  partial: Boolean(options.only),
  onlyStep: options.only ?? null,
  expectedSteps: EXPECTED_STEPS,
  // Explicitly not a certification. This record shows the sequence ran on this
  // host. Whether this host is the intended deployment target is a human
  // judgement that this script has no way to make.
  certifiesTargetHost: false,
  liveTradingCertified: false
};

const missingSteps = EXPECTED_STEPS.filter(name => !steps.some(s => s.name === name));
record.complete = missingSteps.length === 0;
record.missingSteps = missingSteps;

const recordPath = path.join(workDir, 'rehearsal-record.json');
fs.writeFileSync(recordPath, `${JSON.stringify(record, null, 2)}\n`, { mode: 0o600 });

if (options.only) {
  // Deliberately does not copy into the evidence directory. A subset run is a
  // debugging aid; promoting it to evidence would let a one-step run certify a
  // head, which is exactly the false pass this guards against.
  process.stderr.write(
    `--only ${options.only}: wrote a PARTIAL record (${steps.length}/${EXPECTED_STEPS.length} steps). ` +
    'It was NOT copied to the evidence directory. Re-run without --only to produce a recordable rehearsal.\n'
  );
}

if (!options.keep && !options.only) {
  // The record and the pre-deploy dump are the evidence; the scratch tree is not.
  const evidenceDir = path.resolve(process.env.REHEARSAL_EVIDENCE_DIR || path.join(process.cwd(), 'data', 'rehearsal'));
  fs.mkdirSync(evidenceDir, { recursive: true });
  fs.copyFileSync(recordPath, path.join(evidenceDir, 'rehearsal-record.json'));
  for (const step of steps) {
    if (step.detail?.path) {
      fs.copyFileSync(step.detail.path, path.join(evidenceDir, path.basename(step.detail.path)));
    }
  }
  fs.rmSync(workDir, { recursive: true, force: true });
}

process.stdout.write(`${JSON.stringify({ ...record, recordPath: options.keep ? recordPath : null }, null, 2)}\n`);
process.exit(record.ok ? 0 : 1);
