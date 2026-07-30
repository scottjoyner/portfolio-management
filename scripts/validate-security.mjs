#!/usr/bin/env node
import { lstatSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';

const ignoredDirs = new Set([
  '.git', 'node_modules', '.venv', '.venv_test', 'venv', '.cb_sdk_env', 'dist', 'build', 'archive',
  '.pytest_cache', '.mypy_cache', '.ruff_cache', 'data', 'state',
]);
const ignoredFiles = new Set(['pnpm-lock.yaml', 'package-lock.json']);
const ignoredPaths = new Set(['scripts/validate-security.mjs']);
const suspiciousPatterns = [
  { name: 'private_key_block', pattern: /-----BEGIN (RSA |EC |OPENSSH |PGP )?PRIVATE KEY-----/ },
  { name: 'aws_access_key', pattern: /AKIA[0-9A-Z]{16}/ },
  { name: 'github_token', pattern: /gh[pousr]_[A-Za-z0-9_]{20,}/ },
  { name: 'openai_key', pattern: /sk-[A-Za-z0-9_-]{32,}/ },
  { name: 'coinbase_private_key', pattern: /-----BEGIN EC PRIVATE KEY-----/ },
];
const skipped = [];

function walk(directory, out = []) {
  let entries;
  try {
    entries = readdirSync(directory);
  } catch (error) {
    skipped.push({ path: directory, reason: error.code || error.message });
    return out;
  }
  for (const entry of entries) {
    if (ignoredDirs.has(entry)) continue;
    const path = join(directory, entry).replace(/^\.\//, '');
    try {
      const link = lstatSync(path);
      if (link.isSymbolicLink()) {
        skipped.push({ path, reason: 'symbolic_link' });
        continue;
      }
      const stat = statSync(path);
      if (stat.isDirectory()) walk(path, out);
      else if (!ignoredFiles.has(entry) && !ignoredPaths.has(path) && stat.size < 1_000_000) out.push(path);
    } catch (error) {
      skipped.push({ path, reason: error.code || error.message });
    }
  }
  return out;
}

function genericAssignmentEligible(path) {
  return !/(^|\/)(tests?|docs?|examples?|fixtures?|mocks?)(\/|$)/i.test(path)
    && !/(^|\/)(readme|changelog|contributing)(\.|$)/i.test(path);
}

function suspiciousAssignment(content) {
  const findings = [];
  const pattern = /\b(secret|password|api[_-]?key|private[_-]?key|admin[_-]?token|csrf[_-]?token)\b\s*[:=]\s*(['"`])([^'"`\r\n]{12,})\2/ig;
  for (const match of content.matchAll(pattern)) {
    const value = String(match[3] || '').trim();
    if (/replace|example|placeholder|test|dummy|fake|sample|your|xxx|changeme|configured|process\.env|\$\{|<|\*\*\*/i.test(value)) continue;
    if (/\s/.test(value)) continue;
    findings.push({ rule: 'secret_assignment', preview: `${value.slice(0, 3)}***${value.slice(-3)}` });
  }
  return findings;
}

const packageJson = JSON.parse(readFileSync('package.json', 'utf8'));
const deps = { ...(packageJson.dependencies || {}), ...(packageJson.devDependencies || {}) };
const errors = [];
if (!deps.pg) errors.push('pg_dependency_required');

const requiredSecurityTokens = [
  ['docker-compose.production.yml', 'OPERATOR_AUTH_REQUIRED: "true"'],
  ['docker-compose.production.yml', 'CSRF_REQUIRED: "true"'],
  ['docker-compose.production.yml', 'LIVE_TRADING: "false"'],
  ['docker-compose.production.yml', 'REMOTE_LLM_EXECUTION_ENABLED: "false"'],
  ['docker-compose.production.yml', 'read_only: true'],
  ['docker-compose.production.yml', 'no-new-privileges:true'],
  ['packages/config/src/runtimeEnv.mjs', 'OPENROUTER_API_KEY is required when remote LLM execution is enabled'],
  ['packages/config/src/runtimeEnv.mjs', 'DATABASE_URL must not use the default local postgres password/host in production'],
];
for (const [path, token] of requiredSecurityTokens) {
  try {
    if (!readFileSync(path, 'utf8').includes(token)) errors.push(`security_contract_missing:${path}:${token}`);
  } catch {
    errors.push(`security_file_missing:${path}`);
  }
}

const scannedFiles = walk('.');
const findings = [];
for (const path of scannedFiles) {
  if (!/\.(mjs|js|json|yml|yaml|md|env|txt|toml|ini|py|sh)$/i.test(path)) continue;
  const content = readFileSync(path, 'utf8');
  for (const rule of suspiciousPatterns) if (rule.pattern.test(content)) findings.push({ path, rule: rule.name });
  if (genericAssignmentEligible(path)) {
    for (const finding of suspiciousAssignment(content)) findings.push({ path, ...finding });
  }
}

if (findings.length) errors.push(...findings.map(row => `secret_like_content:${row.path}:${row.rule}`));
if (errors.length) {
  process.stderr.write(`${JSON.stringify({ ok: false, errors, findings, skipped }, null, 2)}\n`);
  process.exit(1);
}
process.stdout.write(`${JSON.stringify({ ok: true, scannedFiles: scannedFiles.length, findings: 0, skipped }, null, 2)}\n`);
