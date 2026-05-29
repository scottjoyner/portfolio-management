import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';

const ignoredDirs = new Set(['.git', 'node_modules', '.venv', 'venv', 'dist', 'build', '.pytest_cache', '.mypy_cache', '.ruff_cache']);
const ignoredFiles = new Set(['pnpm-lock.yaml', 'package-lock.json']);
const suspiciousPatterns = [
  { name: 'private_key_block', pattern: /-----BEGIN (RSA |EC |OPENSSH |PGP )?PRIVATE KEY-----/ },
  { name: 'aws_access_key', pattern: /AKIA[0-9A-Z]{16}/ },
  { name: 'github_token', pattern: /gh[pousr]_[A-Za-z0-9_]{20,}/ },
  { name: 'generic_secret_assignment', pattern: /(secret|password|api[_-]?key|private[_-]?key)\s*[:=]\s*['\"][^'\"]{16,}['\"]/i }
];

function walk(dir, out = []) {
  for (const entry of readdirSync(dir)) {
    if (ignoredDirs.has(entry)) continue;
    const path = join(dir, entry);
    const stat = statSync(path);
    if (stat.isDirectory()) walk(path, out);
    else if (!ignoredFiles.has(entry) && stat.size < 1_000_000) out.push(path);
  }
  return out;
}

const packageJson = JSON.parse(readFileSync('package.json', 'utf8'));
const deps = { ...(packageJson.dependencies || {}), ...(packageJson.devDependencies || {}) };
if (!deps.pg) {
  console.error('security validation failed: pg dependency is required for Postgres runtime path');
  process.exit(1);
}

const findings = [];
for (const path of walk('.')) {
  if (!/\.(mjs|js|json|yml|yaml|md|env|txt|toml|ini|py|sh)$/i.test(path)) continue;
  const content = readFileSync(path, 'utf8');
  for (const rule of suspiciousPatterns) {
    if (rule.pattern.test(content)) findings.push({ path, rule: rule.name });
  }
}

const allowed = new Set([
  './docs/OPERATOR_RUNBOOK_P0_P1.md:generic_secret_assignment',
  './README.md:generic_secret_assignment'
]);
const unexpected = findings.filter(finding => !allowed.has(`${finding.path}:${finding.rule}`));
if (unexpected.length) {
  console.error('security validation failed: suspicious secret-like content detected');
  for (const finding of unexpected) console.error(`- ${finding.path}: ${finding.rule}`);
  process.exit(1);
}

console.log('security validation ok');
