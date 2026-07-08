import { existsSync, readFileSync } from 'node:fs';

const requiredFiles = [
  'packages/storage/src/migrations/003_audit_and_certification.sql',
  'packages/storage/src/auditChain.mjs',
  'packages/adapters/src/contracts.mjs',
  'packages/backtesting/src/replayEngine.mjs',
  'docs/FIRST_PROD_RELEASE_CHECKLIST.md'
];

for (const path of requiredFiles) {
  if (!existsSync(path)) {
    console.error(`first-prod validation failed: missing ${path}`);
    process.exit(1);
  }
}

const migration = readFileSync('packages/storage/src/migrations/003_audit_and_certification.sql', 'utf8');
const audit = readFileSync('packages/storage/src/auditChain.mjs', 'utf8');
const adapters = readFileSync('packages/adapters/src/contracts.mjs', 'utf8');
const replay = readFileSync('packages/backtesting/src/replayEngine.mjs', 'utf8');
const checklist = readFileSync('docs/FIRST_PROD_RELEASE_CHECKLIST.md', 'utf8');

const checks = [
  [migration.includes('adapter_certifications'), 'migration must include adapter_certifications table'],
  [migration.includes('previous_hash') && migration.includes('event_hash') && migration.includes('sequence_number'), 'migration must include audit hash-chain fields'],
  [audit.includes('verifyAuditChain') && audit.includes('hashAuditEvent'), 'audit chain helper must include hash and verification functions'],
  [adapters.includes('assertAdapterCertification') && adapters.includes('adapter_live_not_certified'), 'adapter contracts must enforce certification gates'],
  [replay.includes('replayMovingAverageCross') && replay.includes('validateHistoricalBars'), 'replay engine must expose replay and bar validation'],
  [checklist.includes('Live trading remains disabled'), 'release checklist must document live trading disabled'],
  [checklist.includes('pnpm test') && checklist.includes('pnpm build'), 'release checklist must include test/build gates']
];

const failures = checks.filter(([ok]) => !ok).map(([, message]) => message);
if (failures.length) {
  console.error('first-prod validation failed:');
  for (const failure of failures) console.error(`- ${failure}`);
  process.exit(1);
}

console.log('first-prod release validation ok');
