#!/usr/bin/env node
import { createOperatorStore } from '../packages/storage/src/operatorStoreFactory.mjs';
import { verifyAuditChain } from '../packages/storage/src/auditChain.mjs';

const store = createOperatorStore();
try {
  const state = await store.load();
  const result = verifyAuditChain(state.audit || []);
  process.stdout.write(`${JSON.stringify({ ...result, storage: store.getStatus?.() || null }, null, 2)}\n`);
  if (!result.ok) process.exitCode = 1;
} catch (error) {
  process.stderr.write(`${JSON.stringify({ ok: false, error: error.message || String(error) }, null, 2)}\n`);
  process.exitCode = 1;
} finally {
  try {
    await store.close?.();
  } catch {
    // Verification result remains authoritative; shutdown errors are non-destructive.
  }
}
