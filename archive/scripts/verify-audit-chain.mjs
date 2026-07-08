import { createOperatorStore } from '../packages/storage/src/operatorStoreFactory.mjs';
import { verifyAuditIntegrity } from '../packages/audit/src/integrity.mjs';

const store = createOperatorStore({ kind: process.env.OPERATOR_STORE === 'postgres' ? 'postgres' : undefined });
const state = await store.load();
const result = verifyAuditIntegrity(state.audit || []);
console.log(JSON.stringify(result, null, 2));
if (!result.ok) process.exit(1);
