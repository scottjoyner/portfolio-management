import { MemoryOperatorStore, FileOperatorStore } from './operatorStore.mjs';
import { PostgresOperatorStore } from './postgresOperatorStore.mjs';

export function createOperatorStore(options = {}) {
  if (options.store) return options.store;
  if (options.state) return new MemoryOperatorStore(options.state);
  if (options.persist === false || process.env.OPERATOR_STATE_DISABLED === 'true') return new MemoryOperatorStore(options.seedState);
  if (options.kind === 'postgres' || process.env.OPERATOR_STORE === 'postgres') return new PostgresOperatorStore(options);
  return new FileOperatorStore(options.filePath, { seedState: options.seedState });
}
