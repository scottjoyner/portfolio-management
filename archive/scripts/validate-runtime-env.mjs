import { validateRuntimeEnv } from '../packages/config/src/runtimeEnv.mjs';

const result = validateRuntimeEnv(process.env);
console.log(JSON.stringify(result, null, 2));
if (!result.ok) process.exit(1);
