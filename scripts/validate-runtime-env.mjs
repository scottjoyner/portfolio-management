#!/usr/bin/env node
import { validateRuntimeEnv } from '../packages/config/src/runtimeEnv.mjs';

const result = validateRuntimeEnv(process.env);
process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
if (!result.ok) process.exitCode = 1;
