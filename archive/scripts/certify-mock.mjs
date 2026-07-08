import { mkdirSync, writeFileSync } from 'node:fs';
import { runCertification } from '../packages/certification/src/mockCert.mjs';
import { fixtures } from '../packages/testing/src/fixtures.mjs';
const result=runCertification(fixtures);
mkdirSync('artifacts/certification',{recursive:true});
writeFileSync('artifacts/certification/mock-certification.json',JSON.stringify(result,null,2));
writeFileSync('artifacts/certification/mock-certification.md',`# Mock Certification\n\nPass: ${result.pass}\n\nLive trading certified: ${result.liveTradingCertified}\n\n${Object.entries(result.checks).map(([k,v])=>`- ${k}: ${v?'PASS':'FAIL'}`).join('\n')}\n`);
console.log(JSON.stringify(result,null,2));
process.exit(result.pass?0:1);
