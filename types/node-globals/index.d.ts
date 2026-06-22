declare const process: any;
declare const Buffer: any;

declare function fetch(input: any, init?: any): Promise<any>;

declare module 'node:fs' {
  export const readFileSync: any;
  export const writeFileSync: any;
  export const existsSync: any;
  export const mkdirSync: any;
  export const rmSync: any;
  export const readdirSync: any;
  export const readFile: any;
  export const statSync: any;
  export const copyFileSync: any;
  export const unlinkSync: any;
  export const renameSync: any;
}

declare module 'node:path' {
  export const resolve: any;
  export const join: any;
  export const dirname: any;
  export const basename: any;
  export const extname: any;
}

declare module 'node:child_process' {
  export const exec: any;
  export const execSync: any;
  export const spawn: any;
  export const spawnSync: any;
}

declare module 'node:crypto' {
  export const createHmac: any;
  export const createHash: any;
  export const createPrivateKey: any;
  export const sign: any;
  export const randomUUID: any;
  export const constants: any;
}
