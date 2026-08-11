#!/usr/bin/env node
'use strict';

/**
 * Runs the unit test suite (src/**\/*.test.ts) via Node's built-in test
 * runner, in transpile-only ts-node.
 *
 * This exists instead of a plain `node --test 'src/**\/*.test.ts'` npm
 * script for two reasons:
 *   1. Cross-platform: npm scripts are executed via different shells on
 *      different OSes (bash vs cmd.exe), which quote/expand globs
 *      differently. Walking the filesystem in plain Node and passing
 *      explicit file paths to `--test` sidesteps that entirely.
 *   2. Robustness across Node versions: explicit file arguments are the
 *      most universally supported form of `node --test`, rather than
 *      relying on a specific Node version's glob-matching behavior.
 */

const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

function findTestFiles(dir, out) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.name === 'node_modules') continue;
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      findTestFiles(full, out);
    } else if (entry.isFile() && entry.name.endsWith('.test.ts')) {
      out.push(full);
    }
  }
  return out;
}

const srcDir = path.join(__dirname, '..', 'src');
const testFiles = findTestFiles(srcDir, []);

if (testFiles.length === 0) {
  console.error(`No *.test.ts files found under ${srcDir}`);
  process.exit(1);
}

console.log(`Running ${testFiles.length} test file(s):`);
for (const f of testFiles) {
  console.log(`  ${path.relative(process.cwd(), f)}`);
}
console.log('');

const result = spawnSync(
  process.execPath,
  ['-r', 'ts-node/register/transpile-only', '--test', ...testFiles],
  { stdio: 'inherit' }
);

if (result.error) {
  console.error(result.error);
  process.exit(1);
}
process.exit(result.status === null ? 1 : result.status);
