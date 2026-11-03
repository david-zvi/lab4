// Checks dashboard.html's script: it compiles, and md() escapes before formatting.
//     node test_page.js
const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

const src = fs.readFileSync(`${__dirname}/dashboard.html`, 'utf8').split('<script>')[1].split('</script>')[0];
new vm.Script(src);  // syntax

const ctx = {};
vm.runInNewContext(src.split('// ---- markdown ----')[1].split('// ---- settings ----')[0] + '\nthis.md = md;', ctx);
const md = ctx.md;

for (const evil of ['<img src=x onerror=alert(1)>', '<script>alert(1)</script>', '[x](javascript:alert(1))', '`<b>`', '| <i> | a |\n|---|---|\n| <s> | b |']) {
  assert(!/<(img|script|s|i)[ >]|href="javascript/.test(md(evil).replace(/<\/?(p|br|code|table|tr|th|td|i)>/g, '')), evil);
}
assert.strictEqual(md('`a*b*`'), '<p><code>a*b*</code></p>');
assert.strictEqual(md('**b** and *i*'), '<p><b>b</b> and <i>i</i></p>');
assert.strictEqual(md('# T\n- a\n- b\n1. c'), '<h1>T</h1><ul><li>a</li><li>b</li></ul><ol><li>c</li></ol>');
assert.strictEqual(md('```\n<x>\n```'), '<pre><code>&lt;x&gt;</code></pre>');
assert.strictEqual(md('[go](https://a.b/c_d_e)'), '<p><a href="https://a.b/c_d_e" target="_blank" rel="noreferrer">go</a></p>');
assert.strictEqual(md('| a | b |\n|---|---|\n| 1 | 2 |'), '<table><tr><th>a</th><th>b</th></tr><tr><td>1</td><td>2</td></tr></table>');
assert.strictEqual(md('| not a table |'), '<p>| not a table |</p>');
console.log('page ok');
