import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {transformWithEsbuild} from 'vite';
import React from 'react';
import {renderToStaticMarkup} from 'react-dom/server';

async function component(file) {
  const source = readFileSync(new URL(file, import.meta.url), 'utf8');
  const {code: compiled} = await transformWithEsbuild(source, file, {loader:'jsx',format:'cjs'});
  const module = {exports:{}};
  new Function('require','module','exports',compiled)(createRequire(import.meta.url),module,module.exports);
  return module.exports;
}

test('tool cards render actual code, failures and successful recovery separately', async () => {
  const {ToolActivity, mergeToolStep} = await component('./ToolActivity.jsx');
  let steps = [{run_id:'r',step:1,name:'python',arguments:{code:'print(1/0)'},status:'running'}];
  steps = mergeToolStep(steps, {run_id:'r',step:1,status:'error',result:JSON.stringify({error:'ZeroDivisionError'})});
  steps = mergeToolStep(steps, {run_id:'r',step:2,name:'python',arguments:{code:'print(6*7)'},status:'done',result:JSON.stringify({stdout:'42'})});
  assert.equal(steps.length, 2);
  const html = renderToStaticMarkup(React.createElement(ToolActivity,{steps}));
  assert.match(html,/Agent tool activity/);
  assert.match(html,/print\(1\/0\)/);
  assert.match(html,/Code error/);
  assert.match(html,/ZeroDivisionError/);
  assert.match(html,/42/);
});

test('tool results are escaped instead of executed as HTML', async () => {
  const {ToolActivity} = await component('./ToolActivity.jsx');
  const html = renderToStaticMarkup(React.createElement(ToolActivity,{steps:[{
    run_id:'r',step:1,name:'terminal',arguments:{command:'echo test'},status:'done',result:'<script>alert(1)</script>'}]}));
  assert.doesNotMatch(html,/<script>/);
  assert.match(html,/&lt;script&gt;/);
});

test('assistant markdown renders code safely without remote image requests', async () => {
  const {default: MessageContent} = await component('./MessageContent.jsx');
  const text = '## Result\n\n**Passed**\n\n```python\nprint(42)\n```\n\n<script>alert(1)</script>\n\n![tracking](https://example.com/pixel)\n\n[bad](javascript:alert(1))';
  const html = renderToStaticMarkup(React.createElement(MessageContent,{text}));
  assert.match(html,/<h2>Result<\/h2>/);
  assert.match(html,/<strong>Passed<\/strong>/);
  assert.match(html,/<pre><code class="language-python">/);
  assert.doesNotMatch(html,/<script|<img|href="javascript:/);
});
