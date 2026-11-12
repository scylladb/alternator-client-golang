// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const {createRequire} = require('node:module');
const {test} = require('node:test');
const yaml = require('js-yaml');
const guard = require('../scripts/release-notes-guard.cjs');

const repo = {owner: 'scylladb', repo: 'alternator-client-golang'};
const label = 'omit-from-release-notes';
const file = (filename, extra = {}) => ({filename, status: 'modified', ...extra});
const workflowText = readFileSync(join(__dirname, '../workflows/label-release-notes.yml'), 'utf8');
const workflow = yaml.load(workflowText);
const steps = workflow.jobs.classify.steps;
const scripts = steps.filter(step => step.with?.script).map(step => step.with.script);
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const requireFromWorkspace = createRequire(join(__dirname, '../../workflow-adapter.cjs'));

function fixture(options = {}) {
  const files = options.files ?? [file('README.md')];
  const labels = new Set(options.labels ?? ['unrelated']);
  const calls = [];
  let gets = 0;
  let listings = 0;
  const initial = {number: 172, head: {sha: 'head'}, base: {sha: 'base'}, changed_files: files.length};
  const github = {rest: {pulls: {
    get: async params => {
      assert.deepEqual(params, {...repo, pull_number: 172});
      calls.push('get');
      gets++;
      if (options.failGetAt === gets) throw options.getError ?? new Error('PR lookup unavailable');
      return {data: {...initial, ...options.revisions?.[gets - 1]}};
    },
    listFiles: () => {}
  }, issues: {
    listLabelsOnIssue: () => {},
    removeLabel: async params => {
      assert.deepEqual(params, {...repo, issue_number: 172, name: label});
      calls.push('remove');
      if (options.cleanupFailure) throw new Error('Removal unavailable');
      const actual = [...labels].find(name => name.toLowerCase() === label);
      if (!actual) throw {status: 404};
      labels.delete(actual);
    }
  }}, paginate: async (method, params) => {
    assert.equal(params.per_page, 100);
    if (method === github.rest.pulls.listFiles) {
      assert.deepEqual(params, {...repo, pull_number: 172, per_page: 100});
      calls.push('files');
      if (options.fileFailure) throw new Error('File listing unavailable');
      return options.listings?.[listings++] ?? files;
    }
    assert.equal(method, github.rest.issues.listLabelsOnIssue);
    assert.deepEqual(params, {...repo, issue_number: 172, per_page: 100});
    calls.push('labels');
    if (options.labelFailure) throw new Error('Labels unavailable');
    return options.labelMetadata ?? [...labels].map(name => ({name}));
  }};
  return {github, labels, calls};
}

async function adapter(f, hash, overrides = {}, load = requireFromWorkspace) {
  const env = {PR_NUMBER: '172', CONFIRMED_PR: 'true', TARGET_OUTCOME: 'success',
    CHECKOUT_OUTCOME: 'success', INSTALL_OUTCOME: 'success',
    GUARD_OUTCOME: 'success', LABELER_OUTCOME: 'success', UNSAFE_RENAME: 'false',
    SNAPSHOT_HASH: hash, ...overrides};
  const logs = [];
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', steps.at(-1).with.script);
  let error;
  try {
    await run(f.github, {repo}, {info: text => logs.push(text), error: text => logs.push(text)},
      load, {env});
  } catch (caught) { error = caught; }
  return {error, logs};
}

test('canonical Go policy keeps support-only changes eligible and shipped/build inputs visible', async () => {
  const files = ['.github/workflows/tests.yml', '.github/workflows/call_jira_sync.yml',
    '.github/workflows/label-release-notes.yml', '.github/RELEASE_NOTES.md', '.github/labeler.yml',
    '.github/scripts/release-notes-guard.cjs', '.github/tests/release-notes-guard.test.cjs',
    '.github/package.json', '.github/package-lock.json', 'docs/a/b.md', 'README.md',
    'shared/tests/fixture.go', 'shared/http_test.go', 'shared/rt/scope_test.go',
    'sdkv1/helper_test.go', 'sdkv2/helper_test.go', 'test/docker-compose.yml',
    'scripts/release/SIGNING.md', '.golangci.yml', '.gitignore', '.editorconfig'].map(name => file(name));
  assert.equal((await guard.snapshot(fixture({files}).github, repo, 172)).omit, true);
  for (const name of ['sdkv2/helper.go', 'shared/rt/scope.go', 'shared/go.mod', 'shared/go.sum',
    'sdkv1/go.mod', 'sdkv2/go.sum', 'go.work', 'go.work.sum', 'Makefile',
    'scripts/release/pipeline.py', '.github/workflows/release.yml',
    '.github/workflows/release-signing.yml', '.github/release.yml',
    '.github/unknown.yml', '.github/scripts/publish.cjs', 'LICENSE', 'unknown.txt']) {
    const current = await guard.snapshot(fixture({files: [...files, file(name)]}).github, repo, 172);
    assert.equal(current.omit, false, name);
  }
});

test('renames use the actual canonical policy for both prior and current paths', async () => {
  for (const [previous, omit] of [['docs/old.md', true], ['sdkv2/helper.go', false], ['unknown', false]]) {
    const current = await guard.snapshot(fixture({files: [file('docs/new.md', {
      status: 'renamed', previous_filename: previous})]}).github, repo, 172);
    assert.equal(current.omit, omit);
    assert.equal(current.unsafeRename, !omit);
  }
});

test('unsupported config shapes fail instead of approximating a second policy', () => {
  for (const source of ['{}', 'omit-from-release-notes: []',
    'omit-from-release-notes: [{head-branch: [main]}]',
    'omit-from-release-notes: [{changed-files: [{any-glob-to-any-file: ["**"]}]}]',
    'omit-from-release-notes: [{changed-files: [{all-globs-to-all-files: []}]}]',
    'other-label: [{changed-files: [{all-globs-to-all-files: ["**"]}]}]']) {
    assert.throws(() => guard.policy(source), /Unsupported labeler configuration/);
  }
});

test('PR input rejects malformed, ambiguous and unsafe numbers', () => {
  assert.equal(guard.parseNumber('172'), 172);
  for (const value of ['', '0', '-1', '01', '1.0', '1e2', ' 172', '172\n',
    '172,21', '${{ arbitrary }}', '9007199254740992', undefined, 172]) {
    assert.throws(() => guard.parseNumber(value), /canonical positive safe PR number/);
  }
});

test('pagination requests full metadata and accepts a complete listing beyond one page', async () => {
  const f = fixture({files: Array.from({length: 105}, (_, i) => file(`docs/${i}.md`))});
  const snapshot = await guard.snapshot(f.github, repo, 172);
  assert.equal(snapshot.count, 105);
  assert.equal(snapshot.omit, true);
  assert.match(snapshot.hash, /^[0-9a-f]{64}$/);
  assert.deepEqual(f.calls, ['get', 'files', 'get']);
});

test('zero, incomplete, truncated, duplicate and invalid file evidence fail visibly', async () => {
  const cases = [
    {files: []},
    {revisions: [{changed_files: 2}, {changed_files: 2}]},
    {files: [file('README.md'), file('README.md')]},
    {files: [file('README.md', {status: 'renamed'})]},
    {files: [file('README.md', {status: undefined})]},
    {files: [file('README.md', {previous_filename: ''})]},
    {files: [file('README.md')], listings: [[]]}
  ];
  for (const options of cases) await assert.rejects(guard.snapshot(fixture(options).github, repo, 172));
});

test('head, base and count changes during collection are rejected', async () => {
  for (const changed of [{head: {sha: 'new'}}, {base: {sha: 'new'}}, {changed_files: 2}]) {
    const f = fixture({revisions: [{}, changed]});
    await assert.rejects(guard.snapshot(f.github, repo, 172), /revisions changed/);
  }
});

test('verification detects before/after revisions and filename, status or prior-path changes', async () => {
  const original = await guard.snapshot(fixture().github, repo, 172);
  for (const options of [
    {revisions: [{head: {sha: 'new'}}, {head: {sha: 'new'}}]},
    {revisions: [{base: {sha: 'new'}}, {base: {sha: 'new'}}]},
    {files: [file('docs/new.md')]},
    {files: [file('README.md', {status: 'added'})]},
    {files: [file('README.md', {status: 'renamed', previous_filename: 'docs/old.md'})]}
  ]) {
    await assert.rejects(guard.verify(fixture(options).github, repo, 172, original.hash), /snapshot changed/);
  }
  const f = fixture({labels: [label], revisions: [{}, {}, {head: {sha: 'new'}}]});
  await assert.rejects(guard.verify(f.github, repo, 172, original.hash), /revisions changed during label/);
});

test('lookup, listing and actual-label lookup failures propagate', async () => {
  await assert.rejects(guard.snapshot(fixture({failGetAt: 1}).github, repo, 172), /lookup unavailable/);
  await assert.rejects(guard.snapshot(fixture({failGetAt: 2}).github, repo, 172), /lookup unavailable/);
  await assert.rejects(guard.snapshot(fixture({fileFailure: true}).github, repo, 172), /listing unavailable/);
  const original = await guard.snapshot(fixture().github, repo, 172);
  await assert.rejects(guard.verify(fixture({labelFailure: true}).github, repo, 172, original.hash), /Labels unavailable/);
});

test('actual-label verification catches stale state after internal action skip', async () => {
  for (const [files, labels] of [[[file('README.md')], []], [[file('sdkv2/helper.go')], [label]]]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 172);
    const {error} = await adapter(f, original.hash);
    assert.match(error.message, /Actual omission label/);
    assert.equal(f.labels.has(label), false);
    assert.equal(f.labels.has('unrelated'), true);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('failed final snapshot, lookup or label retrieval attempts cleanup', async () => {
  const original = await guard.snapshot(fixture().github, repo, 172);
  for (const options of [{files: [file('docs/changed.md')]}, {failGetAt: 1},
    {fileFailure: true}, {labelFailure: true}]) {
    const f = fixture({...options, labels: [label, 'unrelated']});
    const {error} = await adapter(f, original.hash);
    assert.ok(error);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('successful verification checks actual labels and preserves unrelated labels', async () => {
  for (const [files, labels] of [[[file('README.md')], [label]], [[file('sdkv2/helper.go')], []]]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 172);
    const {error, logs} = await adapter(f, original.hash);
    assert.ifError(error);
    assert.match(logs[0], /Verified head=head base=base count=1 snapshot=.*actual label checked/);
    assert.equal(f.labels.has('unrelated'), true);
    assert.ok(!f.calls.includes('remove'));
  }
});

test('intentional unsafe-rename skip removes omission label and verifies visible state', async () => {
  const f = fixture({files: [file('docs/new.md', {status: 'renamed', previous_filename: 'sdkv2/helper.go'})],
    labels: [label, 'unrelated']});
  const original = await guard.snapshot(f.github, repo, 172);
  const {error, logs} = await adapter(f, original.hash, {UNSAFE_RENAME: 'true', LABELER_OUTCOME: 'skipped'});
  assert.ifError(error);
  assert.match(logs[0], /omit=false; actual label checked/);
  assert.deepEqual([...f.labels], ['unrelated']);
  assert.equal(f.calls.filter(call => call === 'remove').length, 1);
});

test('failed or skipped setup, guard and labeler clean up without requiring the helper', async () => {
  for (const key of ['TARGET_OUTCOME', 'CHECKOUT_OUTCOME', 'INSTALL_OUTCOME', 'GUARD_OUTCOME', 'LABELER_OUTCOME']) {
    for (const outcome of ['failure', 'skipped', 'cancelled', '']) {
      const f = fixture({labels: [label, 'unrelated']});
      const {error} = await adapter(f, '', {[key]: outcome}, () => { throw new Error('Must not require helper'); });
      assert.match(error.message, /predecessor failed or unexpectedly skipped/);
      assert.deepEqual([...f.labels], ['unrelated']);
      assert.deepEqual(f.calls, ['remove']);
    }
  }
  const f = fixture({labels: [label]});
  const {error} = await adapter(f, '', {}, () => { throw new Error('Helper missing'); });
  assert.match(error.message, /Helper missing/);
  assert.deepEqual(f.calls, ['remove']);
});

test('cleanup API failures remain visible without losing the original failure or retrying', async () => {
  const f = fixture({cleanupFailure: true});
  const result = await adapter(f, '', {INSTALL_OUTCOME: 'failure'});
  assert.match(result.error.message, /predecessor failed/);
  assert.match(result.logs[0], /cleanup failed: Removal unavailable/);
  assert.deepEqual(f.calls, ['remove']);
  const unsafe = fixture({cleanupFailure: true});
  const result2 = await adapter(unsafe, '', {UNSAFE_RENAME: 'true', LABELER_OUTCOME: 'skipped'});
  assert.match(result2.error.message, /Removal unavailable/);
  assert.deepEqual(unsafe.calls, ['remove']);
});

test('workflow adapters use environment input and emit bounded verified snapshot outputs', async () => {
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process',
    steps.find(step => step.id === 'guard').with.script);
  const outputs = {};
  const f = fixture();
  await run(f.github, {repo}, {setOutput: (key, value) => { outputs[key] = value; }, info: () => {}},
    requireFromWorkspace, {env: {PR_NUMBER: '172'}});
  assert.deepEqual(Object.keys(outputs), ['hash', 'unsafe-rename']);
  assert.equal(outputs['unsafe-rename'], 'false');
  const invalid = await adapter(f, '', {PR_NUMBER: '01'});
  assert.match(invalid.error.message, /canonical positive safe PR number/);
  assert.equal(steps.at(-1).if, 'always()');
  const checkout = steps.find(step => step.id === 'checkout');
  assert.equal(checkout.with.ref, '${{ github.event.repository.default_branch }}');
  assert.equal(checkout.with['persist-credentials'], false);
  assert.equal(steps.find(step => step.id === 'install').run, 'npm ci --ignore-scripts --prefix .github');
  const action = steps.find(step => step.id === 'labeler');
  assert.equal(action.if, "success() && steps.guard.outputs.unsafe-rename == 'false'");
  assert.equal(action.with['sync-labels'], true);
  assert.equal(action.with.dot, true);
  assert.equal(action.with['pr-number'], '${{ env.PR_NUMBER }}');
  for (const script of scripts) assert.ok(!script.includes('${{'));
});

test('runtime-to-test rename is withheld while support-to-support rename remains eligible', async () => {
  for (const [previous, omit] of [['sdkv2/helper.go', false], ['shared/old_test.go', true]]) {
    const current = await guard.snapshot(fixture({files: [file('sdkv2/new_test.go', {
      status: 'renamed', previous_filename: previous})]}).github, repo, 172);
    assert.equal(current.omit, omit);
    assert.equal(current.unsafeRename, !omit);
  }
});

test('actual local Jira predicate skips only the exact marker label events', () => {
  const jira = yaml.load(readFileSync(join(__dirname, '../workflows/call_jira_sync.yml'), 'utf8'));
  const run = new Function('github', 'return (' + jira.jobs['jira-sync'].if + ');');
  // GitHub expression string comparisons ignore case; model that identity here.
  const evaluate = github => run({event: {...github.event, ...(github.event.label ? {
    label: {name: github.event.label.name.toLowerCase()}
  } : {})}});
  for (const action of jira.on.pull_request_target.types) {
    for (const name of [label, label.toUpperCase(), label + '-extra', 'prefix-' + label, 'other-label', '']) {
      assert.equal(evaluate({event: {action, label: {name}}}),
        !(['labeled', 'unlabeled'].includes(action) && name.toLowerCase() === label), action + ': ' + name);
    }
    if (!['labeled', 'unlabeled'].includes(action)) {
      assert.equal(evaluate({event: {action}}), true, action);
    }
  }
});

test('actual label identity accepts mixed case for support and removes it for runtime', async () => {
  const marker = 'Omit-From-Release-Notes';
  for (const [filename, omit] of [['README.md', true], ['sdkv2/helper.go', false]]) {
    const f = fixture({files: [file(filename)], labels: [marker, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 172);
    const {error} = await adapter(f, original.hash);
    if (omit) {
      assert.ifError(error);
      assert.equal(f.labels.has(marker), true);
      assert.ok(!f.calls.includes('remove'));
    } else {
      assert.match(error.message, /Actual omission label/);
      assert.equal(f.labels.has(marker), false);
      assert.equal(f.calls.filter(call => call === 'remove').length, 1);
    }
    assert.equal(f.labels.has('unrelated'), true);
  }
});

test('marker prefix and suffix names remain unrelated labels', async () => {
  const names = ['prefix-' + label, label + '-extra'];
  const runtime = fixture({files: [file('sdkv2/helper.go')], labels: names});
  const initial = await guard.snapshot(runtime.github, repo, 172);
  assert.ifError((await adapter(runtime, initial.hash)).error);
  assert.deepEqual([...runtime.labels], names);
  const support = fixture({labels: names});
  const supportInitial = await guard.snapshot(support.github, repo, 172);
  assert.match((await adapter(support, supportInitial.hash)).error.message, /Actual omission label/);
  assert.deepEqual([...support.labels], names);
});

test('malformed actual label metadata fails visibly and attempts canonical cleanup', async () => {
  for (const metadata of [[null], [{}], [{name: null}], [{name: 1}], [{name: ''}]]) {
    const f = fixture({labels: ['Omit-From-Release-Notes', 'unrelated'], labelMetadata: metadata});
    const original = await guard.snapshot(f.github, repo, 172);
    const {error} = await adapter(f, original.hash);
    assert.match(error.message, /Invalid label metadata/);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

async function confirmTarget(f, eventName = 'workflow_dispatch', payload = {}) {
  const outputs = {};
  const target = steps.find(step => step.id === 'target');
  const run = new AsyncFunction('github', 'context', 'core', 'process', target.with.script);
  let error;
  try {
    await run(f.github, {repo, eventName, payload},
      {setOutput: (key, value) => { outputs[key] = value; }}, {env: {PR_NUMBER: '172'}});
  } catch (caught) { error = caught; }
  return {outputs, error};
}

test('unconfirmed manual ordinary issues and lookup failures cause zero label writes', async () => {
  for (const getError of [Object.assign(new Error('Not Found'), {status: 404}),
    new Error('API unavailable')]) {
    const f = fixture({labels: [label, 'unrelated'], failGetAt: 1, getError});
    const confirmation = await confirmTarget(f);
    assert.equal(confirmation.error, getError);
    assert.deepEqual(confirmation.outputs, {});
    const result = await adapter(f, '', {CONFIRMED_PR: '', TARGET_OUTCOME: 'failure',
      CHECKOUT_OUTCOME: 'skipped'}, () => { throw new Error('Must not load helper'); });
    assert.match(result.error.message, /not confirmed as a pull request/);
    assert.deepEqual([...f.labels], [label, 'unrelated']);
    assert.deepEqual(f.calls, ['get']);
  }
});

test('mismatched manual or trusted event identities never authorize label writes', async () => {
  for (const eventName of ['workflow_dispatch', 'pull_request_target']) {
    const f = fixture({labels: [label], revisions: [{number: 173}]});
    const confirmation = await confirmTarget(f, eventName, {pull_request: {number: 173}});
    assert.match(confirmation.error.message, /not the requested pull request/);
    assert.deepEqual(confirmation.outputs, {});
    const result = await adapter(f, '', {CONFIRMED_PR: '', TARGET_OUTCOME: 'failure'});
    assert.match(result.error.message, /not confirmed as a pull request/);
    assert.deepEqual([...f.labels], [label]);
    assert.deepEqual(f.calls, eventName === 'workflow_dispatch' ? ['get'] : []);
  }
});

test('confirmed manual PRs retain cleanup after every later setup or classifier failure', async () => {
  for (const key of ['CHECKOUT_OUTCOME', 'INSTALL_OUTCOME', 'GUARD_OUTCOME', 'LABELER_OUTCOME']) {
    const f = fixture({labels: [label, 'unrelated'], failGetAt: 2});
    const confirmation = await confirmTarget(f);
    assert.ifError(confirmation.error);
    assert.deepEqual(confirmation.outputs, {'confirmed-pr': 'true'});
    const result = await adapter(f, '', {CONFIRMED_PR: confirmation.outputs['confirmed-pr'],
      [key]: 'failure'}, () => { throw new Error('Must not load helper'); });
    assert.match(result.error.message, /predecessor failed or unexpectedly skipped/);
    assert.deepEqual(f.calls, ['get', 'remove']);
    assert.deepEqual([...f.labels], ['unrelated']);
  }
});

test('trusted matching PR events remain confirmed despite later lookup or helper failure', async () => {
  const f = fixture({labels: [label, 'unrelated'], failGetAt: 1});
  const confirmation = await confirmTarget(f, 'pull_request_target', {pull_request: {number: 172}});
  assert.ifError(confirmation.error);
  assert.deepEqual(confirmation.outputs, {'confirmed-pr': 'true'});
  assert.deepEqual(f.calls, []);
  const result = await adapter(f, '', {CONFIRMED_PR: confirmation.outputs['confirmed-pr']});
  assert.match(result.error.message, /PR lookup unavailable/);
  assert.deepEqual(f.calls, ['get', 'remove']);
  assert.deepEqual([...f.labels], ['unrelated']);
});

test('target confirmation is before setup and finalization consumes its independent output', () => {
  assert.equal(steps[0].id, 'target');
  assert.ok(steps.findIndex(step => step.id === 'target') < steps.findIndex(step => step.id === 'checkout'));
  assert.ok(steps.findIndex(step => step.id === 'target') < steps.findIndex(step => step.id === 'install'));
  assert.equal(steps.at(-1).env.CONFIRMED_PR, '${{ steps.target.outputs.confirmed-pr }}');
  assert.equal(steps.at(-1).env.TARGET_OUTCOME, '${{ steps.target.outcome }}');
  assert.equal(workflow.on.workflow_dispatch.inputs.pull_request.type, 'string');
  assert.equal(workflow.jobs.classify.env.PR_NUMBER,
    '${{ github.event.pull_request.number || inputs.pull_request }}');
});
