/**
 * The decoder, run against payloads rather than only type-checked.
 *
 * `npm run typecheck` proves React cannot reach for a raw statistical value. It proves nothing
 * about what happens when the server sends a field this build does not recognise, or sends the
 * boolean as a string, or stops sending the banner — and those are the ways a banner disappears
 * in production while every type still lines up.
 *
 * The negative fixture is the point of the file: strip `display_banner`, keep the structured
 * state, and the semantics must survive. If they do not, the copy was the contract.
 */
// `process` without @types/node, declared narrowly rather than pulling a whole type package
// in for one call. The runner needs a non-zero exit or a failing decoder test is green in CI.
declare const process: { exit(code: number): never };

import { decodeRankingProvenance, decodeRun, TransportContractError } from '../semantics/decode.js';

let ok = 0;
let bad = 0;

function check(name: string, fn: () => void) {
  try {
    fn();
    console.log(`  PASS  ${name}`);
    ok += 1;
  } catch (e) {
    console.log(`  FAIL  ${name}: ${String(e)}`);
    bad += 1;
  }
}

function assert(cond: boolean, msg: string) {
  if (!cond) throw new Error(msg);
}

function throws(fn: () => unknown, msg: string) {
  try {
    fn();
  } catch (e) {
    if (e instanceof TransportContractError) return;
    throw new Error(`${msg} — threw the wrong error: ${String(e)}`);
  }
  throw new Error(msg);
}

const EXPLORATORY = {
  ranking_usage: 'POST_EXPOSURE_EXPLORATORY',
  policy_timing: 'REGISTERED_AFTER_EVIDENCE_EXPOSURE',
  preregistered_for_snapshot: false,
  ranking_policy_hash: '2aef967dc92786ce',
  ranking_policy_version: 'historical_ranking_policy_v1',
  display_banner: ['EXPLORATORY RANKING', 'Policy registered after evidence exposure',
                   'Not preregistered for this snapshot'],
};

const PROSPECTIVE = {
  ranking_usage: 'PROSPECTIVE_REGISTERED',
  policy_timing: 'REGISTERED_BEFORE_EVIDENCE_EXISTED',
  preregistered_for_snapshot: true,
  ranking_policy_hash: '2aef967dc92786ce',
  ranking_policy_version: 'historical_ranking_policy_v1',
  display_banner: ['PROSPECTIVE RANKING'],
};

function run(provenance: unknown): Record<string, unknown> {
  return {
    run_id: 'r1', input_state_hash: 'a', current_state_hash: 'a', freshness: 'FRESH',
    selectable_count: 31, ranked_count: 31, displayed_count: 0, display_policy: 'top_0',
    sampling_target: 'opportunity_bootstrap', null_family: 'OPPORTUNITY_LEVEL',
    integrity_status: 'VALID', data_provenance: 'SYNTHETIC_FIXTURE',
    evidence_origin: 'HISTORICAL_RESEARCH',
    instrument_validation_basis: 'SYNTHETIC_CAPABILITY_VALIDATED',
    application_maturity: 'FIRST_HISTORICAL_APPLICATION',
    result_role: 'ENGINE_QUALIFICATION_EVIDENCE', allowed_actions: ['inspect'],
    artifact_hash: 'h', ranking_provenance: provenance, rows: [],
  };
}

check('1 · exploratory state decodes as exploratory', () => {
  const p = decodeRankingProvenance(EXPLORATORY);
  assert(p.ranking_usage === 'POST_EXPOSURE_EXPLORATORY', p.ranking_usage);
  assert(p.preregistered_for_snapshot === false, 'preregistered leaked true');
});

check('2 · NEGATIVE FIXTURE · copy removed, semantics survive', () => {
  const stripped = { ...EXPLORATORY };
  delete (stripped as Record<string, unknown>).display_banner;
  const p = decodeRankingProvenance(stripped);
  assert(p.ranking_usage === 'POST_EXPOSURE_EXPLORATORY',
         'the standing left with the copy — display_banner was the carrier after all');
  assert(p.preregistered_for_snapshot === false, 'preregistered flipped when the copy vanished');
  assert(p.display_banner.length === 0, 'a banner was invented on the client');
});

check('3 · NEGATIVE FIXTURE · copy replaced with a reassuring lie', () => {
  const lying = { ...EXPLORATORY, display_banner: ['PREREGISTERED SELECTION', 'Fully validated'] };
  const p = decodeRankingProvenance(lying);
  assert(p.ranking_usage === 'POST_EXPOSURE_EXPLORATORY',
         'copy overrode state; the browser concluded a status from text');
  assert(p.preregistered_for_snapshot === false, 'copy promoted the ranking');
});

check('4 · the block missing entirely is a broken payload, not a quiet default', () => {
  throws(() => decodeRankingProvenance(undefined), 'a run with no ranking provenance decoded');
  throws(() => decodeRun(run(undefined)), 'a run decoded with no ranking provenance');
});

check('5 · a stringified boolean is refused', () => {
  throws(() => decodeRankingProvenance({ ...EXPLORATORY, preregistered_for_snapshot: 'false' }),
         '"false" is truthy in JS and was accepted as a boolean');
});

check('6 · an unrecognised state decodes as UNKNOWN and fails closed', () => {
  const p = decodeRankingProvenance({
    ...EXPLORATORY, ranking_usage: 'SEMI_PREREGISTERED_V3',
    policy_timing: 'SOMETHING_NEW', preregistered_for_snapshot: true,
  });
  assert(p.ranking_usage === 'UNKNOWN', p.ranking_usage);
  assert(p.policy_timing === 'UNKNOWN', p.policy_timing);
  assert(p.preregistered_for_snapshot === false,
         'an unrecognised standing kept its preregistered flag; fail-closed means the weaker one');
});

check('7 · internally inconsistent state is refused before it is rendered', () => {
  throws(() => decodeRankingProvenance({ ...EXPLORATORY, preregistered_for_snapshot: true }),
         'a payload claimed exploratory AND preregistered');
  throws(() => decodeRankingProvenance({ ...PROSPECTIVE, preregistered_for_snapshot: false }),
         'a payload claimed prospective AND not preregistered');
  throws(() => decodeRankingProvenance({
    ...EXPLORATORY, policy_timing: 'REGISTERED_BEFORE_EVIDENCE_EXISTED' }),
         'exploratory usage accepted before-evidence timing');
});

check('8 · prospective decodes as prospective', () => {
  const p = decodeRankingProvenance(PROSPECTIVE);
  assert(p.ranking_usage === 'PROSPECTIVE_REGISTERED', p.ranking_usage);
  assert(p.preregistered_for_snapshot === true, 'a genuine prospective ranking was downgraded');
});

check('9 · a whole run carries the provenance through', () => {
  const decoded = decodeRun({ run: run(EXPLORATORY) });
  assert(decoded.ranking_provenance.ranking_usage === 'POST_EXPOSURE_EXPLORATORY',
         'the run decoder dropped the ranking provenance');
});

console.log(`  ${ok} passed · ${bad} failed`);
process.exit(bad > 0 ? 1 : 0);
