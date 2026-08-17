# The band was never the price of breadth

Three explanations were offered for why M4 was a low-sensitivity search. Two of them were mine
and both were wrong. This is the third, and it is measured rather than argued.

## What was claimed, and what killed it

**"Exhaustive search over 4,636 claims costs sensitivity."**
Refuted by `band(k)`. The band is +4.7872 at k = 4,636 and +4.7872 at k = 2,500, and the
ceiling appears in random subsets exactly when one specific claim is drawn — p90 crosses at
k = 500 (10.8% inclusion) and the median at k = 2,500 (53.9%). Four boundary crossings, all
predicted by a single dominator. Breadth was never paying for it.

**"One pathological high-variance claim taxes everyone."**
Closer, but the diagnosis was wrong. `GR_G3+RSI_0_30` is the permutation maximum in **120 of
120** draws with permuted p95 = +4.787 and sd = **0.305**. That concentration is not variance.
An unbiased estimator under a valid null sits near zero; this one sits near +4.8 every single
time.

## What it actually is

```
                          block-constant   date-constant
GR_G3+RSI_0_30                    94.9%           48.8%
L1+H1_DR   (the q50 needle)       85.1%           18.7%
RSI_0_30                          78.8%            3.7%
MACRO_VIX_UP (known DAY_LEVEL)   100.0%          100.0%
```

The registered null permutes outcomes **inside (date × family)**. A claim that is constant
within a block puts that whole block in one arm, so no outcome ever crosses and the block's
contribution to θ survives the permutation untouched. The permuted statistic therefore keeps
the observed contrast, which is exactly why it lands at +4.787 with almost no spread.

This is N0's mechanism, arriving through a door nobody was watching:

    N0's DATE_LEVEL_RULE     "a feature takes one value within every trading DATE"
    the null's actual block  (date × family)

Those are not the same granularity, and my routing audit inherited the mismatch — it measured
constancy within `sig_date`. `GR_G3+RSI_0_30` is date-constant on only 48.8% of dates, so it
was never flagged, and it routed to OPPORTUNITY_LEVEL where the null cannot break it.

## It is not one claim. It is the whole universe, in degree.

```
10 highest-variance claims   block-constancy  median 94.4%   min 90.7%
300 random claims            block-constancy  median 85.2%   p90 93.3%

63.0% of claims are >80% block-constant
27.3% are >90%
```

23,541 blocks over 275,307 rows — median block size **3**, and 30% are singletons. At that
granularity almost any sparse claim is constant inside most of its blocks. The ten worst
offenders are simply the top decile of a defect the entire search space shares.

## What this does and does not mean

**Type-I error is fine.** Under H₀ there is no association to leak, so the null distribution
is correct and the band is conservative. ComboLab v2's measured FWER of 0.065 is not
contradicted by anything here.

**Power is destroyed.** Under H₁ the alternative leaks into its own null: a planted effect
survives the permutation in the same blocks where membership does, so it appears on both sides
of the comparison. That is why detection needed δ ≈ 4.2–4.9 pp, and it is why the +4.79 band
sits so close to the largest observed estimate — the band is partly built from the signal it is
supposed to be testing against.

**M4 does not move.** `0 / 4,636 · max θ̂ +4.411 · band +4.8302` stands. A claim whose null is
degenerate cannot be promoted any more than it could be rejected, and the universe was frozen.
`GR_G3+RSI_0_30` is not rescued by this — it is disqualified from being a candidate at all,
because its estimate of +4.411 is *below* what its own broken null produces from noise.

**It is not a patch.** v2 already wrote the sentence for the day-level case: giving the band a
matched permutation for the affected claims is a new multiplicity design, not a repair. A
coarser block (date only) would destroy the within-setup stratification the estimand is built
on; a different null (within-stratum sign flip, residual permutation) is a different registered
mechanism with its own qualification.

## The one number to carry forward

Not `0/4,636`, and not the sensitivity curve. This:

> **63% of the search space is more than 80% constant inside the permutation block.**

Any future instrument on this population must either measure constancy at the block
granularity and route on it, or use a null whose unit is coarse enough to have something to
exchange. The routing rule that exists today checks the wrong thing, and it checks it
convincingly enough that nothing looked wrong for the length of an entire study.
