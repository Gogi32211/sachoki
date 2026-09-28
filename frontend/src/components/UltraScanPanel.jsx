import { useState, useEffect, useMemo, useRef, useCallback } from 'react'
import { api } from '../api'
import { pwlAdd, pwlHas, pwlRemove } from './PersonalWatchlistPanel'
import { idbGet, idbSet, getCacheBackend } from '../turboCache'
import ScannerDataGrid from './ScannerDataGrid'
import CodeCandleChart from './CodeCandleChart'
import { gexSortVal, vrpSortVal, requestGexBulk, subscribeGex } from '../gexStore'
import { anatSortVal, anatomyReady, getAnatomy, requestAnatomy, subscribeAnatomy } from '../anatomyStore'
import { atrForecast } from '../atrForecast'
import { v4Score, v4FiredLabels } from '../lib/v4Score'
import { V4_WEIGHTS } from '../lib/v4Weights'
import { V4_EXTRA_GROUPS } from '../lib/v4ExtraGroups'

// ── Universes ─────────────────────────────────────────────────────────────────
const UNIVERSES = [
  { key: 'sp500',     label: 'S&P 500',      desc: '~500 large-caps',     cls: 'text-blue-300'   },
  { key: 'index',     label: '📊 INDEX',     desc: 'SPY·QQQ·DIA·IWM + 11 sector ETFs + SMH — sector reversals', cls: 'text-yellow-200' },
  { key: 'nasdaq',    label: 'NASDAQ',        desc: '~4K NASDAQ stocks',   cls: 'text-cyan-300'   },
  { key: 'russell2k', label: 'Russell 2K',   desc: 'All US small-caps',   cls: 'text-orange-300' },
  { key: 'all_us',    label: '🌐 All US',    desc: '~8K tickers (Massive)',cls: 'text-violet-300' },
  { key: 'split',     label: '✂️ SPLIT',     desc: 'Reverse splits: −7d / +5d window',
                                                                            cls: 'text-amber-300'  },
  { key: 'zone',      label: '⊏⊐ ZONE',      desc: 'Currently inside an active HV zone',
                                                                            cls: 'text-emerald-300' },
]

// ── Timeframes ────────────────────────────────────────────────────────────────
const TF_OPTS = ['1wk', '1d', '4h', '1h']

// ── Score bands (multi-select) ────────────────────────────────────────────────
const SCORE_BANDS = [
  { key: 'all',   label: 'All'    },
  { key: '0-20',  label: '0–20',  min: 0,  max: 20  },
  { key: '21-40', label: '21–40', min: 21, max: 40  },
  { key: '41-60', label: '41–60', min: 41, max: 60  },
  { key: '61-80', label: '61–80', min: 61, max: 80  },
  { key: '81+',   label: '81–100',min: 81, max: 1e9 },
]

// ── Direction filter ──────────────────────────────────────────────────────────
const DIR_OPTS = [
  { key: 'all',  label: 'ALL'  },
  { key: 'bull', label: 'BULL' },
  { key: 'bear', label: 'BEAR' },
]

// ── All signal filters — grouped by engine family ─────────────────────────────
// divider:true = thin separator between groups
// custom: fn(row)→bool for computed filters
// Exported (2026-09-23) so V4 (src/lib/v4Score.js) can reuse this EXACT catalog on the
// Superchart side too, instead of a second hand-maintained signal list that would drift.
export const SIG_GROUPS = [
  // ── ★ COMPOSITE setups (backtested edge, 8M-bar fwd-return analysis) ──────
  //   Vol-Bull   = bias_up + volume spike (V×5/V×10)            → ~66-69% fwd_10d win
  //   Struct-BO  = LVBO + bullish-engulf + RSI>65               → ~82% next-pivot-HH
  //   Absorb→EB  = prev-bar L34 absorption → EB engulf + RSI>65 → ~78% next-pivot-HH
  //   (RSI>65 gate: structural breakouts need momentum context — verified.)
  //   Blowoff↓   = volume climax (V×5/V×10) + weak close (c=O)  → SHORT fade,
  //               +10.8…+13.4 clip25-lift, 6/6 yrs (WHAT_ACTUALLY_WORKS.md).
  //               ⚠️ short tail risk: ~1/20 trade is a >50% squeeze — size + hard stop.
  { key: '_combo_volbull',  label: '⚡Vol-Bull',  cls: 'text-lime-300 font-bold',
    custom: r => !!(r.bias_up && (r.vol_spike_5x || r.vol_spike_10x)) },
  { key: '_combo_structbo', label: '❖Struct-BO', cls: 'text-cyan-300 font-bold',
    custom: r => !!(r.pb_lvbo && r.eb_bull && r.rsi > 65) },
  { key: '_combo_absorbeb', label: '❖Absorb→EB', cls: 'text-teal-300 font-bold',
    custom: r => !!(r.seq_l34_eb && r.rsi > 65) },
  { key: '_combo_blowoff',  label: '⚡Blowoff↓',  cls: 'text-red-400 font-bold',
    custom: r => !!((r.vol_spike_5x || r.vol_spike_10x) && (r.tz_wlnbb_full_suffix || '').includes('O')) },
  { divider: true, label: '★ combo (backtested)' },
  // ── VABS ──────────────────────────────────────────────────────────────
  { key: 'best_sig',   label: 'BEST★',  cls: 'text-lime-300'    },
  { key: 'strong_sig', label: 'STRONG', cls: 'text-emerald-300' },
  { key: 'vol_spike_20x', label: 'V×20', cls: 'text-red-300 font-bold' },
  { key: 'vol_spike_10x', label: 'V×10', cls: 'text-orange-300'  },
  { key: 'vol_spike_5x',  label: 'V×5',  cls: 'text-yellow-300'  },
  { key: 'vbo_up',     label: 'VBO↑',  cls: 'text-green-300'   },
  { key: 'abs_sig',    label: 'ABS',    cls: 'text-teal-300'    },
  { key: 'climb_sig',  label: 'CLB',    cls: 'text-cyan-300'    },
  { key: 'load_sig',   label: 'LD',     cls: 'text-blue-300'    },
  { divider: true },
  // ── Wyckoff (VABS legacy) ─────────────────────────────────────────────
  { key: 'ns',         label: 'NS',     cls: 'text-teal-300'    },
  { key: 'sq',         label: 'SQ',     cls: 'text-cyan-400'    },
  { key: 'sc',         label: 'SC',     cls: 'text-orange-300'  },
  { key: 'nd',         label: 'ND',     cls: 'text-pink-300'    },
  { divider: true },
  // ── Combo ─────────────────────────────────────────────────────────────
  { key: 'buy_2809',   label: 'BUY',    cls: 'text-lime-400'    },
  { key: 'rocket',     label: '🚀',     cls: 'text-red-300'     },
  { key: 'sig3g',      label: '3G',     cls: 'text-cyan-300'    },
  { key: 'rtv',        label: 'RTV',    cls: 'text-blue-300'    },
  { key: 'hilo_buy',   label: 'HILO↑', cls: 'text-green-300'   },
  { key: 'atr_brk',    label: 'ATR↑',  cls: 'text-emerald-300' },
  { key: 'bb_brk',     label: 'BB↑',   cls: 'text-teal-300'    },
  { key: 'va',         label: 'VA',    cls: 'text-lime-300'    },
  { key: 'bias_up',    label: '↑BIAS', cls: 'text-green-400'   },
  { key: 'um_2809',    label: 'UM',     cls: 'text-teal-300'    },
  { key: 'svs_2809',   label: 'SVS',    cls: 'text-orange-300'  },
  { key: 'conso_2809', label: 'CON',    cls: 'text-yellow-300'  },
  // PT5 · Preview T5 — HISTORICAL research preview over the frozen T5 research (not a new
  // trading rule, not forward validation, never injected into EDGE ranking). Rows after the
  // 2026-08-20 validation cutoff carry no PT5 by default.
  { key: 'pt5',        label: 'PT5',    cls: 'text-emerald-300' },
  { key: 'pt5_h1_any', label: 'PT5·1H', cls: 'text-violet-300'  },
  { key: 'pt5_m15_any',label: 'PT5·15', cls: 'text-cyan-300'    },
  { key: 'pt5_strong', label: 'PT5+',   cls: 'text-emerald-400' },
  { key: 'pt5_xr_volw_va', label: 'XR:VOL_W↔VA', cls: 'text-amber-300' },
  // MT5+ — NOT a PT5 variant. PT5·1H/PT5·15 mean frozen token-family membership; MT5+
  // means the SAME T5 literally fired on 1H and 15m that session. Separate builder
  // (mt5_build.py), separate parquet, separate cache. Never injected into EDGE ranking.
  //
  // LABEL CORRECTED 2026-09-02 after a horizon sweep. "Confirmation" implies something
  // happens AT the bar; it does not. The c2-minus-c0 lift per bar is flat — 0.030 / 0.040 /
  // 0.040 / 0.035 / 0.030 at maxh 3 / 5 / 10 / 20 / 60 — i.e. a constant drift-rate
  // difference, not an event. At 3 bars the whole lift is +0.09. The state is real
  // (60-bar median +2.13, win 55.7%, PF 1.48, MAE better than baseline at every horizon)
  // but it is a SLOW SELECTION criterion, not entry timing.
  { key: 'mt5_strong', label: 'MT5+',  cls: 'text-rose-300',
    hint: 'MT5+ — a 1D T5 whose same T5 also fired on 1H and 15m that session (close ≥ $21).\n\n'
        + 'NOT an entry-timing signal. A horizon sweep shows the lift is a constant drift '
        + 'rate (~+0.035%/bar, flat from 3 to 60 bars), not an event at the bar: at 3 bars '
        + 'the whole edge is +0.09.\n\nRead it as a slow selection criterion — over 60 bars '
        + 'median +2.13, win 55.7%, PF 1.48, and lower MAE than baseline at every horizon.\n\n'
        + 'SEARCH-EXPOSED CANDIDATE: not forward validated, not a book edge. Unrelated to PT5, '
        + 'which is frozen token-family membership.' },
  // PT9 — full ladder: T9's 15m phase is now built (260 cluster representatives), so ·15
  // and + are real states here, not UNAVAILABLE.
  { key: 'pt9',        label: 'PT9',    cls: 'text-emerald-300' },
  { key: 'pt9_h1_any', label: 'PT9·1H', cls: 'text-violet-300'  },
  { key: 'pt9_m15_any',label: 'PT9·15', cls: 'text-cyan-300'    },
  { key: 'pt9_strong', label: 'PT9+',   cls: 'text-emerald-400' },
  // PT3 / PT1 — BASE and 1H only. Their 15m capability has not been run, so a ·15 chip
  // would filter on UNAVAILABLE rather than false, and UNAVAILABLE is not "no match".
  { key: 'pt3',        label: 'PT3',    cls: 'text-emerald-300' },
  { key: 'pt3_h1_any', label: 'PT3·1H', cls: 'text-violet-300'  },
  { key: 'pt1',        label: 'PT1',    cls: 'text-emerald-300' },
  { key: 'pt1_h1_any', label: 'PT1·1H', cls: 'text-violet-300'  },
  // ALL — the UNION across families, never a strength score. Two families marking the same
  // bar are two separate research previews, not one stronger signal.
  { key: 'pt_any',        label: 'PT·ALL',   cls: 'text-teal-300'    },
  { key: 'pt_any_h1',     label: 'PT·ALL1H', cls: 'text-violet-300'  },
  { key: 'pt_any_strong', label: 'PT·ALL+',  cls: 'text-emerald-400' },
  // ── L-BAL — the TradingView "260906_LTF_L_COUNT" label, ported (lbal_build.py → data/
  //   lbal_signals.parquet, joined by (ticker, scan_date) in _enrich_lbal). Two lines per 1D
  //   session from its 26 15m bars: UDN = effort (L34+L3 vs L46), UDN+ = labelled 15m bars by
  //   their own candle. The five chips are the AGREEMENT states between the lines and the
  //   daily candle. DESCRIPTIVE ONLY — INTRADAY_EFFORT_BALANCE_V1 closed 16/16 NULL (2026-09-06):
  //   the effort direction adds nothing measurable to the candle. Never a ranking input.
  // ── ★ L-BAL — BOTH counting modes as separate chips (user, 2026-09-10). Measured over
  //   3,652,833 sessions: the V and all counts differ on the marks in 55.0 % of them and fully
  //   invert (U vs D) in 13.0 %. A single toggled set of chips therefore hid a contradictory
  //   reading on most bars; these are two different questions and each gets its own chip.
  { divider: true, label: '★ L-BAL (15m L-label balance vs candle · V and all counts, both shown · descriptive)' },
  { key: 'lbal_star_v', label: '★V', cls: 'text-yellow-300', custom: (r) => !!r.lbal_star,
    hint: '★ divergence — the UDN effort line (15m L34+L3 vs L46) opposes the daily candle: U on a red day, or D on a green day. — COUNTS ONLY 15m bars with volume > SMA20 — the TradingView setting (L34V / L46V labels).'
        + '\n\nThe V and all counts DISAGREE on 55.6 % of sessions and invert outright on 13.0 % (3.65 M sessions measured), so both are shown as separate chips instead of one behind a switch — filtering on one of them is a different question from filtering on the other.\n\nDescriptive only: INTRADAY_EFFORT_BALANCE_V1 tested this direction and closed 16/16 NULL.' },
  { key: 'lbal_conflict_v', label: '★★V', cls: 'text-orange-300', custom: (r) => !!r.lbal_conflict,
    hint: '★★ conflict — the effort line and the candle line oppose each other: D & U+ or U & D+. UDN+ counts the labelled 15m bars by their own candle. — COUNTS ONLY 15m bars with volume > SMA20 — the TradingView setting (L34V / L46V labels).'
        + '\n\nThe V and all counts DISAGREE on 55.6 % of sessions and invert outright on 13.0 % (3.65 M sessions measured), so both are shown as separate chips instead of one behind a switch — filtering on one of them is a different question from filtering on the other.\n\nDescriptive only: INTRADAY_EFFORT_BALANCE_V1 tested this direction and closed 16/16 NULL.' },
  { key: 'lbal_half_up_v', label: '★★★V', cls: 'text-sky-300', custom: (r) => !!r.lbal_half_up,
    hint: '★★★ half-up — one line U, the other neutral: U & N+ or N & U+. — COUNTS ONLY 15m bars with volume > SMA20 — the TradingView setting (L34V / L46V labels).'
        + '\n\nThe V and all counts DISAGREE on 55.6 % of sessions and invert outright on 13.0 % (3.65 M sessions measured), so both are shown as separate chips instead of one behind a switch — filtering on one of them is a different question from filtering on the other.\n\nDescriptive only: INTRADAY_EFFORT_BALANCE_V1 tested this direction and closed 16/16 NULL.' },
  { key: 'lbal_half_dn_v', label: '○○○V', cls: 'text-sky-300', custom: (r) => !!r.lbal_half_dn,
    hint: '○○○ half-down — one line D, the other neutral: D & N+ or N & D+. — COUNTS ONLY 15m bars with volume > SMA20 — the TradingView setting (L34V / L46V labels).'
        + '\n\nThe V and all counts DISAGREE on 55.6 % of sessions and invert outright on 13.0 % (3.65 M sessions measured), so both are shown as separate chips instead of one behind a switch — filtering on one of them is a different question from filtering on the other.\n\nDescriptive only: INTRADAY_EFFORT_BALANCE_V1 tested this direction and closed 16/16 NULL.' },
  { key: 'lbal_nn_v', label: 'XXXV', cls: 'text-gray-300', custom: (r) => !!r.lbal_nn,
    hint: 'XXX double-neutral — both lines N (equal counts on both sides). — COUNTS ONLY 15m bars with volume > SMA20 — the TradingView setting (L34V / L46V labels).'
        + '\n\nThe V and all counts DISAGREE on 55.6 % of sessions and invert outright on 13.0 % (3.65 M sessions measured), so both are shown as separate chips instead of one behind a switch — filtering on one of them is a different question from filtering on the other.\n\nDescriptive only: INTRADAY_EFFORT_BALANCE_V1 tested this direction and closed 16/16 NULL.' },
  { key: 'lbal_star_a', label: '★a', cls: 'text-yellow-300', custom: (r) => !!r.lbal_all_star,
    hint: '★ divergence — the UDN effort line (15m L34+L3 vs L46) opposes the daily candle: U on a red day, or D on a green day. — COUNTS EVERY labelled 15m bar (the Pine default).'
        + '\n\nThe V and all counts DISAGREE on 55.6 % of sessions and invert outright on 13.0 % (3.65 M sessions measured), so both are shown as separate chips instead of one behind a switch — filtering on one of them is a different question from filtering on the other.\n\nDescriptive only: INTRADAY_EFFORT_BALANCE_V1 tested this direction and closed 16/16 NULL.' },
  { key: 'lbal_conflict_a', label: '★★a', cls: 'text-orange-300', custom: (r) => !!r.lbal_all_conflict,
    hint: '★★ conflict — the effort line and the candle line oppose each other: D & U+ or U & D+. UDN+ counts the labelled 15m bars by their own candle. — COUNTS EVERY labelled 15m bar (the Pine default).'
        + '\n\nThe V and all counts DISAGREE on 55.6 % of sessions and invert outright on 13.0 % (3.65 M sessions measured), so both are shown as separate chips instead of one behind a switch — filtering on one of them is a different question from filtering on the other.\n\nDescriptive only: INTRADAY_EFFORT_BALANCE_V1 tested this direction and closed 16/16 NULL.' },
  { key: 'lbal_half_up_a', label: '★★★a', cls: 'text-sky-300', custom: (r) => !!r.lbal_all_half_up,
    hint: '★★★ half-up — one line U, the other neutral: U & N+ or N & U+. — COUNTS EVERY labelled 15m bar (the Pine default).'
        + '\n\nThe V and all counts DISAGREE on 55.6 % of sessions and invert outright on 13.0 % (3.65 M sessions measured), so both are shown as separate chips instead of one behind a switch — filtering on one of them is a different question from filtering on the other.\n\nDescriptive only: INTRADAY_EFFORT_BALANCE_V1 tested this direction and closed 16/16 NULL.' },
  { key: 'lbal_half_dn_a', label: '○○○a', cls: 'text-sky-300', custom: (r) => !!r.lbal_all_half_dn,
    hint: '○○○ half-down — one line D, the other neutral: D & N+ or N & D+. — COUNTS EVERY labelled 15m bar (the Pine default).'
        + '\n\nThe V and all counts DISAGREE on 55.6 % of sessions and invert outright on 13.0 % (3.65 M sessions measured), so both are shown as separate chips instead of one behind a switch — filtering on one of them is a different question from filtering on the other.\n\nDescriptive only: INTRADAY_EFFORT_BALANCE_V1 tested this direction and closed 16/16 NULL.' },
  { key: 'lbal_nn_a', label: 'XXXa', cls: 'text-gray-300', custom: (r) => !!r.lbal_all_nn,
    hint: 'XXX double-neutral — both lines N (equal counts on both sides). — COUNTS EVERY labelled 15m bar (the Pine default).'
        + '\n\nThe V and all counts DISAGREE on 55.6 % of sessions and invert outright on 13.0 % (3.65 M sessions measured), so both are shown as separate chips instead of one behind a switch — filtering on one of them is a different question from filtering on the other.\n\nDescriptive only: INTRADAY_EFFORT_BALANCE_V1 tested this direction and closed 16/16 NULL.' },
  // ── L-VX — the TradingView "260906_WLNBB_L34_L46_VX_CHART" script, ported (same builder,
  //   data/lvx_signals.parquet, _enrich_lvx). The DAILY L34 / L46 graded: V = daily volume > SMA20 ·
  //   VL = a lower TF (15m or 60m) has a plain L34/L46 bar inside the day · VH = a lower TF has an
  //   L34V/L46V bar inside · VX = both 15m AND 60m echo. Each chip is a THRESHOLD ("at least this
  //   tier"), like the script's "Minimum tier to show". DESCRIPTIVE ONLY — never a ranking input.
  { divider: true, label: 'L-VX (daily L34/L46 · V vol>SMA20 · VL/VH one lower TF · VX both · descriptive)' },
  { key: 'lvx_l34_v',  label: 'L34V',  cls: 'text-green-300',
    hint: 'L34V — daily L34 with volume > SMA20(volume). Chip = tier ≥ V (includes VL / VH / VX).' },
  { key: 'lvx_l34_vl', label: 'L34VL', cls: 'text-green-300',
    hint: 'L34VL — L34V and at least one lower TF (15m or 60m) has a plain L34 bar inside the day. Chip = tier ≥ VL.' },
  { key: 'lvx_l34_vh', label: 'L34VH', cls: 'text-green-200',
    hint: 'L34VH — L34V and at least one lower TF has an L34V bar (its own volume > its SMA20) inside the day. Chip = tier ≥ VH.' },
  { key: 'lvx_l34_vx', label: 'L34VX', cls: 'text-lime-300',
    hint: 'L34VX — L34V confirmed on BOTH 15m and 60m (script default: any confirmation on both TFs).' },
  { key: 'lvx_l46_v',  label: 'L46V',  cls: 'text-red-300',
    hint: 'L46V — daily L46 with volume > SMA20(volume). Chip = tier ≥ V.' },
  { key: 'lvx_l46_vl', label: 'L46VL', cls: 'text-red-300',
    hint: 'L46VL — L46V and at least one lower TF has a plain L46 bar inside the day. Chip = tier ≥ VL.' },
  { key: 'lvx_l46_vh', label: 'L46VH', cls: 'text-orange-300',
    hint: 'L46VH — L46V and at least one lower TF has an L46V bar inside the day. Chip = tier ≥ VH.' },
  { key: 'lvx_l46_vx', label: 'L46VX', cls: 'text-orange-200',
    hint: 'L46VX — L46V confirmed on BOTH 15m and 60m.' },
  // ── OVD daily map — the TradingView "260904_OVD_4_VOLUME_LOGICS_DAILY_MAP" script ported for
  //   display (ovd_map_build.py → data/ovdmap_signals.parquet, _enrich_ovdmap). Tokens keep the
  //   user's chart naming (OB/RC/CD/HO) so they never collide with the WLNBB L1..L6 labels.
  //   Same-slot 15m RVOL = value / median of the last 20 FULL regular sessions. DESCRIPTIVE ONLY —
  //   the sealed OVD research family closed 0 BUILD / 0 VETO (2026-09-04). Never a ranking input.
  { divider: true, label: 'OVD daily map (opening / closing 15m volume logics · descriptive)' },
  { key: 'ovdmap_ob30', label: 'OB·30', cls: 'text-cyan-300',
    hint: 'OB·30 — Logic 1 opening build: same-slot RVOL of BOTH opening 30m windows (09:30-10:00, 10:00-10:30) rose three sessions in a row.' },
  { key: 'ovdmap_ob60', label: 'OB·60', cls: 'text-blue-300',
    hint: 'OB·60 — Logic 1 opening build: the opening-60m same-slot RVOL rose three sessions in a row.' },
  { key: 'ovdmap_rc30', label: 'RC·30', cls: 'text-lime-300',
    hint: 'RC·30 — Logic 2 prior-HV reclaim: both opening 30m windows ≥ the same windows of the nearest HV-decline event 6..30 sessions back (opening-60m RVOL ≥ 2 on a day whose prior close was below the close 6 sessions earlier).' },
  { key: 'ovdmap_rc60', label: 'RC·60', cls: 'text-green-300',
    hint: 'RC·60 — Logic 2 prior-HV reclaim: opening 60m ≥ the nearest HV-decline event\'s opening 60m.' },
  { key: 'ovdmap_cd30', label: 'CD·30', cls: 'text-orange-300',
    hint: 'CD·30 — Logic 3 close dominance: close < close 5 sessions ago, last-30m RVOL > 1 and last 30m volume ≥ first 30m volume.' },
  { key: 'ovdmap_cd60', label: 'CD·60', cls: 'text-red-300',
    hint: 'CD·60 — Logic 3 close dominance: close < close 5 sessions ago, last-60m RVOL > 1 and last 60m volume ≥ first 60m volume.' },
  { key: 'ovdmap_ho30', label: 'HO·30', cls: 'text-fuchsia-300',
    hint: 'HO·30 — Logic 4 handoff: yesterday (in a 5-day decline) closed with last-30m RVOL > 1, today\'s first 30m has RVOL > 1 and is 0.80..1.25× yesterday\'s last 30m.' },
  { key: 'ovdmap_ho60', label: 'HO·60', cls: 'text-violet-300',
    hint: 'HO·60 — Logic 4 handoff: the 60m analogue (yesterday\'s last 60m → today\'s first 60m, 0.80..1.25×).' },
  { key: 'ovdmap_nm',   label: 'NM?',   cls: 'text-yellow-300',
    hint: 'NM? — near-miss proxy: reclaim60 in [0.50, 0.75) of the nearest HV event and all four opening 15m slots have RVOL > 1. The script itself calls this a proxy; the sealed Family-B eligibility is not reproduced.' },
  // ── VOL7 — the TradingView "260829 • 7-Level Volume MR + Sigma Consensus/Divergence + B/VB + Shift"
  //   script ported for display (vol7_build.py → data/vol7_signals.parquet, _enrich_vol7). Daily bars
  //   only. MR level = volume / 20-day MEDIAN (primary); σ level = BB(volume,20,1) comparison. M5/M6
  //   are the script's B/VB and deliberately NOT named B/VB: in this app B/VB is the WLNBB bucket the
  //   book edges use (VB = vol ≥ 2·mid+σ). DESCRIPTIVE ONLY; SHIFT is an unstudied setup marker.
  { divider: true, label: 'VOL7 (7-level volume regime · median-ratio vs sigma · jumps · VB2 · SHIFT · descriptive)' },
  { key: 'vol7_m0',  label: 'M0',  cls: 'text-gray-400',
    hint: 'M0 — extreme low volume: < 0.40× the 20-day median (the script\'s grey dot).' },
  { key: 'vol7_m5',  label: 'M5',  cls: 'text-orange-300',
    hint: 'M5 — 2.5..5× the 20-day median (the script calls it B). NOT the app\'s WLNBB B bucket. Inside the book\'s 2-3× sweet zone on absorption bars.' },
  { key: 'vol7_m6',  label: 'M6',  cls: 'text-red-300',
    hint: 'M6 — ≥ 5× the 20-day median (the script calls it VB). NOT the app\'s WLNBB VB bucket; 93% of M6 bars are WLNBB VB, but only 21% of WLNBB VB bars are M6.' },
  { key: 'vol7_up2', label: '▲+2', cls: 'text-lime-300',
    hint: '▲+2 — the MR volume level skipped UP exactly two levels vs yesterday (small green circle in the script).' },
  { key: 'vol7_dn2', label: '▼−2', cls: 'text-red-300',
    hint: '▼−2 — the MR volume level skipped DOWN exactly two levels vs yesterday.' },
  { key: 'vol7_up3', label: '◆+3', cls: 'text-lime-200',
    hint: '◆+3 — BIG volume jump UP: three or more MR levels vs yesterday (green diamond in the script).' },
  { key: 'vol7_dn3', label: '◆−3', cls: 'text-red-200',
    hint: '◆−3 — BIG volume jump DOWN: three or more MR levels vs yesterday.' },
  { key: 'vol7_sigma_plus', label: 'Σ+', cls: 'text-orange-200',
    hint: 'Σ+ — the sigma model rates the bar ≥ 2 levels above the median-ratio model: a quiet, uniform 20-day window makes σ tiny, so σ-based labels (the WLNBB bucket and its L-labels included) over-rate this bar. ~8% of bars.' },
  { key: 'vol7_mr_plus', label: 'MR+', cls: 'text-cyan-200',
    hint: 'MR+ — the median-ratio model rates the bar ≥ 2 levels above the sigma model (a spiky window inflates σ). Rare, ~0.2% of bars.' },
  { key: 'vol7_vb2', label: 'VB2', cls: 'text-purple-300',
    hint: 'VB2 — a NEW extreme-volume (M6) event at least 6 bars after the previous VB bar; VBs within 2 bars count as one event. The script\'s purple diamond.' },
  { key: 'vol7_shift_up', label: 'SHIFT↑', cls: 'text-lime-300',
    hint: 'SHIFT↑ — bullish regime shift: VB2 after a ≥ 3% decline over the prior 10 sessions, then within 5 bars a GREEN close above the VB bar\'s high (range expanded by VBs within 2 bars). Unstudied setup marker — descriptive only.' },
  { key: 'vol7_shift_dn', label: 'SHIFT↓', cls: 'text-red-300',
    hint: 'SHIFT↓ — bearish regime shift: VB2 after a ≥ 3% rise over the prior 10 sessions, then within 5 bars a RED close below the VB bar\'s low. Unstudied setup marker — descriptive only.' },
  // ── SHAPE × CONTEXT — the TradingView "260910_SHAPE_CTX" script ported for display
  //   (shape_ctx_build.py → data/shapectx_signals.parquet, _enrich_shapectx). Daily bars only.
  //   DESCRIPTIVE ONLY, and unusually well measured: four sealed families, k = 21, 0 BUILD.
  //   ⚠ 🎯 and 🔁 are NOT strength marks — SHAPE_CLUSTER_V1 measured the ladder MONOTONE and
  //   DOWNWARD (NO_CLUSTER −0.13 was the BEST cell, 🎯🔁 −0.68 the WORST, 0/4 years). The one
  //   chip with two-window evidence is ⛔LST↑ and it is a VETO.
  { divider: true, label: 'SHAPE × CONTEXT (body-nest shapes · effort · cluster · descriptive — clustering measured WORSE)' },
  { key: 'shape_mth', label: 'MTH', cls: 'text-indigo-200', custom: (r) => r.shape_code === 'MTH',
    hint: 'MOTHER — bars t−2 and t−1 nested inside bar t−3\'s body, and this bar closes above it. MOTHER_V1 (sealed, k=5): 4 NULL + 1 VETO; RSI<35 was the WORST cell (−3.54 / −2.04, 0/4 years), the opposite of the hypothesis.' },
  { key: 'shape_cl4', label: 'CL4', cls: 'text-indigo-200', custom: (r) => r.shape_code === 'CL4',
    hint: 'COIL4 — bar t−2 inside bar t−1, bar t−1 touching bar t−3\'s body, and this bar closes above t−3\'s body top.' },
  { key: 'shape_mid', label: 'MID', cls: 'text-slate-300', custom: (r) => r.shape_code === 'MID',
    hint: 'MIDNEST — bars t−2 and t sit inside bar t−1\'s body. Symmetric: the Pine gives it no direction.' },
  { key: 'shape_exp', label: 'EXP', cls: 'text-slate-300', custom: (r) => r.shape_code === 'EXP',
    hint: 'EXPAND — t−2 ⊂ t−1 ⊂ t. A swallow shape, so it carries the ↑↓ arrow. EXP ⊂ LST ⊂ WRP.' },
  { key: 'shape_con', label: 'CON', cls: 'text-slate-300', custom: (r) => r.shape_code === 'CON',
    hint: 'CONTRACT — t ⊂ t−1 ⊂ t−2. Symmetric, no direction.' },
  { key: 'shape_lst', label: 'LST', cls: 'text-slate-300', custom: (r) => r.shape_code === 'LST',
    hint: 'LASTNEST — this bar\'s body swallows BOTH prior bodies. Split it by direction: LST↓ is NULL, LST↑ is the one measured VETO.' },
  { key: 'shape_wrp', label: 'WRP', cls: 'text-slate-300', custom: (r) => r.shape_code === 'WRP',
    hint: 'WRAP — bar t−2 inside this bar, minus the tighter LASTNEST / EXPAND. SWALLOW_DIR_V1: stable UP > DN in both windows (the opposite of the book\'s red-beats-green law), but both halves are negative.' },
  { key: 'shape_lstup_veto', label: '⛔LST↑', cls: 'text-red-300',
    hint: '⛔ VETO — a GREEN bar swallowing both prior bodies. The ONLY shape cell with two-window evidence: −0.63 MINE / −0.47 VERIFY, 0 of 4 positive years, worst year −3.41, DSR_neg 0.998 over 29,911 trades, and the only shape cell that does not flip sign between windows. Do not buy it.' },
  { key: 'shape_veto', label: '⛔KNF', cls: 'text-red-300',
    hint: '⛔ KNIFE — MOTHER / COIL4 fired while rsi < 35. Measured −3.54 MINE / −2.04 VERIFY, 0/4 years: a breakout close while still deeply oversold is a dead cat.' },
  { key: 'shape_absorb', label: '💨abs', cls: 'text-emerald-300',
    hint: '💨 absorbed — volume ≥ 1.5× avg20 on a range ≤ 1 ATR. EFFORT is the only axis the script scores.' },
  { key: 'shape_dry', label: '⛔dry', cls: 'text-red-200',
    hint: '⛔ dry — volume < 0.7× avg20 on the shape bar.' },
  { key: 'shape_sweet', label: '✅swt', cls: 'text-emerald-200',
    hint: '✅ the 2-3× inverted-U volume band — where absorption bars measured best (project_volume_magnitude).' },
  { key: 'shape_floor', label: '📍flr', cls: 'text-cyan-300',
    hint: '📍 FLOOR — close in the bottom 35% of the 20-bar range. CONTEXT ONLY: corr(pos20, rsi) = +0.908, so scoring it would score rsi twice.' },
  { key: 'shape_key', label: '🧱key', cls: 'text-cyan-300',
    hint: '🧱 KEY — at least 2 bars in the last 40 whose low sits within 0.5 ATR of the 20-bar low.' },
  { key: 'shape_rs', label: '🏆rs', cls: 'text-amber-300',
    hint: '🏆 RS intact — close/SPY above its EMA200. Cross-sectional, so it is a screener filter and never a per-chart score.' },
  { key: 'shape_by_fam', label: '🎯div', cls: 'text-gray-400',
    hint: '🎯 DIVERSITY — ≥3 of the 4 shape families in the last 10 bars. ⚠ NOT a strength mark: SHAPE_CLUSTER_V1 measured NO_CLUSTER as the BEST cell (−0.13) and 🎯🔁 as the WORST (−0.68, 0/4 years), decaying monotonically with more clustering.' },
  { key: 'shape_by_den', label: '🔁den', cls: 'text-gray-400',
    hint: '🔁 DENSITY — ≥4 of the last 10 bars carried a shape. Measured no different from 🎯 (−0.27 vs −0.19): the two marks read differently on a chart but did not behave as different predictive axes.' },
  // ── PRICE × VOLUME — the TradingView "260921_PV_MULTI" script ported for display
  //   (pv_multi_build.py → data/pv_multi_signals.parquet, _enrich_pv_multi). Daily bars only.
  //   TWO GROUPS because the script offers two price sources and they DISAGREE: over 2.15M firing
  //   sessions `close` and `ohlc4` pick the same shape only 52.7% of the time.
  //   DESCRIPTIVE ONLY. PV_MULTI_V1, sealed 2026-09-21, k = 9 → 0 BUILD / 4 VETO_CANDIDATE /
  //   5 NULL, with EVERY cell negative in MINE. The four VETOes (REV RE2 UPP RUP) are RECORDED,
  //   NOT APPLIED — they cover ~13.6% of bars, and applying them is an interaction question that
  //   needs its own family and its own k. Nothing here is green, because nothing here measured
  //   positive. Never a ranking input.
  { divider: true, label: 'PRICE × VOLUME · price = OHLC4 (Pine default) — descriptive; 0 BUILD / 4 VETO / 5 NULL' },
  { key: 'pv_o_div', label: 'DIV', cls: 'text-slate-300',
    hint: 'DIV (ohlc4) — price fell 3 bars while volume rose 3 bars. Fires on 5.95% of liquid bars. Sealed verdict: NULL — no effect that replicated.' },
  { key: 'pv_o_upp', label: 'UPP', cls: 'text-rose-300',
    hint: 'UPP (ohlc4) — price rose 3 bars while volume rose 3 bars. Fires on 5.97% of liquid bars. Sealed verdict: VETO_CANDIDATE — −0.618 MINE / −0.573 VERIFY, negative in all 4 MINE years, dsr_neg 1.000.' },
  { key: 'pv_o_upr', label: 'UPR', cls: 'text-slate-300',
    hint: 'UPR (ohlc4) — volume rose 3 bars and price is above its level 2 bars ago (clean UPP excluded). Fires on 3.58% of liquid bars. Sealed verdict: NULL — no effect that replicated.' },
  { key: 'pv_o_rev', label: 'REV', cls: 'text-rose-300',
    hint: 'REV (ohlc4) — price and volume both fell 3 bars, then both turned up. Fires on 1.64% of liquid bars. Sealed verdict: VETO_CANDIDATE — −0.909 MINE / −0.686 VERIFY, negative in all 4 MINE years, dsr_neg 1.000.' },
  { key: 'pv_o_rup', label: 'RUP', cls: 'text-rose-300',
    hint: 'RUP (ohlc4) — volume contracted 3 bars with day-3 price above day-1, then price and volume turned up. Fires on 3.83% of liquid bars. Sealed verdict: VETO_CANDIDATE — −0.350 MINE / −0.316 VERIFY, negative in all 4 MINE years, dsr_neg 0.996.' },
  { key: 'pv_o_vup', label: 'VUP', cls: 'text-slate-300',
    hint: 'VUP (ohlc4) — volume fell 3 bars while price stayed above its level 2 bars ago. Fires on 10.55% of liquid bars. Sealed verdict: NULL — no effect that replicated.' },
  { key: 'pv_o_turn', label: 'TURN', cls: 'text-slate-300',
    hint: 'TURN (ohlc4) — price declined then recovered 2 bars, with the script\'s volume turn structure. Fires on 0.74% of liquid bars. Sealed verdict: NULL — no effect that replicated.' },
  { key: 'pv_o_up4', label: 'UP4', cls: 'text-slate-300',
    hint: 'UP4 (ohlc4) — price rose 4 bars while volume expanded, rested, then re-expanded above the prior peak. Fires on 1.42% of liquid bars. Sealed verdict: NULL — no effect that replicated.' },
  { key: 'pv_o_re2', label: 'RE2', cls: 'text-rose-300',
    hint: 'RE2 (ohlc4) — price down-down-up while volume went up-down-up. Fires on 2.19% of liquid bars. Sealed verdict: VETO_CANDIDATE — −0.689 MINE / −0.320 VERIFY, dsr_neg 1.000.' },
  { key: 'pv_veto_o', label: '⛔veto', cls: 'text-red-300',
    hint: '⛔ any of the four VETO_CANDIDATE shapes (REV RE2 UPP RUP) on the ohlc4 source. Recorded, NOT applied — the app does not suppress anything on this.' },
  { key: 'pv_agree', label: '=both', cls: 'text-gray-400',
    hint: 'Both price sources picked the SAME shape on this bar. They agree on only 52.7% of firing sessions, so this chip selects the unambiguous half.' },
  { divider: true, label: 'PRICE × VOLUME · price = CLOSE' },
  { key: 'pv_c_div', label: 'DIV', cls: 'text-slate-300',
    hint: 'DIV (close) — price fell 3 bars while volume rose 3 bars. Fires on 5.95% of liquid bars. Sealed verdict: NULL — no effect that replicated.' },
  { key: 'pv_c_upp', label: 'UPP', cls: 'text-rose-300',
    hint: 'UPP (close) — price rose 3 bars while volume rose 3 bars. Fires on 5.97% of liquid bars. Sealed verdict: VETO_CANDIDATE — −0.618 MINE / −0.573 VERIFY, negative in all 4 MINE years, dsr_neg 1.000.' },
  { key: 'pv_c_upr', label: 'UPR', cls: 'text-slate-300',
    hint: 'UPR (close) — volume rose 3 bars and price is above its level 2 bars ago (clean UPP excluded). Fires on 3.58% of liquid bars. Sealed verdict: NULL — no effect that replicated.' },
  { key: 'pv_c_rev', label: 'REV', cls: 'text-rose-300',
    hint: 'REV (close) — price and volume both fell 3 bars, then both turned up. Fires on 1.64% of liquid bars. Sealed verdict: VETO_CANDIDATE — −0.909 MINE / −0.686 VERIFY, negative in all 4 MINE years, dsr_neg 1.000.' },
  { key: 'pv_c_rup', label: 'RUP', cls: 'text-rose-300',
    hint: 'RUP (close) — volume contracted 3 bars with day-3 price above day-1, then price and volume turned up. Fires on 3.83% of liquid bars. Sealed verdict: VETO_CANDIDATE — −0.350 MINE / −0.316 VERIFY, negative in all 4 MINE years, dsr_neg 0.996.' },
  { key: 'pv_c_vup', label: 'VUP', cls: 'text-slate-300',
    hint: 'VUP (close) — volume fell 3 bars while price stayed above its level 2 bars ago. Fires on 10.55% of liquid bars. Sealed verdict: NULL — no effect that replicated.' },
  { key: 'pv_c_turn', label: 'TURN', cls: 'text-slate-300',
    hint: 'TURN (close) — price declined then recovered 2 bars, with the script\'s volume turn structure. Fires on 0.74% of liquid bars. Sealed verdict: NULL — no effect that replicated.' },
  { key: 'pv_c_up4', label: 'UP4', cls: 'text-slate-300',
    hint: 'UP4 (close) — price rose 4 bars while volume expanded, rested, then re-expanded above the prior peak. Fires on 1.42% of liquid bars. Sealed verdict: NULL — no effect that replicated.' },
  { key: 'pv_c_re2', label: 'RE2', cls: 'text-rose-300',
    hint: 'RE2 (close) — price down-down-up while volume went up-down-up. Fires on 2.19% of liquid bars. Sealed verdict: VETO_CANDIDATE — −0.689 MINE / −0.320 VERIFY, dsr_neg 1.000.' },
  { key: 'pv_veto_c', label: '⛔veto', cls: 'text-red-300',
    hint: '⛔ any of the four VETO_CANDIDATE shapes (REV RE2 UPP RUP) on the close source. Recorded, NOT applied.' },
  // ── VOL ECHO — the TradingView "260925_VOL_ECHO" script ported at its defaults
  //   (vol_echo_build.py → data/vol_echo_signals.parquet, _enrich_vol_echo). Daily bars only.
  //   DESCRIPTIVE ONLY: VOL_ECHO_LONG_V1 0/274 · LONG_2326 0/286 · TRADE_V1 0/9 — every long study
  //   NULL. The one confirmed result is a VETO (QR_REL_V1: yesterday Q∧R, today the first ▲ release,
  //   −1.40 pp vs a random buy in VERIFY, 6/6 years) — recorded, NOT applied. SPK is hindsight and is
  //   deliberately not offered as a filter. Never a ranking input.
  { divider: true, label: 'VOL ECHO — descriptive; long studies NULL, ⛔QR▲ = confirmed veto' },
  { key: 've_echo', label: 'VE', cls: 'text-cyan-300',
    hint: 'VE — echo: volume within ±40% of an earlier spike (≥1.5× median), in the same price zone, 5-250 bars later. Fires on ~13.5% of bars: context, not an event.' },
  { key: 've_q', label: 'Q', cls: 'text-purple-300',
    hint: 'Q — closes inside the armed echo zone on volume ≤ 0.8 × the 20-bar median (zone armed 60 bars after the echo).' },
  { key: 've_r', label: 'R', cls: 'text-orange-300',
    hint: 'R — volume ≤ 0.8 × median within 20 bars after BD▼ / BDV▼. Fires on ~15.6% of bars.' },
  { key: 've_qr', label: 'Q∧R', cls: 'text-orange-200',
    hint: 'Q∧R — both on the same bar: price broke below the zone, then came back inside on low volume. QR_RSI_V1: ≈0 alone, −0.88 pp with RSI > 50 (below the veto bar).' },
  { key: 've_rel_up', label: '▲rel', cls: 'text-green-300',
    hint: '▲ RELEASE — green bar with volume > SMA20 after quiet bars (first 3 of the series). VOL_ECHO_TRADE_V1: NULL as an entry.' },
  { key: 've_rel_dn', label: '▼rel', cls: 'text-red-300',
    hint: '▼ RELEASE — red bar with volume > SMA20 after quiet bars (first 3 of the series).' },
  { key: 've_bo', label: 'BO▲', cls: 'text-blue-300',
    hint: 'BO▲ — first green candle that opens AND closes above the echo zone (within 120 bars). NULL as an entry.' },
  { key: 've_bov', label: 'BOV▲', cls: 'text-blue-200',
    hint: 'BOV▲ — BO▲ with volume > SMA20. NULL as an entry (SL5/TP15, raw / EMA200 / RS).' },
  { key: 've_bd', label: 'BD▼', cls: 'text-fuchsia-300',
    hint: 'BD▼ — first red candle that opens AND closes below the echo zone.' },
  { key: 've_bdv', label: 'BDV▼', cls: 'text-fuchsia-200',
    hint: 'BDV▼ — BD▼ with volume > SMA20.' },
  { key: 've_zone_armed', label: 'zone', cls: 'text-gray-400',
    hint: 'An echo zone is armed on this bar (≤ 60 bars since the last echo).' },
  { key: 've_spike', label: '●spk', cls: 'text-amber-300',
    hint: 'Spike — volume ≥ 1.5 × the 20-bar median. ~23% of bars: context.' },
  { key: 've_qr_rel_veto', label: '⛔QR▲', cls: 'text-rose-300',
    hint: '⛔ QR_REL_V1 VETO CONFIRMED — yesterday Q∧R, today the first ▲ release. −1.82 pp MINE / −1.40 pp VERIFY vs a random buy, negative in all 6 years (book ATR×12 exit). Recorded, NOT applied — nothing is suppressed.' },
  // ── TURN·58 + ⟲ROW — turn-zone gauges (turn_rowseq_build.py → data/turn_rowseq_signals.parquet,
  //   _enrich_turn_rowseq; computed nightly with the Superchart's OWN lib/turnCount.js + lib/rowSeq.js).
  //   DESCRIPTIVE ONLY (research_out/TURN_SET_V1.md, ROWSEQ_V1.md): both identify turn zones out of sample;
  //   neither picks a better trade (same-day Δ ≈ 0) and ≥ +30 % big-move prediction was NULL — a
  //   watchlist filter, never a ranking input. Daily bars only.
  { divider: true, label: 'TURN · ⟲ROW — turn-zone identification; not a buy signal' },
  { key: 'turn_ge20', label: 'TURN≥20', cls: 'text-amber-200',
    hint: 'TURN·58 ≥ 20 — 10-bar low with ≥20 of the 58 turn-enriched signals in the last 3 bars. 2024-26: turned 1.34× as often as an average 10-bar low (NASDAQ out of sample 1.42). As a trade: not better than the day\'s other lows.' },
  { key: 'turn_ge26', label: 'TURN≥26', cls: 'text-amber-200',
    hint: 'TURN·58 ≥ 26 — NASDAQ out of sample: 1.56× the turn rate of an average 10-bar low (~3 % of lows). Identification only.' },
  { key: 'turn_ge30', label: 'TURN≥30', cls: 'text-amber-100',
    hint: 'TURN·58 ≥ 30 — 2024-26: 1.41× (plateau top band). Identification only.' },
  { key: 'rs_pair', label: '◆V∧M', cls: 'text-amber-100',
    hint: '◆ VOL7 and MTF rows both CONFIRMED — the one row pair that added beyond the best single row: turn-zone lift 1.88 after removing price-location × ATR% effects (2024-26). ~0.4 % of bars. Same-day return ≈ 0.' },
  { key: 'rs_c2', label: 'ROW●≥2', cls: 'text-amber-200',
    hint: '≥ 2 rows CONFIRMED at once. ROWSEQ_V1: many rows together added NOTHING over the best single row (COUNT: 1.51 vs MTF 1.52) — kept as a convenience filter.' },
  { key: 'rs_c3', label: 'ROW●≥3', cls: 'text-amber-200',
    hint: '≥ 3 rows CONFIRMED at once. Same caveat: confluence of rows did not add beyond the best row.' },
  { key: 'rs_fly_c', label: 'FLY●', cls: 'text-amber-200',
    hint: 'FLY row turn sequence CONFIRMED (top 5 % of MINE scores; ✦fresh ABCD/CD/BD/AD at t-0/1). Turn-zone lift 1.63 after removing price-location × ATR% effects (2024-26). Identification only.' },
  { key: 'rs_fly_e', label: 'FLY○+', cls: 'text-slate-300',
    hint: 'FLY row turn sequence EARLY or CONFIRMED (top 20 % of MINE scores).' },
  { key: 'rs_gr_c', label: 'GR●', cls: 'text-amber-200',
    hint: 'GR row turn sequence CONFIRMED (top 5 % of MINE scores; G3 / V gaps, G1→G3 orders). Turn-zone lift 1.6 after removing price-location × ATR% effects (2024-26). Identification only.' },
  { key: 'rs_gr_e', label: 'GR○+', cls: 'text-slate-300',
    hint: 'GR row turn sequence EARLY or CONFIRMED (top 20 % of MINE scores).' },
  { key: 'rs_mtf_c', label: 'MTF●', cls: 'text-amber-200',
    hint: 'MTF row turn sequence CONFIRMED (top 5 % of MINE scores; ▲4H / △1H REV triggers, repeated, 1H→4H order). Turn-zone lift 1.52 after removing price-location × ATR% effects (2024-26). Identification only.' },
  { key: 'rs_mtf_e', label: 'MTF○+', cls: 'text-slate-300',
    hint: 'MTF row turn sequence EARLY or CONFIRMED (top 20 % of MINE scores).' },
  { key: 'rs_phys_c', label: '⚛●', cls: 'text-amber-200',
    hint: '⚛ row turn sequence CONFIRMED (top 5 % of MINE scores; R·D, K1D, S3D, ★/★★, gG2/3, SPRING⚛). Turn-zone lift 1.45 after removing price-location × ATR% effects (2024-26). Identification only.' },
  { key: 'rs_phys_e', label: '⚛○+', cls: 'text-slate-300',
    hint: '⚛ row turn sequence EARLY or CONFIRMED (top 20 % of MINE scores).' },
  { key: 'rs_vol7_c', label: 'VOL7●', cls: 'text-amber-200',
    hint: 'VOL7 row turn sequence CONFIRMED (top 5 % of MINE scores; M4-M6·σ5/σ6, Σ+, VB2 sequences). Turn-zone lift 1.43 after removing price-location × ATR% effects (2024-26). Identification only.' },
  { key: 'rs_vol7_e', label: 'VOL7○+', cls: 'text-slate-300',
    hint: 'VOL7 row turn sequence EARLY or CONFIRMED (top 20 % of MINE scores).' },
  { key: 'rs_pv_c', label: 'PV●', cls: 'text-amber-200',
    hint: 'PV row turn sequence CONFIRMED (top 5 % of MINE scores; price×volume shapes). Turn-zone lift 1.42 after removing price-location × ATR% effects (2024-26). Identification only.' },
  { key: 'rs_pv_e', label: 'PV○+', cls: 'text-slate-300',
    hint: 'PV row turn sequence EARLY or CONFIRMED (top 20 % of MINE scores).' },
  { key: 'rs_break_c', label: 'BRK●', cls: 'text-amber-200',
    hint: 'BRK row turn sequence CONFIRMED (top 5 % of MINE scores; BO/BX/BE/EB/FBO/4BF sequences). Turn-zone lift 1.41 after removing price-location × ATR% effects (2024-26). Identification only.' },
  { key: 'rs_break_e', label: 'BRK○+', cls: 'text-slate-300',
    hint: 'BRK row turn sequence EARLY or CONFIRMED (top 20 % of MINE scores).' },
  { key: 'rs_ovd_c', label: 'OVD●', cls: 'text-amber-200',
    hint: 'OVD row turn sequence CONFIRMED (top 5 % of MINE scores; OB/RC/CD/HO opening/closing volume logics). Turn-zone lift 1.41 after removing price-location × ATR% effects (2024-26). Identification only.' },
  { key: 'rs_ovd_e', label: 'OVD○+', cls: 'text-slate-300',
    hint: 'OVD row turn sequence EARLY or CONFIRMED (top 20 % of MINE scores).' },
  { key: 'rs_delta_c', label: 'Δ●', cls: 'text-amber-200',
    hint: 'Δ row turn sequence CONFIRMED (top 5 % of MINE scores; FLP↑ ΔΔ↑ Δ↑ NS …). Turn-zone lift 1.4 after removing price-location × ATR% effects (2024-26). Identification only.' },
  { key: 'rs_delta_e', label: 'Δ○+', cls: 'text-slate-300',
    hint: 'Δ row turn sequence EARLY or CONFIRMED (top 20 % of MINE scores).' },
  { divider: true },
  // ── F / G signals ─────────────────────────────────────────────────────
  { key: 'cd',  label: 'CD',  cls: 'text-lime-300'    },
  { key: 'ca',  label: 'CA',  cls: 'text-cyan-300'    },
  { key: 'cw',  label: 'CW',  cls: 'text-yellow-300'  },
  { key: 'g1',  label: 'G1',  cls: 'text-lime-300'    },
  { key: 'g2',  label: 'G2',  cls: 'text-cyan-300'    },
  { key: 'g4',  label: 'G4',  cls: 'text-fuchsia-300' },
  { key: 'g6',  label: 'G6',  cls: 'text-orange-300'  },
  { key: 'g11',       label: 'G11',  cls: 'text-yellow-300'  },
  { key: 'seq_bcont', label: 'SBC',  cls: 'text-violet-300'  },
  { divider: true },
  // ── T signals (Bullish — priority engine) ────────────────────────────
  // Keys map to sig_ages entries → N= lookback works automatically
  { key: 'tz_any_t', label: 'ANY T', cls: 'text-blue-200 font-semibold' },
  { key: 'tz_t1g',   label: 'T1G',   cls: 'text-amber-300 font-bold'   },
  { key: 'tz_t2g',   label: 'T2G',   cls: 'text-amber-400 font-bold'   },
  { key: 'tz_t1',    label: 'T1',    cls: 'text-lime-300'               },
  { key: 'tz_t2',    label: 'T2',    cls: 'text-lime-400'               },
  { key: 'tz_t3',    label: 'T3',    cls: 'text-green-300'              },
  { key: 'tz_t4',    label: 'T4',    cls: 'text-emerald-300 font-bold'  },
  { key: 'tz_t5',    label: 'T5',    cls: 'text-teal-300'               },
  { key: 'tz_t6',    label: 'T6',    cls: 'text-cyan-300 font-bold'     },
  { key: 'tz_t9',    label: 'T9',    cls: 'text-sky-300'                },
  { key: 'tz_t10',   label: 'T10',   cls: 'text-blue-300'               },
  { key: 'tz_t11',   label: 'T11',   cls: 'text-indigo-300'             },
  { key: 'tz_t12',   label: 'T12',   cls: 'text-violet-300'             },
  { key: 'tz_bull_flip', label: 'TZ→3', cls: 'text-lime-300'            },
  { key: 'tz_attempt',   label: 'TZ→2', cls: 'text-cyan-300'            },
  { key: 'tz_weak_bull', label: 'W',    cls: 'text-yellow-300'          },
  { divider: true },
  // ── Z signals (Bearish — priority engine) ────────────────────────────
  { key: 'tz_any_z', label: 'ANY Z', cls: 'text-red-300 font-semibold'  },
  { key: 'tz_z1g',   label: 'Z1G',   cls: 'text-red-200 font-bold'     },
  { key: 'tz_z2g',   label: 'Z2G',   cls: 'text-red-300 font-bold'     },
  { key: 'tz_z1',    label: 'Z1',    cls: 'text-orange-300'             },
  { key: 'tz_z2',    label: 'Z2',    cls: 'text-orange-400'             },
  { key: 'tz_z3',    label: 'Z3',    cls: 'text-rose-300'               },
  { key: 'tz_z4',    label: 'Z4',    cls: 'text-red-400 font-bold'      },
  { key: 'tz_z5',    label: 'Z5',    cls: 'text-pink-300'               },
  { key: 'tz_z6',    label: 'Z6',    cls: 'text-fuchsia-400 font-bold'  },
  { key: 'tz_z7',    label: 'Z7',    cls: 'text-rose-400'               },
  { key: 'tz_z9',    label: 'Z9',    cls: 'text-orange-300'             },
  { key: 'tz_z10',   label: 'Z10',   cls: 'text-red-300'                },
  { key: 'tz_z11',   label: 'Z11',   cls: 'text-rose-300'               },
  { key: 'tz_z12',   label: 'Z12',   cls: 'text-pink-400'               },
  { divider: true, label: 'L1 · L code' },
  // ── Line 1 — L code (l_sig = "L" + fired digits, ascending; e.g. L46 = L4+L6).
  //    Single digits match PRESENCE (L4 fires inside L46/L34/L4); combos are exact.
  { key: '_wl_any', label: 'ANY L', cls: 'text-sky-200 font-semibold',
    custom: r => /^L[1-6]+$/.test(r.tz_wlnbb_l_signal || '') },
  { key: '_wl_l1',  label: 'L1',   cls: 'text-sky-300',
    custom: r => { const s = r.tz_wlnbb_l_signal || ''; return /^L[1-6]+$/.test(s) && s.includes('1') } },
  { key: '_wl_l2',  label: 'L2',   cls: 'text-cyan-300',
    custom: r => { const s = r.tz_wlnbb_l_signal || ''; return /^L[1-6]+$/.test(s) && s.includes('2') } },
  { key: '_wl_l3',  label: 'L3',   cls: 'text-teal-300',
    custom: r => { const s = r.tz_wlnbb_l_signal || ''; return /^L[1-6]+$/.test(s) && s.includes('3') } },
  { key: '_wl_l4',  label: 'L4',   cls: 'text-blue-300',
    custom: r => { const s = r.tz_wlnbb_l_signal || ''; return /^L[1-6]+$/.test(s) && s.includes('4') } },
  { key: '_wl_l5',  label: 'L5',   cls: 'text-indigo-300',
    custom: r => { const s = r.tz_wlnbb_l_signal || ''; return /^L[1-6]+$/.test(s) && s.includes('5') } },
  { key: '_wl_l6',  label: 'L6',   cls: 'text-violet-300',
    custom: r => { const s = r.tz_wlnbb_l_signal || ''; return /^L[1-6]+$/.test(s) && s.includes('6') } },
  // exact combos of interest: L34 = L3+L4, L46 = L4+L6 (L12/L25 compose via L1-L6)
  { key: '_wl_l34', label: 'L34',  cls: 'text-lime-300',
    custom: r => r.tz_wlnbb_l_signal === 'L34' },
  { key: '_wl_l46', label: 'L46',  cls: 'text-orange-300',
    custom: r => r.tz_wlnbb_l_signal === 'L46' },
  { divider: true, label: 'L2 suffix' },
  // ── Line 2 — suffix: NE + wick(U/D/B) + penetration(P/R/H) + close(A/O/I) ──
  { key: '_ne_n', label: 'N',  cls: 'text-gray-300',
    custom: r => ((r.tz_wlnbb_ne_suffix || r.tz_wlnbb_full_suffix) || '').startsWith('N') },
  { key: '_ne_e', label: 'E',  cls: 'text-lime-300',
    custom: r => ((r.tz_wlnbb_ne_suffix || r.tz_wlnbb_full_suffix) || '').startsWith('E') },
  { key: '_wk_u', label: 'U', cls: 'text-teal-300',
    custom: r => (r.tz_wlnbb_wick_suffix || '').includes('U') },
  { key: '_wk_d', label: 'D', cls: 'text-red-300',
    custom: r => (r.tz_wlnbb_wick_suffix || '').includes('D') },
  { key: '_wk_b', label: 'B', cls: 'text-yellow-300',
    custom: r => (r.tz_wlnbb_wick_suffix || '').includes('B') },
  // penetration P/R/H + close A/O/I — read the full suffix string (disjoint letters)
  { key: '_pen_p', label: 'P',  cls: 'text-emerald-300',
    custom: r => (r.tz_wlnbb_full_suffix || '').includes('P') },
  { key: '_pen_r', label: 'R',  cls: 'text-amber-300',
    custom: r => (r.tz_wlnbb_full_suffix || '').includes('R') },
  { key: '_pen_h', label: 'H',  cls: 'text-orange-300',
    custom: r => (r.tz_wlnbb_full_suffix || '').includes('H') },
  { key: '_cl_a', label: 'A',  cls: 'text-lime-400',
    custom: r => (r.tz_wlnbb_full_suffix || '').includes('A') },
  { key: '_cl_o', label: 'O',  cls: 'text-gray-300',
    custom: r => (r.tz_wlnbb_full_suffix || '').includes('O') },
  { key: '_cl_i', label: 'I',  cls: 'text-red-400',
    custom: r => (r.tz_wlnbb_full_suffix || '').includes('I') },
  { divider: true, label: 'L3 body·wick' },
  // ── Line 3 — bar body/wick classification ────────────────────────────
  { key: '_bw_x',  label: 'X',   cls: 'text-lime-300 font-bold',
    custom: r => (r.tz_wlnbb_bar_body_wick || '').startsWith('X') },
  { key: '_bw_m',  label: 'M',   cls: 'text-yellow-300',
    custom: r => (r.tz_wlnbb_bar_body_wick || '').startsWith('M') },
  { key: '_bw_s',  label: 'S',   cls: 'text-blue-300',
    custom: r => (r.tz_wlnbb_bar_body_wick || '').startsWith('S') },
  { key: '_bw_j',  label: 'J',   cls: 'text-gray-400',
    custom: r => (r.tz_wlnbb_bar_body_wick || '').includes('J') },
  { key: '_bw_tb', label: 'TB',  cls: 'text-red-300',
    custom: r => (r.tz_wlnbb_bar_body_wick || '').includes('TB') },
  { key: '_bw_bb', label: 'BB',  cls: 'text-green-300',
    custom: r => (r.tz_wlnbb_bar_body_wick || '').includes('BB') },
  { key: '_bw_f',  label: 'F',   cls: 'text-cyan-300',
    custom: r => (r.tz_wlnbb_bar_body_wick || '').includes('F') },
  { key: '_bw_xf', label: 'XF',  cls: 'text-lime-400 font-semibold',
    custom: r => r.tz_wlnbb_bar_body_wick === 'XF' },
  { key: '_bw_mf', label: 'MF',  cls: 'text-yellow-400',
    custom: r => r.tz_wlnbb_bar_body_wick === 'MF' },
  { divider: true, label: '⚛ physics (260815)' },
  // ── ⚛ Market-physics state, the same fields the superchart ⚛ row prints ──────
  // Added the day the row was, because a state the chart marks and the screener
  // cannot filter is half a feature — you can see it on one name and have no way
  // to go looking for it across the universe.
  //
  // What each is worth was measured today and is deliberately NOT encoded here as
  // a ranking: on 3.3M rows crossed against the book's edges, only three states
  // held their sign across both a 2021-23 mining window and a reserved 2024-26 one
  // (RN and C0 mildly positive, RF consistently negative). BB led the mining
  // window and inverted out of sample. These are filters, not recommendations.
  { key: '_ph_ra', label: 'RA',  cls: 'text-sky-300 font-semibold',
    custom: r => r.phys_r === 'RA' },
  { key: '_ph_rn', label: 'RN',  cls: 'text-slate-300',
    custom: r => r.phys_r === 'RN' },
  { key: '_ph_rf', label: 'RF',  cls: 'text-orange-300',
    custom: r => r.phys_r === 'RF' },
  { key: '_ph_u',  label: '·U',  cls: 'text-emerald-300',
    custom: r => r.phys_regime === 'U' },
  { key: '_ph_d',  label: '·D',  cls: 'text-rose-300',
    custom: r => r.phys_regime === 'D' },
  { key: '_ph_e2', label: 'E2',  cls: 'text-amber-300',
    custom: r => (r.phys_e || '').startsWith('E2') },
  { key: '_ph_es', label: 'E★',  cls: 'text-violet-300 font-bold',
    custom: r => (r.phys_e || '').includes('★') },
  { key: '_ph_k2', label: 'K2',  cls: 'text-red-300 font-bold',
    custom: r => (r.phys_k || '').startsWith('K2') },
  { key: '_ph_k1', label: 'K1',  cls: 'text-red-200',
    custom: r => (r.phys_k || '').startsWith('K1') },
  { key: '_ph_k0', label: 'K0',  cls: 'text-gray-400',
    custom: r => r.phys_k === 'K0' },
  { key: '_ph_c0', label: 'C0',  cls: 'text-teal-300',
    custom: r => r.phys_c === 'C0' },
  { key: '_ph_c1', label: 'C1',  cls: 'text-gray-400',
    custom: r => r.phys_c === 'C1' },
  { key: '_ph_c2', label: 'C2',  cls: 'text-fuchsia-300',
    custom: r => r.phys_c === 'C2' },
  { key: '_ph_h0', label: 'H0',  cls: 'text-emerald-300',
    custom: r => r.phys_h === 'H0' },
  { key: '_ph_h2', label: 'H2',  cls: 'text-rose-300',
    custom: r => r.phys_h === 'H2' },
  { key: '_ph_m2', label: 'M2',  cls: 'text-blue-300',
    custom: r => r.phys_m === 'M2' },
  { key: '_ph_s3u', label: 'S3U', cls: 'text-green-300',
    custom: r => r.phys_s === 'S3U' },
  { key: '_ph_s3d', label: 'S3D', cls: 'text-red-300',
    custom: r => r.phys_s === 'S3D' },
  // AD split by grade. The ⚛ row draws ★ / ★★ / ★A as different marks and the one
  // chip collapsed all three; ★A (the exhaustion bar was absorbed) is 1,548 bars
  // against ★★'s 31,788, so "any AD" is effectively "★★" and the rare one is
  // unreachable through it.
  { key: '_ph_ad',  label: 'AD any', cls: 'text-fuchsia-300',
    custom: r => !!r.phys_ad },
  { key: '_ph_ad1', label: '★',      cls: 'text-fuchsia-300',
    custom: r => r.phys_ad === '★' },
  { key: '_ph_ad2', label: '★★',     cls: 'text-fuchsia-300 font-bold',
    custom: r => r.phys_ad === '★★' },
  { key: '_ph_ada', label: '★A',     cls: 'text-violet-300 font-bold',
    custom: r => r.phys_ad === '★A' },
  { key: '_ph_gg3', label: 'gG3', cls: 'text-amber-300',
    custom: r => r.phys_gap_true === 'G3' },
  // ⚛ Wyckoff EVENTS. Not the same thing as the WYC: row above, which reads
  // wyc_phase — the two agree on 97.4% of bars, and disagree exactly on the events:
  // wyc_phase says UTAD on 390 bars where phys_wyc says it on 8,833, because the
  // older column mostly reports the macro phase and this one marks the event on top
  // of it. Selecting UTAD in one place and UTAD⚛ here are different questions.
  { key: '_ph_spr',  label: 'SPRING⚛',  cls: 'text-lime-300',
    custom: r => r.phys_wyc === 'SPRING' },
  { key: '_ph_sprs', label: 'SPRING★⚛', cls: 'text-lime-300 font-bold',
    custom: r => r.phys_wyc === 'SPRING★' },
  { key: '_ph_utad', label: 'UTAD⚛',    cls: 'text-rose-300 font-bold',
    custom: r => r.phys_wyc === 'UTAD' },
  { key: '_ph_sos',  label: 'SOS★⚛',    cls: 'text-green-300 font-bold',
    custom: r => r.phys_wyc === 'SOS★' },
  { key: '_ph_acc',  label: 'ACC-TR⚛',  cls: 'text-teal-300',
    custom: r => r.phys_wyc === 'ACC-TR' },
  { key: '_ph_dist', label: 'DIST-TR⚛', cls: 'text-amber-300',
    custom: r => r.phys_wyc === 'DIST-TR' },
  { key: '_ph_mkup', label: 'MARKUP⚛',  cls: 'text-emerald-300',
    custom: r => r.phys_wyc === 'MARKUP' },
  { key: '_ph_mkdn', label: 'MKDN⚛',    cls: 'text-rose-300',
    custom: r => r.phys_wyc === 'MKDN' },
  { divider: true, label: '⟂ CISD (260815)' },
  // ── ⟂ Change in state of delivery ────────────────────────────────────────────
  // +S is the structural half. Until today's engine fix it could never fire at all
  // (the market structure seeded from 0.0, so `low < bottomPrice` meant `low < 0`),
  // which is also why the ++- sequence had never been true in 773,606 bars.
  { key: '_ci_ps', label: '+S',  cls: 'text-emerald-300 font-bold',
    custom: r => !!r.cisd_plus_struct },
  { key: '_ci_ms', label: '−S',  cls: 'text-slate-400',
    custom: r => !!r.cisd_minus_struct },
  { key: '_ci_sq', label: '⟂seq', cls: 'text-gray-400',
    custom: r => !!r.cisd_seq },
  { key: '_ci_mp', label: '⟂mpm', cls: 'text-gray-400',
    custom: r => !!r.cisd_mpm },
  { divider: true, label: 'L4 gap·range' },
  // ── Line 4 — gap/range vs ATR ────────────────────────────────────────
  { key: '_gr_g1', label: 'G1',  cls: 'text-sky-300',
    custom: r => (r.tz_wlnbb_bar_gap_range || '').includes('G1') },
  { key: '_gr_g2', label: 'G2',  cls: 'text-blue-300',
    custom: r => (r.tz_wlnbb_bar_gap_range || '').includes('G2') },
  { key: '_gr_g3', label: 'G3',  cls: 'text-violet-300',
    custom: r => (r.tz_wlnbb_bar_gap_range || '').includes('G3') },
  { key: '_gr_v',  label: 'V',   cls: 'text-red-300 font-bold',
    custom: r => (r.tz_wlnbb_bar_gap_range || '').includes('V') },
  { key: '_gr_c',  label: 'C',   cls: 'text-teal-300',
    custom: r => (r.tz_wlnbb_bar_gap_range || '').includes('C') },
  { key: '_gr_n',  label: 'N',   cls: 'text-gray-400',
    custom: r => r.tz_wlnbb_bar_gap_range === 'N' },
  { divider: true, label: 'L5 vix·psar·rsi2' },
  // ── Line 5 — VIX-Fix / PSAR / RSI2 ───────────────────────────────────
  { key: '_l5_any', label: 'L5∗',  cls: 'text-amber-200 font-semibold',
    custom: r => !!r.tz_wlnbb_bar_line5 },
  { key: '_l5_vx',  label: 'VX',   cls: 'text-red-300 font-bold',
    custom: r => (r.tz_wlnbb_bar_line5 || '').includes('VX') },
  { key: '_l5_vr',  label: 'VR',   cls: 'text-orange-300',
    custom: r => (r.tz_wlnbb_bar_line5 || '').includes('VR') },
  { key: '_l5_pb',  label: 'PB',   cls: 'text-lime-300',
    custom: r => (r.tz_wlnbb_bar_line5 || '').includes('PB') },
  { key: '_l5_ps',  label: 'PS',   cls: 'text-red-400',
    custom: r => (r.tz_wlnbb_bar_line5 || '').includes('PS') },
  { key: '_l5_r2h', label: 'R2H',  cls: 'text-blue-300',
    custom: r => (r.tz_wlnbb_bar_line5 || '').includes('R2H') },
  { key: '_l5_r2l', label: 'R2L',  cls: 'text-cyan-300',
    custom: r => (r.tz_wlnbb_bar_line5 || '').includes('R2L') },
  { key: '_l5_r2x', label: 'R2X',  cls: 'text-indigo-300',
    custom: r => (r.tz_wlnbb_bar_line5 || '').includes('R2X') },
  { key: '_l5_r2d', label: 'R2D',  cls: 'text-fuchsia-300',
    custom: r => (r.tz_wlnbb_bar_line5 || '').includes('R2D') },
  { divider: true, label: 'L6 vol' },
  // ── Line 6 — volume bucket (Live uses tz_wlnbb_volume_bucket, DB-instant uses vol_bucket) ──
  { key: '_vb_vb', label: 'VB',  cls: 'text-red-300 font-bold',
    custom: r => (r.tz_wlnbb_volume_bucket || r.vol_bucket) === 'VB' },
  { key: '_vb_b',  label: 'B',   cls: 'text-orange-300',
    custom: r => (r.tz_wlnbb_volume_bucket || r.vol_bucket) === 'B' },
  { key: '_vb_n',  label: 'N',   cls: 'text-yellow-300',
    custom: r => (r.tz_wlnbb_volume_bucket || r.vol_bucket) === 'N' },
  { key: '_vb_l',  label: 'L',   cls: 'text-blue-400',
    custom: r => (r.tz_wlnbb_volume_bucket || r.vol_bucket) === 'L' },
  { key: '_vb_w',  label: 'W',   cls: 'text-gray-400',
    custom: r => (r.tz_wlnbb_volume_bucket || r.vol_bucket) === 'W' },
  { divider: true, label: 'Wyckoff cycle (260529)' },
  // ── Wyckoff V2 Soft state machine: SC→AR→ST→Spring→SOS/JAC→LPS ──────────
  { key: 'w2_accum',  label: 'ACC',  cls: 'text-green-300 font-semibold' },
  { key: 'w2_break',  label: 'BRK',  cls: 'text-lime-300 font-semibold' },
  { key: 'w2_sc',     label: 'SC',   cls: 'text-red-300' },
  { key: 'w2_ar',     label: 'AR',   cls: 'text-orange-300' },
  { key: 'w2_st',     label: 'ST',   cls: 'text-purple-300' },
  { key: 'w2_spring', label: 'SPR',  cls: 'text-teal-300 font-semibold' },
  { key: 'w2_sos',    label: 'SOS',  cls: 'text-green-400 font-semibold' },
  { key: 'w2_jac',    label: 'JAC',  cls: 'text-lime-400 font-semibold' },
  { key: 'w2_lps',    label: 'LPS',  cls: 'text-blue-300 font-semibold' },
  { key: 'w2_evr',    label: 'EVR',  cls: 'text-fuchsia-300' },
  { divider: true, label: 'Wyckoff trig (agent)' },
  // ── Structure triggers (valid-TR gated) ────────────────────────────────
  { key: 'wt_valid_tr', label: 'TR',    cls: 'text-cyan-300' },
  { key: 'wt_spring',   label: 'tSPR',  cls: 'text-teal-300' },
  { key: 'wt_sos',      label: 'tSOS',  cls: 'text-green-400' },
  { key: 'wt_lps',      label: 'tLPS',  cls: 'text-blue-300' },
  { key: 'wt_evr',      label: 'tEVR',  cls: 'text-fuchsia-300' },
  { divider: true },
  // ── WLNBB / L-signals ─────────────────────────────────────────────────
  { key: '_l_any',  label: 'L∗',  cls: 'text-blue-200',
    custom: r => r.l34 || r.l43 || r.be_up },
  { key: '_be_any', label: 'BE',  cls: 'text-emerald-200',
    custom: r => r.be_up || r.be_dn },
  { divider: true },
  { key: 'fri34',         label: 'FRI34',    cls: 'text-cyan-400'     },
  { key: 'fri43',         label: 'FRI43',    cls: 'text-sky-300'      },
  { key: 'fri64',         label: 'FRI64',    cls: 'text-indigo-300'   },
  { key: 'l34',           label: 'L34',      cls: 'text-blue-300'     },
  { key: 'l43',           label: 'L43',      cls: 'text-teal-300'     },
  { key: 'l22',           label: 'L22',      cls: 'text-red-300'      },
  { key: 'l555',          label: 'L555',     cls: 'text-rose-400'     },
  { key: 'blue',          label: 'BL',       cls: 'text-sky-300'      },
  { key: 'cci_ready',     label: 'CCI',      cls: 'text-violet-300'   },
  { key: 'cci_0_retest',  label: 'CCI0R',    cls: 'text-violet-400'   },
  { key: 'cci_blue_turn', label: 'CCIB',     cls: 'text-purple-300'   },
  { key: 'bo_up',         label: 'BO↑',      cls: 'text-lime-300'     },
  { key: 'bo_dn',         label: 'BO↓',      cls: 'text-red-400'      },
  { key: 'bx_up',         label: 'BX↑',      cls: 'text-lime-400'     },
  { key: 'bx_dn',         label: 'BX↓',      cls: 'text-red-400'      },
  { key: 'be_up',         label: 'BE↑',      cls: 'text-emerald-300'  },
  { key: 'be_dn',         label: 'BE↓',      cls: 'text-red-300'      },
  { key: 'fuchsia_rh',    label: 'RH',       cls: 'text-fuchsia-400'  },
  { key: 'fuchsia_rl',    label: 'RL',       cls: 'text-fuchsia-300'  },
  { key: 'pre_pump',      label: 'PP',       cls: 'text-yellow-400'   },
  { divider: true },
  // ── Wick / CISD ───────────────────────────────────────────────────────
  { key: 'x2g_wick',  label: 'X2G',  cls: 'text-cyan-300'    },
  { key: 'x2_wick',   label: 'X2',   cls: 'text-sky-300'     },
  { key: 'x1g_wick',  label: 'X1G',  cls: 'text-lime-300'    },
  { key: 'x1_wick',   label: 'X1',   cls: 'text-green-300'   },
  { key: 'x3_wick',   label: 'X3',   cls: 'text-yellow-300'  },
  { key: 'wick_bull', label: 'WK↑',  cls: 'text-emerald-400' },
  { divider: true },
  // ── ULTRA v2 ──────────────────────────────────────────────────────────
  { key: 'best_long',  label: 'BEST↑', cls: 'text-yellow-300'  },
  { key: 'fbo_bull',   label: 'FBO↑',  cls: 'text-sky-300'     },
  { key: 'fbo_bear',   label: 'FBO↓',  cls: 'text-red-400'     },
  { key: 'eb_bull',    label: 'EB↑',   cls: 'text-amber-300'   },
  { key: 'eb_bear',    label: 'EB↓',   cls: 'text-red-400'     },
  { key: 'bf_buy',     label: '4BF',    cls: 'text-pink-300'    },
  { key: 'bf_sell',    label: '4BF↓',   cls: 'text-red-400'    },
  { key: 'ultra_3up',  label: '3↑',     cls: 'text-lime-300'    },
  { divider: true },
  // ── 260308 + L88 ──────────────────────────────────────────────────────
  { key: 'sig_l88',    label: 'L88',    cls: 'text-violet-200'  },
  { key: 'sig_260308', label: '260308', cls: 'text-purple-300'  },
  { divider: true },
  // ── Delta / order-flow (260403) ────────────────────────────────────────
  { key: 'd_blast_bull',  label: 'ΔΔ↑',  cls: 'text-yellow-300'  },
  { key: 'd_surge_bull',  label: 'Δ↑',   cls: 'text-teal-300'    },
  { key: 'd_strong_bull', label: 'B/S↑', cls: 'text-lime-300'    },
  { key: 'd_absorb_bull', label: 'Ab↑',  cls: 'text-yellow-400'  },
  { key: 'd_spring',      label: 'dSPR', cls: 'text-lime-300'    },
  { key: 'd_div_bull',    label: 'T↓',   cls: 'text-cyan-300'    },
  { key: 'd_vd_div_bull', label: 'NS',   cls: 'text-teal-400'    },
  { key: 'd_cd_bull',     label: 'cd↑',  cls: 'text-sky-300'     },
  { key: 'd_flip_bull',       label: 'FLP↑',  cls: 'text-orange-300'  },
  { key: 'd_orange_bull',     label: 'ORG↑',  cls: 'text-orange-400'  },
  { key: 'd_blast_bull_red',  label: 'ΔΔ↑R',  cls: 'text-rose-300'    },
  { key: 'd_surge_bull_red',  label: 'Δ↑R',   cls: 'text-pink-300'    },
  { key: 'd_surge_bear_grn',  label: 'Δ↓G',   cls: 'text-red-400'     },
  { key: 'd_blast_bear_grn',  label: 'ΔΔ↓G',  cls: 'text-red-500'     },
  { key: 'd_vd_div_bear',     label: 'ND',     cls: 'text-red-400'     },
  { divider: true },
  // ── PREUP — EMA cross ↑ ──────────────────────────────────────────────
  { key: '_any_p', label: 'ANY P', cls: 'text-cyan-200 font-semibold',
    custom: r => !!(r.preup66 || r.preup55 || r.preup89 || r.preup3 || r.preup2 || r.preup50) },
  { key: 'preup66', label: 'P66', cls: 'text-lime-300'    },
  { key: 'preup55', label: 'P55', cls: 'text-emerald-300' },
  { key: 'preup89', label: 'P89', cls: 'text-teal-300'    },
  { key: 'preup3',  label: 'P3',  cls: 'text-cyan-300'    },
  { key: 'preup2',  label: 'P2',  cls: 'text-cyan-400'    },
  { key: 'preup50', label: 'P50', cls: 'text-sky-300'     },
  { divider: true },
  // ── PREDN — EMA drop ↓ ───────────────────────────────────────────────
  { key: '_any_d', label: 'ANY D', cls: 'text-red-300 font-semibold',
    custom: r => !!(r.predn66 || r.predn55 || r.predn89 || r.predn3 || r.predn2 || r.predn50) },
  { key: 'predn66', label: 'D66', cls: 'text-red-300'     },
  { key: 'predn55', label: 'D55', cls: 'text-red-400'     },
  { key: 'predn89', label: 'D89', cls: 'text-orange-400'  },
  { key: 'predn3',  label: 'D3',  cls: 'text-orange-300'  },
  { key: 'predn2',  label: 'D2',  cls: 'text-red-300'     },
  { key: 'predn50', label: 'D50', cls: 'text-orange-300'  },
  { divider: true },
  // ── Price vs EMA ─────────────────────────────────────────────────────
  // Live scan returns raw EMA values (r.ema20/50/89/200); DB-instant mode does
  // not (the precomputed boolean flags price_gt_/lt_ are stored instead). Use
  // either source so the filter works in both modes.
  { key: '_gt_ema200', label: 'P>200', cls: 'text-lime-300',
    custom: r => !!r.price_gt_200 || (r.ema200 > 0 && r.last_price > r.ema200) },
  { key: '_gt_ema89',  label: 'P>89',  cls: 'text-emerald-300',
    custom: r => !!r.price_gt_89  || (r.ema89  > 0 && r.last_price > r.ema89)  },
  { key: '_gt_ema50',  label: 'P>50',  cls: 'text-teal-300',
    custom: r => !!r.price_gt_50  || (r.ema50  > 0 && r.last_price > r.ema50)  },
  { key: '_gt_ema20',  label: 'P>20',  cls: 'text-cyan-300',
    custom: r => !!r.price_gt_20  || (r.ema20  > 0 && r.last_price > r.ema20)  },
  { key: '_lt_ema20',  label: 'P<20',  cls: 'text-red-400',
    custom: r => !!r.price_lt_20  || (r.ema20  > 0 && r.last_price < r.ema20)  },
  { key: '_lt_ema50',  label: 'P<50',  cls: 'text-orange-400',
    custom: r => !!r.price_lt_50  || (r.ema50  > 0 && r.last_price < r.ema50)  },
  { key: '_lt_ema89',  label: 'P<89',  cls: 'text-orange-300',
    custom: r => !!r.price_lt_89  || (r.ema89  > 0 && r.last_price < r.ema89)  },
  { key: '_lt_ema200', label: 'P<200', cls: 'text-red-300',
    custom: r => !!r.price_lt_200 || (r.ema200 > 0 && r.last_price < r.ema200) },
  { divider: true },
  // ── RS / Relative Strength ────────────────────────────────────────────
  { key: 'rs_strong',  label: 'RS+',    cls: 'text-lime-300'    },
  { key: 'rs',         label: 'RS',     cls: 'text-green-400'   },
  { divider: true },
  // ── PARA 260420 — Parabola Start Detector ─────────────────────────────
  { key: 'para_prep',   label: 'PREP',   cls: 'text-green-300'   },
  { key: 'para_start',  label: 'PARA',   cls: 'text-lime-300'    },
  { key: 'para_plus',   label: 'PARA+',  cls: 'text-cyan-300 font-semibold' },
  { key: 'para_retest', label: 'RETEST', cls: 'text-emerald-300' },
  { divider: true },
  // ── RGTI 260404 + SMX 260402 — multi-TF EMA ───────────────────────────
  { key: 'rgti_ll',       label: 'LL',   cls: 'text-purple-300'   },
  { key: 'rgti_up',       label: 'UP',   cls: 'text-blue-300'     },
  { key: 'rgti_upup',     label: '↑↑',   cls: 'text-fuchsia-300'  },
  { key: 'rgti_upupup',   label: '↑↑↑',  cls: 'text-sky-300'      },
  { key: 'rgti_orange',   label: 'ORG',  cls: 'text-orange-300'   },
  { key: 'rgti_green',    label: 'GRN',  cls: 'text-green-300'    },
  { key: 'rgti_greencirc',label: 'GC',   cls: 'text-emerald-300'  },
  { key: 'smx',           label: 'SMX',  cls: 'text-lime-300'     },
  { divider: true, label: '🥇 Macro / sector (2026-08-06)' },
  // validated per-edge: median lift 7/7 edges, DSR 0.00→0.9 on G3/G3A/L43
  { key: 'lead_lag', label: '🥇LEAD-in-LAG', cls: 'text-yellow-300 font-bold',
    custom: (r) => /🥇/.test((r.edges ?? []).join(' ')) },
  { key: 'lead_g3',  label: '🥇G3',  cls: 'text-yellow-200',
    custom: (r) => (r.edges ?? []).includes('🥇G3') },
  { key: 'lead_g3a', label: '🥇G3A', cls: 'text-yellow-200',
    custom: (r) => (r.edges ?? []).includes('🥇G3A') },
  { key: 'lead_l43', label: '🥇L43', cls: 'text-yellow-200',
    custom: (r) => (r.edges ?? []).includes('🥇L43') },
  { divider: true, label: '🟡 Seq-edges (2026-08-04)' },
  // the five user-built sequence setups — match on the row's EDGE chip list
  { key: 'seq_any',   label: '🟡SEQ*',  cls: 'text-amber-300 font-bold',
    custom: (r) => /🌉|🧲|🧺|👑/.test((r.edges ?? []).join(' ')) },
  { key: 'seq_crown', label: '👑Z1G',   cls: 'text-amber-300',
    custom: (r) => (r.edges ?? []).includes('👑Z1G🟡') },
  { key: 'seq_z1gt4', label: '🌉Z1G4',  cls: 'text-amber-200',
    custom: (r) => (r.edges ?? []).includes('🌉Z1G4🟡') },
  { key: 'seq_v2',    label: '🌉v2',    cls: 'text-amber-200',
    custom: (r) => (r.edges ?? []).includes('🌉v2🟡') },
  { key: 'seq_z9hl',  label: '🧲Z9HL',  cls: 'text-amber-200',
    custom: (r) => (r.edges ?? []).includes('🧲Z9HL🟡') },
  { key: 'seq_20',    label: '🧺SEQ',   cls: 'text-amber-200',
    custom: (r) => (r.edges ?? []).includes('🧺SEQ🟡') },
  { divider: true, label: '📐 MTF-EMA (15m·1h·4h DB)' },
  // ── 📐 MTF-EMA (DB-computed, 260704) — SMX/RGTI EMA-stack geometry from the
  //   real 15m·1h·4h bars (EOD daily snapshot), NOT the live single-bar flags above.
  //   r.mtf_ema_variants is attached from the /api/mtf-ema-scan map.
  // 📐 MTF-EMA chips: age-aware — match a variant that fired within the last N bars
  // (r.mtf_ema_ages[V] = trading days back; N=1 → today only). age 99 = never in window.
  { key: 'mtf_k0',     label: '📐K0',  cls: 'text-cyan-300 font-bold',
    custom: (r, N) => (r.mtf_ema_ages?.K0 ?? 99) < (N || 1) },
  { key: 'mtf_smx',    label: '📐SMX', cls: 'text-lime-300 font-bold',
    custom: (r, N) => (r.mtf_ema_ages?.SMX ?? 99) < (N || 1) },
  { key: 'mtf_orange', label: '📐ORG', cls: 'text-orange-300 font-bold',
    custom: (r, N) => (r.mtf_ema_ages?.ORANGE ?? 99) < (N || 1) },
  { key: 'mtf_up',     label: '📐UP',  cls: 'text-blue-300',
    custom: (r, N) => (r.mtf_ema_ages?.UP ?? 99) < (N || 1) },
  { key: 'mtf_upup',   label: '📐↑↑',  cls: 'text-indigo-300',
    custom: (r, N) => (r.mtf_ema_ages?.UPUP ?? 99) < (N || 1) },
  { key: 'mtf_upupup', label: '📐↑↑↑', cls: 'text-violet-300',
    custom: (r, N) => (r.mtf_ema_ages?.UPUPUP ?? 99) < (N || 1) },
  { key: 'mtf_ll',     label: '📐LL',  cls: 'text-purple-300',
    custom: (r, N) => (r.mtf_ema_ages?.LL ?? 99) < (N || 1) },
  { divider: true },
  // ── GOG Priority Engine (260501 FULL) ────────────────────────────────
  { key: 'akan_sig', label: 'A',    cls: 'text-orange-300 font-semibold'  },
  { key: 'smx_sig',  label: 'SM',   cls: 'text-lime-300 font-semibold'    },
  { key: 'nnn_sig',  label: 'N',    cls: 'text-cyan-300 font-semibold'    },
  { key: 'mx_sig',   label: 'MX',   cls: 'text-pink-300 font-semibold'    },
  { key: 'gog_sig',  label: 'GOG',  cls: 'text-fuchsia-300 font-semibold' },
  { key: 'gog_g1p',  label: 'G1P',  cls: 'text-green-300 font-bold'       },
  { key: 'gog_g2p',  label: 'G2P',  cls: 'text-green-400 font-bold'       },
  { key: 'gog_g3p',  label: 'G3P',  cls: 'text-green-500 font-bold'       },
  { key: 'gog_g1l',  label: 'G1L',  cls: 'text-emerald-300 font-semibold' },
  { key: 'gog_g2l',  label: 'G2L',  cls: 'text-emerald-400 font-semibold' },
  { key: 'gog_g3l',  label: 'G3L',  cls: 'text-emerald-500 font-semibold' },
  { key: 'gog_g1c',  label: 'G1C',  cls: 'text-teal-300 font-semibold'    },
  { key: 'gog_g2c',  label: 'G2C',  cls: 'text-teal-400 font-semibold'    },
  { key: 'gog_g3c',  label: 'G3C',  cls: 'text-teal-500 font-semibold'    },
  { key: '_gog_any_p', label: '★GOG+', cls: 'text-lime-300 font-bold',
    custom: r => !!(r.gog_g1p || r.gog_g2p || r.gog_g3p) },
  { key: '_not_ext', label: '!EXT', cls: 'text-sky-300',
    custom: r => !r.already_extended },
  { divider: true },
  // ── GOG Context Signals ───────────────────────────────────────────────
  { key: 'ctx_lds',  label: 'LDS',  cls: 'text-blue-300'    },
  { key: 'ctx_ldc',  label: 'LDC',  cls: 'text-blue-400'    },
  { key: 'ctx_ldp',  label: 'LDP',  cls: 'text-blue-200 font-semibold' },
  { key: 'ctx_lrc',  label: 'LRC',  cls: 'text-violet-300'  },
  { key: 'ctx_lrp',  label: 'LRP',  cls: 'text-violet-200 font-semibold' },
  { key: 'ctx_wrc',  label: 'WRC',  cls: 'text-indigo-300'  },
  { key: 'ctx_sqb',  label: 'SQB',  cls: 'text-cyan-300'    },
  { key: 'ctx_bct',  label: 'BCT',  cls: 'text-cyan-200 font-semibold' },
  { divider: true },
  // ── FLY 260424 — ABCD EMA DP ──────────────────────────────────────────
  { key: 'fly_abcd', label: 'ABCD', cls: 'text-lime-300 font-semibold' },
  { key: 'fly_cd',   label: 'CD',   cls: 'text-cyan-300'   },
  { key: 'fly_bd',   label: 'BD',   cls: 'text-blue-300'   },
  { key: 'fly_ad',   label: 'AD',   cls: 'text-violet-300' },
  { divider: true },
  // ── Context ───────────────────────────────────────────────────────────
  { key: '_rsi_os',    label: 'RSI≤35', cls: 'text-lime-300',
    custom: r => (r.rsi || 100) <= 35 },
  { key: '_rsi_ob',    label: 'RSI≥70', cls: 'text-red-400',
    custom: r => (r.rsi || 0) >= 70 },
  { key: '_yf_only',   label: 'yf',     cls: 'text-orange-400',
    custom: r => r.data_source === 'yfinance' },
  { divider: true },
  // ── Cross-engine diversity filters ────────────────────────────────────────
  { key: '_cross2',  label: '⚡×2+', cls: 'text-yellow-300',
    custom: r => engineFamilies(r).size >= 2 },
  { key: '_cross3',  label: '⚡×3+', cls: 'text-lime-300',
    custom: r => engineFamilies(r).size >= 3 },
  { key: '_cross4',  label: '⚡×4+', cls: 'text-lime-400',
    custom: r => engineFamilies(r).size >= 4 },
  { key: '_early',   label: '[E]',   cls: 'text-sky-300',
    custom: r => setupPhase(r) === 'Early' },
]

// V4_ALL_GROUPS — SIG_GROUPS plus V4_EXTRA_GROUPS (RANK/CONF/EDGES/SEQ/MTF/DIV — signal-like
// fields on the row that never became SIG_GROUPS filter chips). V4 scoring uses this merged
// catalog on BOTH surfaces so Ultra and Superchart score from the exact same signal list;
// SIG_GROUPS itself is untouched (still drives the filter-chip UI as before).
export const V4_ALL_GROUPS = [...SIG_GROUPS, ...V4_EXTRA_GROUPS]

// ── Live-only signal keys (no Studio-DB column) ──────────────────────────────
// These are computed only in the live scan path (turbo_engine / gog_engine) and
// have no enriched column in the DuckDB. In DB-instant mode they match ZERO rows,
// so the chips are HIDDEN there (see visibleSigGroups) to avoid clutter; they
// reappear in Live mode. Verified against ultra_db_scan.py passthrough/alias maps.
const LIVE_ONLY_SIGS = new Set([
  // 2809 combo
  'rtv', 'atr_brk', 'bb_brk', 'um_2809',
  // TZ transitional states
  'tz_attempt', 'tz_weak_bull',
  // CISD wick (only X2G lacks a DB column; X1/X2/X3/X1G are mapped)
  'x2g_wick',
  // Relative strength
  'rs_strong', 'rs',
  // RGTI multi-TF EMA
  'rgti_ll', 'rgti_up', 'rgti_upup', 'rgti_upupup',
  'rgti_orange', 'rgti_green', 'rgti_greencirc',
  // SMX / GOG priority engine
  'smx', 'akan_sig', 'smx_sig', 'nnn_sig', 'mx_sig', 'gog_sig', 'gog_g3l',
  // NOTE: mtf_* (📐 MTF-EMA) are intentionally NOT here — their variants are attached
  // client-side from /api/mtf-ema-scan (keyed by ticker), so they work in BOTH DB and
  // live modes. Adding them to LIVE_ONLY would wrongly hide the chips in DB-instant mode.
  // GOG context signals
  'ctx_lds', 'ctx_ldc', 'ctx_ldp', 'ctx_lrc', 'ctx_lrp', 'ctx_wrc', 'ctx_sqb', 'ctx_bct',
  // yfinance source flag — every DB row is enriched, so this matches nothing in DB mode
  '_yf_only',
])

// ── T/Z weight map (for display colour) ──────────────────────────────────────
const TZ_STRONG = new Set(['T4','T6','T1G','T2G'])
const TZ_BEAR   = new Set(['Z4','Z6','Z1G','Z2G','Z1','Z2','Z3','Z5','Z7','Z9','Z10','Z11','Z12'])

// ── Colour helpers ────────────────────────────────────────────────────────────
function scoreColor(s) {
  if (s >= 65) return 'text-lime-300 font-bold'
  if (s >= 50) return 'text-yellow-300 font-semibold'
  if (s >= 35) return 'text-blue-300'
  if (s >= 20) return 'text-md-on-surface'
  return 'text-md-on-surface-var/70'
}

function scoreBg(s) {
  if (s >= 65) return 'bg-lime-900/25'
  if (s >= 50) return 'bg-yellow-900/15'
  if (s >= 35) return 'bg-blue-900/10'
  return ''
}

// ── ULTRA Score colour helper (replay v2 calibration) ───────────────────────
// 90+ A+ HIGH_PRIORITY → strongest visual treatment (replay edge concentrated
// here). 80–89 A WATCH_A. 65–79 B STRONG_WATCH. 50–64 C CONTEXT_WATCH. <50 D LOW.
function ultraScoreCls(s) {
  if (s == null) return 'text-gray-700'
  if (s >= 90) return 'text-emerald-200 font-extrabold drop-shadow-[0_0_3px_rgba(110,231,183,0.45)]'
  if (s >= 80) return 'text-emerald-300 font-bold'
  if (s >= 65) return 'text-teal-300 font-semibold'
  if (s >= 50) return 'text-yellow-200/90'
  return 'text-md-on-surface-var'
}

// v2 band/priority labels — UI prefers v2 over old A/B/C/D when present.
function ultraBandV2Label(s, fallback) {
  if (s == null) return fallback || ''
  if (s >= 90) return 'A+'
  if (s >= 80) return 'A'
  if (s >= 65) return 'B'
  if (s >= 50) return 'C'
  return 'D'
}
function ultraPriorityLabel(s) {
  if (s == null) return ''
  if (s >= 90) return 'HIGH_PRIORITY'
  if (s >= 80) return 'WATCH_A'
  if (s >= 65) return 'STRONG_WATCH'
  if (s >= 50) return 'CONTEXT_WATCH'
  return 'LOW'
}

function betaZoneCls(zone) {
  switch (zone) {
    case 'ELITE':       return 'text-amber-200 font-bold'
    case 'OPTIMAL':     return 'text-emerald-300 font-bold'
    case 'BUY':         return 'text-blue-300 font-semibold'
    case 'WATCH':       return 'text-violet-300'
    case 'BUILDING':    return 'text-yellow-400'
    case 'EXTENDED':    return 'text-amber-400'
    case 'SHORT_WATCH': return 'text-red-400'
    default:            return 'text-md-on-surface-var/70'
  }
}

// ── Compact-label maps for Pullback / Rare Reversal cells (and CSV) ──────────
const PULLBACK_COMPACT = {
  ANECDOTAL_PULLBACK: 'APB',
  CONFIRMED_PULLBACK: 'CPB',
  READY_PULLBACK:     'RPB',
  FORMING_PULLBACK:   'FPB',
  GO_PULLBACK:        'GPB',
  WATCH_PULLBACK:     'WPB',
}
function pullbackCompact(p) {
  if (!p) return ''
  const tier = (p.evidence_tier || '').toUpperCase()
  if (PULLBACK_COMPACT[tier]) return PULLBACK_COMPACT[tier]
  // Fallback: first letter of each underscore-separated token
  return tier.split('_').map(t => t.slice(0, 1)).join('')
}

const RARE_COMPACT = {
  FORMING_PATTERN:   'FP',
  CONFIRMED_PATTERN: 'CP',
  CONFIRMED_RARE:    'CP',
  READY_PATTERN:     'RP',
  ACTIVE_PATTERN:    'AP',
  WATCH_PATTERN:     'WP',
  ANECDOTAL_RARE:    'AR',
}
function rareCompact(r) {
  if (!r) return ''
  const tier = (r.evidence_tier || '').toUpperCase()
  if (RARE_COMPACT[tier]) return RARE_COMPACT[tier]
  return tier.split('_').map(t => t.slice(0, 1)).join('')
}

// ── Badge component ───────────────────────────────────────────────────────────
function Badge({ label, cls }) {
  return <span className={`px-1 rounded text-[10px] leading-tight ${cls}`}>{label}</span>
}

// ── fmt helper ────────────────────────────────────────────────────────────────
const fmt = (v, d = 2) => v == null ? '—' : Number(v).toFixed(d)

// ── GOG tier badge colour ─────────────────────────────────────────────────────
function gogTierCls(tier) {
  if (!tier) return ''
  if (tier === 'G1P' || tier === 'G2P' || tier === 'G3P')
    return 'bg-green-800 text-green-100 ring-1 ring-green-400 font-bold'
  if (tier === 'G1L' || tier === 'G2L' || tier === 'G3L')
    return 'bg-emerald-800 text-emerald-100 ring-1 ring-emerald-400'
  if (tier === 'G1C' || tier === 'G2C' || tier === 'G3C')
    return 'bg-teal-800 text-teal-100 ring-1 ring-teal-400'
  return 'bg-fuchsia-800 text-fuchsia-100 ring-1 ring-fuchsia-400'
}

// ── Context token colour ───────────────────────────────────────────────────────
function ctxTokCls(tok) {
  if (tok === 'LDP' || tok === 'LRP') return 'bg-green-900 text-green-200 font-semibold'
  if (tok === 'LDC' || tok === 'LRC') return 'bg-teal-900 text-teal-200'
  if (tok === 'LDS' || tok === 'LD')  return 'bg-cyan-900 text-cyan-300'
  if (tok === 'BCT')                  return 'bg-blue-900 text-blue-200 font-semibold'
  if (tok === 'SQB')                  return 'bg-blue-900 text-blue-300'
  if (tok === 'WRC' || tok === 'F8C') return 'bg-slate-700 text-slate-200'
  return 'bg-md-surface-high text-md-on-surface-var'
}

// ── Active context tokens from a turbo row (priority order) ───────────────────
const CTX_PRIO = [
  ['ctx_ldp','LDP'],['ctx_lrp','LRP'],
  ['ctx_ldc','LDC'],['ctx_lrc','LRC'],
  ['ctx_lds','LDS'],['ctx_ld','LD'],
  ['ctx_bct','BCT'],['ctx_sqb','SQB'],
  ['ctx_wrc','WRC'],['ctx_f8c','F8C'],['ctx_svs','SVS'],
]
function ctxTokens(r) {
  return CTX_PRIO.filter(([k]) => r[k]).map(([, t]) => t)
}

// ── SIGNAL_SCORE chip colour ───────────────────────────────────────────────────
function scoreCls(n) {
  if (n >= 120) return 'text-yellow-300 font-bold'
  if (n >= 100) return 'text-lime-300 font-bold'
  if (n >= 80)  return 'text-green-300 font-semibold'
  if (n >= 60)  return 'text-teal-300'
  return 'text-md-on-surface-var'
}

// ── Engine family membership (for cross-engine diversity scoring) ─────────────
// Each set represents a truly independent engine/observation model.
// Signals from different sets = orthogonal evidence.
// Signals from the same set = same-theme cluster (high conviction, lower diversity).
function engineFamilies(r) {
  const fams = new Set()
  // VABS (volume/accumulation state machine)
  if (r.best_sig || r.strong_sig || r.vbo_up || r.abs_sig || r.ns || r.sq || r.load_sig || r.va)
    fams.add('Vol')
  // Delta / order-flow (260403)
  if (r.d_spring || r.d_strong_bull || r.d_absorb_bull || r.d_blast_bull || r.d_surge_bull || r.d_flip_bull || r.d_orange_bull)
    fams.add('Δ')
  // T/Z candlestick state engine
  if (r.tz_sig || r.tz_bull_flip || r.tz_attempt)
    fams.add('T/Z')
  // WLNBB / L-structure / EMA-cross
  if (r.fri34 || r.fri43 || r.l34 || r.preup66 || r.preup55 || r.preup3 || r.preup2 || r.preup50)
    fams.add('L')
  // Combo/2809 (multi-condition composites)
  if (r.rocket || r.buy_2809 || r.seq_bcont)
    fams.add('Cmb')
  // Breakout / ULTRA / RS
  if (r.fbo_bull || r.eb_bull || r.rs_strong || r.ultra_3up)
    fams.add('Brk')
  return fams
}

// ── Setup timing phase ────────────────────────────────────────────────────────
// Early = phase C / fresh regime flip  (best risk/reward, needs more confirmation)
// Mid   = LPS / T4 / EMA cross        (structure confirmed, still actionable)
// Late  = confirmed breakout combo     (high conviction but entry may be extended)
function setupPhase(r) {
  const early = r.d_spring || r.tz_bull_flip
  const late  = (r.rocket || r.buy_2809) && (r.fbo_bull || r.eb_bull || r.vbo_up)
  if (early && !late) return 'Early'
  if (late)           return 'Late'
  return null  // mid / neutral — don't label
}

// ── Score reason — grouped by family, shown as tooltip + inline line ─────────
function scoreReason(r) {
  const parts = []

  // Vol/VABS family (VABS + Wyckoff share Vol cap but are sub-families)
  const vol = []
  if (r.best_sig)  vol.push('BEST★')
  else if (r.strong_sig) vol.push('STR')
  if (r.vbo_up)    vol.push('VBO↑')
  if (r.ns)        vol.push('NS')
  else if (r.sq)   vol.push('SQ')
  if (r.load_sig)  vol.push('LD')
  if (r.va)        vol.push('VA')
  if (r.sig_l88)   vol.push('L88')
  if (vol.length)  parts.push(`Vol:${vol.join('+')}`)

  // Breakout family
  const brk = []
  if (r.fbo_bull)   brk.push('FBO↑')
  if (r.eb_bull)    brk.push('EB↑')
  if (r.rs_strong)  brk.push('RS+')
  else if (r.rs)    brk.push('RS')
  if (r.ultra_3up)  brk.push('3↑')
  if (brk.length)   parts.push(`Brk:${brk.join('+')}`)

  // Combo family
  const cmb = []
  if (r.rocket)    cmb.push('🚀')
  else if (r.buy_2809) cmb.push('BUY')
  if (r.cd)        cmb.push('CD')
  else if (r.ca)   cmb.push('CA')
  else if (r.cw)   cmb.push('CW')
  if (r.seq_bcont) cmb.push('SBC')
  if (cmb.length)  parts.push(`Cmb:${cmb.join('+')}`)

  // Trend/TZ family
  const trd = []
  if (r.tz_sig)        trd.push(r.tz_sig)
  if (r.tz_bull_flip)  trd.push('TZ→3')
  else if (r.tz_attempt) trd.push('TZ→2')
  if (r.fri34)         trd.push('FRI34')
  else if (r.fri43)    trd.push('FRI43')
  else if (r.l34)      trd.push('L34')
  if (r.preup66)        trd.push('P66')
  else if (r.preup55)   trd.push('P55')
  else if (r.preup89)   trd.push('P89')
  else if (r.preup3)    trd.push('P3')
  else if (r.preup2)    trd.push('P2')
  else if (r.preup50)   trd.push('P50')
  if (trd.length)      parts.push(`Trd:${trd.join('+')}`)

  // Delta family
  const dlt = []
  if (r.d_spring)      dlt.push('dSPR')
  if (r.d_blast_bull)  dlt.push('ΔΔ↑')
  else if (r.d_surge_bull) dlt.push('Δ↑')
  if (r.d_strong_bull) dlt.push('B/S↑')
  if (r.d_absorb_bull) dlt.push('Ab↑')
  if (dlt.length)      parts.push(`Δ:${dlt.join('+')}`)

  // B/G signals (show first 4)
  const bg = []
  for (let i = 1; i <= 11; i++) { if (r[`b${i}`]) bg.push(`B${i}`) }
  for (const k of ['g1','g2','g4','g6','g11']) { if (r[k]) bg.push(k.toUpperCase()) }
  if (bg.length) parts.push(bg.slice(0, 4).join('+'))

  const sc = r.turbo_score ?? 0
  const tier = sc >= 65 ? '🔥' : sc >= 50 ? '★' : sc >= 35 ? '▲' : ''

  // Cross-engine diversity indicator — how many independent engine families fired
  const n = engineFamilies(r).size
  const cross = n >= 4 ? '⚡×4' : n === 3 ? '⚡×3' : n === 2 ? '⚡×2' : ''

  // Timing phase (Early setup vs Late/confirmed)
  const phase = setupPhase(r)
  const phaseTag = phase === 'Early' ? ' [E]' : phase === 'Late' ? ' [L]' : ''

  return parts.length
    ? `${tier}${cross ? ' ' + cross : ''} ${parts.join(' | ')}${phaseTag} → ${sc.toFixed(1)}`
    : `score ${sc.toFixed(1)}`
}

// ── Mini chart popup ──────────────────────────────────────────────────────────
function MiniChartPopup({ row, tf, pos, onClose }) {
  const [info, setInfo] = useState(null)

  useEffect(() => {
    api.tickerInfo(row.ticker).then(setInfo).catch(() => {})
  }, [row.ticker])

  const CHART_W = 780
  const CHART_H = 380

  // position: below the row, right side; flip left if too close to edge
  const POPUP_W = 820
  const POPUP_H = 520
  const vw = window.innerWidth
  const vh = window.innerHeight
  let left = pos.x + 16
  if (left + POPUP_W > vw - 8) left = pos.x - POPUP_W - 8
  let top = pos.y + 20   // start below the row center (≈ row bottom + gap)
  if (top + POPUP_H > vh - 8) top = vh - POPUP_H - 8
  if (top < 8) top = 8

  const chg = row.change_pct ?? 0

  return (
    <div
      className="fixed z-50 bg-md-surface-con border border-md-outline-var rounded-lg shadow-2xl text-xs text-md-on-surface pointer-events-none"
      style={{ left, top, width: POPUP_W }}
    >
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2.5 border-b border-white/[0.07]">
        <div className="flex items-center gap-3 min-w-0">
          <span className="font-mono font-bold text-blue-300 text-base shrink-0">{row.ticker}</span>
          {row.vol_bucket && <span className="text-md-on-surface-var text-sm shrink-0">{row.vol_bucket}</span>}
          {row.tz_sig && (
            <span className={`font-mono font-semibold text-sm shrink-0 ${TZ_STRONG.has(row.tz_sig) ? 'text-lime-300' : TZ_BEAR.has(row.tz_sig) ? 'text-red-400' : 'text-blue-300'}`}>
              {row.tz_sig}
            </span>
          )}
          {info?.name && info.name !== row.ticker && (
            <span className="text-md-on-surface text-sm truncate">{info.name}</span>
          )}
          {info?.sector && (
            <span className="text-md-on-surface-var text-xs shrink-0 bg-md-surface-high px-1.5 py-0.5 rounded">{info.sector}</span>
          )}
        </div>
        <div className="text-right shrink-0 ml-3">
          <span className="font-mono text-md-on-surface text-base">${fmt(row.last_price)}</span>
          <span className={`ml-2 font-mono text-sm ${chg >= 0 ? 'text-lime-400' : 'text-red-400'}`}>
            {chg >= 0 ? '+' : ''}{fmt(chg)}%
          </span>
        </div>
      </div>

      {/* Stats row */}
      <div className="flex items-center gap-4 px-4 py-2 border-b border-white/[0.07] text-md-on-surface-var">
        <span>RSI <span className={row.rsi <= 35 ? 'text-lime-400' : row.rsi >= 70 ? 'text-red-400' : 'text-md-on-surface'}>{fmt(row.rsi, 0)}</span></span>
        <span>CCI <span className={row.cci >= 100 ? 'text-lime-400' : row.cci <= -100 ? 'text-red-400' : 'text-md-on-surface'}>{fmt(row.cci, 0)}</span></span>
        <span>Score <span className={`font-semibold ${scoreColor(row.turbo_score ?? 0)}`}>{fmt(row.turbo_score, 1)}</span></span>
        {row.avg_vol > 0 && (
          <span className="ml-auto">
            {row.avg_vol >= 1_000_000 ? `${(row.avg_vol/1_000_000).toFixed(1)}M` : row.avg_vol >= 1_000 ? `${Math.round(row.avg_vol/1_000)}K` : Math.round(row.avg_vol)}
          </span>
        )}
      </div>

      {/* Chart */}
      <div style={{ width: CHART_W }}>
        {/* lbal={false}: the hover preview stays a clean candle chart (user, 2026-09-06) */}
        <CodeCandleChart bare codes={false} lbal={false} lvx={false} ovd={false} vol7={false} ticker={row.ticker} tf={tf} interactive={false} height={CHART_H} />
      </div>

      {/* Signal summary */}
      <div className="px-4 py-2 text-md-on-surface-var text-xs truncate border-t border-md-outline-var">
        {scoreReason(row)}
      </div>
    </div>
  )
}

// ── ULTRA scan localStorage cache (separate keyspace from Turbo) ─────────────
// Cache version bump invalidates ALL cached entries that pre-date the bump.
// Increment this when row schema changes (new enrichment columns added) so
// stale caches without the new fields don't survive a redeploy.
const _CACHE_VERSION = '260622_v4.6'  // bumped: vol3rise T5/T9/T12 enrichment filters added

const _tsKey  = (tf, uni) => `sachoki_ultra_${tf}_${uni}`
const _tsGet  = (tf, uni) => {
  try {
    const raw = JSON.parse(localStorage.getItem(_tsKey(tf, uni)) || 'null')
    if (!raw) return null
    // Reject entries written before the current cache version
    if (raw._cv !== _CACHE_VERSION) {
      localStorage.removeItem(_tsKey(tf, uni))
      return null
    }
    return raw
  } catch { return null }
}

const _ALL_TF  = ['1d', '4h', '1h', '30m', '15m', '1wk']
const _ALL_UNI = ['sp500', 'nasdaq', 'russell2k', 'all_us', 'split', 'index']

// Map a selected universe → the backend universe list. Single source of truth so the
// DB and Preview fetch paths never drift (2026-07-07 fix: russell2k/all_us were missing
// from fetchFromDB → "Russell 2K" and "🌐 All US" silently fetched only sp500+nasdaq).
const _uniList = (u) =>
    u === 'sp500'     ? ['sp500']
  : u === 'index'     ? ['index']
  : u === 'nasdaq'    ? ['nasdaq']
  : u === 'russell2k' ? ['russell2k']
  : u === 'all_us'    ? ['sp500', 'nasdaq', 'russell2k']
  : u === 'split'     ? ['split']   // backend cross-filters to the live split window
  : u === 'zone'      ? ['zone']    // backend filters to tickers currently in a zone
  : ['sp500', 'nasdaq']             // 'all' / default

// Evict ALL cached entries except the one being written
function _evictAll(exceptKey) {
  for (const tf of _ALL_TF) for (const uni of _ALL_UNI) {
    const key = _tsKey(tf, uni)
    if (key === exceptKey) continue
    try { localStorage.removeItem(key) } catch {}
  }
}

// Keep only fields needed for display — omit 0-valued booleans to save space
// sig_ages handled separately: only keep entries with age < 15 to cut size
const KEEP_ALWAYS = new Set([
  'ticker','turbo_score','turbo_score_n3','turbo_score_n5','turbo_score_n10',
  'buy_score','buy_tag',
  'tz_sig','tz_bull','last_price','change_pct','rsi','cci','avg_vol',
  'vol_bucket','data_source',
  'ema20','ema50','ema89','ema200',
  'sector',
  // ULTRA / Beta / Bull scores — non-1 numeric values that the slimmer would
  // otherwise drop, leaving the table cells showing "—".
  'ultra_score','ultra_score_band','ultra_score_band_v2','ultra_score_priority',
  'ultra_score_reasons','ultra_score_flags','ultra_score_raw_before_penalty',
  'ultra_score_penalty_total','ultra_score_regime_bonus',
  'ultra_score_caps_applied','ultra_score_cap_reason',
  // v3 reweighted ranker (2026-07-18) — alongside the old score, non-destructive
  'ultra_score_v3','ultra_score_v3_band','ultra_score_v3_reasons',
  // 🎲 score-hits (2026-07-27) — agreement count across the 6 rankers' own good zones
  'score_hits','score_hits_which',
  // 📐 divergence × 🏆RS (2026-07-28) — bars-ago of the freshest fire (blank = none in 5 bars)
  'div_buy','div_deep','div_top','div_rsi_lo','div_rsi_hi',
  // validated zone buy-flags (2026-07-18)
  'buy_flag','rev_buy','brk_buy','mtf_echo','mtf_score_conf','turn_echo_n','h4_rev_today','h1_rev_today','heavy_l','edges','edge_n','edge_rev','rank_pct','rank_edge','rank_fam','rank_n','seq34','seq_ctx','conf_score','conf_top','conf_ext','conf_ext_top',
  'atr_pct','tt10','tt10_hit','ttdn10','no_vol_event',
  'beta_score','beta_zone','beta_auto_buy',
  'final_bull_score','final_regime',
  'gog_score','gog_tier',
  'profile_score','profile_category','sweet_spot_active','late_warning',
  'scan_date','tz_state','tz_sig_t','tz_sig_z',
  // RTB v4
  'rtb_build','rtb_turn','rtb_ready','rtb_bonus3',
  'rtb_late','rtb_total','rtb_phase','rtb_transition','rtb_phase_age',
  // 260523 — Advanced filter fields (MUST be kept so client-side filter works after cache reload)
  'ad_fresh','ad_cluster',
  'wyc_phase','wyc_spring','wyc_sos','wyc_acc_tr','wyc_markup','wyc_in_tr','wyc_sow',
  'swing_type','swing_ret_from_prev','fwd_swing_ret','fwd_swing_bars',
  'is_pivot_high','is_pivot_low',
  'prebreak_prime','prebreak_ready','prebreak_watch','prebreak_score',
  'pb_lvbo','pb_stop_cause','pb_pp_rtv','pb_fly_cd_c',
  'pb_wvf_confirm','pb_follow_confirm','pb_macro_penalty',
  // Capit→Atomic confluence (boolean — needed for the 🔥Capit→Atom filter after cache reload)
  'atomic_post_capit','atomic_capit_age',
  // SHAPE × CONTEXT (2026-09-10) — booleans and strings, so `v === 1` would drop them all and
  // every shape chip would stop filtering after a cache reload. Descriptive only.
  'shape','shape_code','shape_arrow','shape_label','shape_grade','shape_vr','shape_absorb',
  'shape_dry','shape_sweet','shape_pos20','shape_floor','shape_touches','shape_key','shape_rs',
  'shape_rsi','shape_band','shape_knife','shape_veto','shape_lstup_veto','shape_dir_up',
  'shape_dir_dn','shape_can_dir','shape_cl_fam','shape_cl_bars','shape_by_fam','shape_by_den',
  'shape_mark','shape_legs',
  // PRICE × VOLUME (2026-09-22) — strings + booleans, same reason as SHAPE: without this every
  // PV chip would stop filtering after a cache reload. Descriptive only.
  'pv','pv_c','pv_o','pv_text','pv_agree','pv_veto_c','pv_veto_o',
  'pv_c_div','pv_c_upp','pv_c_upr','pv_c_rev','pv_c_rup','pv_c_vup','pv_c_turn','pv_c_up4','pv_c_re2',
  'pv_o_div','pv_o_upp','pv_o_upr','pv_o_rev','pv_o_rup','pv_o_vup','pv_o_turn','pv_o_up4','pv_o_re2',
  // VOL ECHO (2026-09-27) — booleans + ints; same reason: without this the chips stop filtering
  // after a cache reload. Descriptive only.
  've','ve_text','ve_echo_col','ve_qr','ve_spike','ve_spk','ve_echo','ve_q','ve_r','ve_rel_up','ve_rel_dn',
  've_bo','ve_bd','ve_bov','ve_bdv','ve_zone_armed','ve_qr_rel_veto',
  've_echo_gap','ve_echo_nmatch','ve_echo_hits','ve_qn','ve_rn','ve_rel_n','ve_rel_q','ve_bo_age',
  've_zone_pos','ve_echo_vr','ve_bov_x','ve_zone_top','ve_zone_bot',
  // TURN·58 + ⟲ROW (2026-09-28) — booleans + ints; without this the chips stop filtering after a cache reload.
  'turn58_cand','turn58_n','turn_ge20','turn_ge26','turn_ge30','rs_pair','rs_nconf','rs_nearly','rs_text','rs_c2','rs_c3','rs_fly','rs_fly_c','rs_fly_e','rs_gr','rs_gr_c','rs_gr_e','rs_mtf','rs_mtf_c','rs_mtf_e','rs_phys','rs_phys_c','rs_phys_e','rs_vol7','rs_vol7_c','rs_vol7_e','rs_pv','rs_pv_c','rs_pv_e','rs_break','rs_break_c','rs_break_e','rs_ovd','rs_ovd_c','rs_ovd_e','rs_delta','rs_delta_c','rs_delta_e',
])
function _slimRow(r) {
  const out = {}
  for (const [k, v] of Object.entries(r)) {
    if (k.startsWith('_') || k === 'scan_id') continue
    if (k === 'sig_ages') {
      if (v) {
        try {
          const ages = JSON.parse(v)
          const compact = {}
          for (const [sk, sv] of Object.entries(ages)) {
            if (sv < 15) compact[sk] = sv
          }
          if (Object.keys(compact).length > 0) out[k] = JSON.stringify(compact)
        } catch {}
      }
      continue
    }
    if (KEEP_ALWAYS.has(k)) { if (v != null) out[k] = v; continue }
    if (v === 1) out[k] = 1  // only store truthy signal flags
  }
  return out
}

function ultraCacheSet(tf, uni, results, lastScan) {
  if (getCacheBackend() === 'idb') {
    // IndexedDB: no size limit, store full results without slimming
    idbSet(tf, uni, results, lastScan)
    return
  }
  // localStorage path (D+A): slim rows + truncation fallback
  const key = _tsKey(tf, uni)
  const payload = { _cv: _CACHE_VERSION, results: results.map(_slimRow), lastScan }
  const write = (p) => localStorage.setItem(key, JSON.stringify(p))
  try {
    write(payload)
  } catch {
    _evictAll(key)  // free ALL space, retry
    try {
      write(payload)
    } catch {
      // last resort: top 1000 by score
      try {
        const top = [...results].sort((a,b) => (b.turbo_score??0)-(a.turbo_score??0)).slice(0,1000)
        write({ results: top.map(_slimRow), lastScan, truncated: true })
      } catch {}
    }
  }
}

const _tsSet = ultraCacheSet

// ─────────────────────────────────────────────────────────────────────────────
const _initTf  = () => { try { return localStorage.getItem('sachoki_ultra_tf')  || '1d'    } catch { return '1d'    } }
const _initUni = () => { try { return localStorage.getItem('sachoki_ultra_uni') || 'sp500' } catch { return 'sp500' } }

function StarBtn({ ticker, tf, onToggle }) {
  const [saved, setSaved] = useState(() => pwlHas(ticker, tf))
  return (
    <button
      title={saved ? 'Remove from watchlist' : 'Save to watchlist'}
      className={`text-sm transition-colors ${saved ? 'text-yellow-400' : 'text-gray-700 hover:text-yellow-400'}`}
      onClick={e => {
        e.stopPropagation()
        onToggle?.()
        setSaved(s => !s)
      }}>
      ★
    </button>
  )
}

export default function UltraScanPanel({ onSelectTicker }) {
  // The ★ L-BAL chips are now fixed pairs (★V / ★a …), so this panel no longer follows the shared
  // switch and does not need to re-filter when it changes.
  const [localTf,    setLocalTf]    = useState(_initTf)
  const [universe,   setUniverse]   = useState(_initUni)
  const [allResults, setAllResults] = useState(() => { const tf = _initTf(); const uni = _initUni(); return _tsGet(tf, uni)?.results || [] })
  const [lastScan,   setLastScan]   = useState(() => { const tf = _initTf(); const uni = _initUni(); return _tsGet(tf, uni)?.lastScan || null })
  const [scanning,   setScanning]   = useState(false)
  const [error,      setError]      = useState(null)
  const pollIvRef   = useRef(null)   // interval handle — prevents duplicate polls
  const fetchSeqRef = useRef(0)      // monotonic counter — discard stale fetches
  const [massiveReady, setMassiveReady] = useState(null)
  const [scoreBands, setScoreBands] = useState(new Set(['all']))
  const [direction,  setDirection]  = useState('bull')
  const [secFilter,  setSecFilter]  = useState('')    // '' = all sectors
  const [sectorMap,  setSectorMap]  = useState({})    // { TICKER: sector_string }
  const [mtfEmaMap,  setMtfEmaMap]  = useState({})    // { TICKER: [variants] } from /api/mtf-ema-scan
  const [mtfEmaAgeMap, setMtfEmaAgeMap] = useState({}) // { TICKER: {variant: age_days} } — age-aware N filtering
  const [selSigs,    setSelSigs]    = useState(new Set())   // AND filter
  // ⛓ SEQUENCE  [260815] — three per-bar filter sets, oldest first: bar−2, bar−1, today.
  //
  // The N lookback asks "did this fire ANYWHERE in the last N bars"; a sequence asks
  // "was it THIS then THAT then THIS". N cannot express order, and order is most of
  // what a setup is.
  //
  // seqTarget routes a chip click: 'main' keeps the existing behaviour, 0/1/2 put the
  // chip in that bar's set. The chip grid is not duplicated — every filter that exists
  // gains a per-bar form because the same predicate is run against seq3[k] instead of
  // against the row.
  const SEQ_BARS = 5
  const [seqSlots,   setSeqSlots]   = useState(() => Array.from({length: 5}, () => new Set()))
  const [seqTarget,  setSeqTarget]  = useState('main')
  // derived here, NOT below the filter memo: useMemo's factory runs during render, so a
  // const declared later is still in its temporal dead zone when the memo reads it.
  const seqActive = seqSlots.some(x => x.size > 0)
  const clearSeq  = () => { setSeqSlots(Array.from({length: SEQ_BARS}, () => new Set())); setSeqTarget('main') }
  const [rtbPhase,    setRtbPhase]    = useState('')      // '' = all phases
  const [exported,   setExported]   = useState(false)
  const [tvExported, setTvExported] = useState(false)
  const [sortBy,     setSortBy]     = useState('turbo_score')
  const [sortDir,    setSortDir]    = useState('desc')
  const [gexTick,    setGexTick]    = useState(0)   // bumps as 💠 GEX data streams in → re-sorts
  useEffect(() => subscribeGex(() => setGexTick(t => t + 1)), [])
  const [anatTick,   setAnatTick]   = useState(0)   // bumps when ▽△ anatomy map loads → re-sorts
  useEffect(() => { requestAnatomy(); return subscribeAnatomy(() => setAnatTick(t => t + 1)) }, [])

  // ── Pre-market cache: { TICKER: { pm_price, pm_chg_pct, pm_vol } } ────────
  const [pmData,    setPmData]    = useState({})
  const pmTimerRef = useRef(null)
  const [lookbackN,  setLookbackN]  = useState(1)
  const [pickedTickers, setPickedTickers] = useState(new Set())  // individually selected rows
  // Source mode: 'live' = traditional 30-60min scan; 'db' = instant Studio DB lookup
  const [sourceMode, setSourceMode] = useState(() => {
    try { return localStorage.getItem('ultra_source_mode') || 'db' } catch { return 'db' }
  })
  const switchSource = (mode) => {
    setSourceMode(mode)
    try { localStorage.setItem('ultra_source_mode', mode) } catch {}
    // Drop any selected live-only chips when entering DB mode — they have no DB
    // column and would otherwise silently filter every row away.
    if (mode === 'db') {
      setSelSigs(prev => {
        const next = new Set([...prev].filter(k => !LIVE_ONLY_SIGS.has(k)))
        return next.size === prev.size ? prev : next
      })
    }
  }

  const _pwlToggle = (row) => {
    const r = { ...row, _tf: localTf }
    if (pwlHas(row.ticker, localTf)) { pwlRemove(row.ticker, localTf) }
    else { pwlAdd(r) }
  }
  const [sweetSpotFilter, setSweetSpotFilter] = useState(false)
  const [buyFilter, setBuyFilter] = useState(() => new Set())  // multi-select AND: rev/brk/conf/turn/h4/any/veto (2026-07-19)
  // ▽△ Bottom-Anatomy filter (2026-09-09) — the ▽△ column was sortable but had no filter.
  // Verdict chips (durable/struct/shake/cont) are OR'd (one bar has ONE verdict); 's5' ANDs on top.
  const [anatFilter, setAnatFilter] = useState(() => new Set())
  const [buildingFilter,  setBuildingFilter]  = useState(false)
  const [watchFilter,     setWatchFilter]     = useState(false)
  // HV-Zone re-test filter (3 vol-spike tiers, multi-select; union of selected sets)
  const [zoneTiers, setZoneTiers] = useState({ t25: false, t510: false, t10p: false })
  const [zoneTierSets, setZoneTierSets] = useState({}) // {tierKey: Set<string>}
  const [zoneRetestBusy, setZoneRetestBusy] = useState(false)
  // Gann zone filter (current close inside top or bottom Gann zone over a lookback window)
  const [gannFilter, setGannFilter] = useState(false)
  const [gannSet, setGannSet] = useState(null)
  const [gannBusy, setGannBusy] = useState(false)
  // Volume-class filter — current bar's TZ_WLNBB bucket is VB and/or W (union).
  // Pure client-side on existing row data (vol_bucket), no fetch needed.
  const [vbwFilter, setVbwFilter] = useState({ vb: false, w: false })
  const [atomicFilter, setAtomicFilter] = useState(false)   // ⚛ weak-close gap-up (atomic edge)
  const [shortFilter,  setShortFilter]  = useState(false)   // ⚡ blow-off fade (short edge)
  const [capFilter,    setCapFilter]    = useState(false)   // 💥 capitulation bounce (long edge)
  const [momFilter,    setMomFilter]    = useState(false)   // 🚀 momentum zone-dense (long edge)
  const [postCapitFilter, setPostCapitFilter] = useState(false)  // 🔥 Capit→Atom confluence (premium atomic subset)
  // (2026-08-04) tzt4 / ttt6 / t1seq / t3seq / t9rsi / z1gt2g filters REMOVED —
  // path-sim audit: every variant negative-median and <4/6 positive years.
  const [vol3t5Filter,  setVol3t5Filter]  = useState(false)  // 📈 T5 + vol↑↑↑ + RSI drop 2-10pt
  const [vol3t9Filter,  setVol3t9Filter]  = useState(false)  // 📈 T9 + vol↑↑↑ + RSI 25-40
  const [vol3t12Filter, setVol3t12Filter] = useState(false)  // 📈 T12 + vol↑↑↑ + RSI drop 2-10pt
  const ZONE_TIERS = [
    { key: 't25',  label: 'x2–5',  vmin: 2,  vmax: 5,  color: 'sky' },
    { key: 't510', label: 'x5–10', vmin: 5,  vmax: 10, color: 'teal' },
    { key: 't10p', label: 'x10+',  vmin: 10, vmax: null, color: 'cyan' },
  ]
  // union of all enabled tiers; null while nothing toggled OR any toggled tier not yet loaded
  const activeZoneSet = (() => {
    const enabled = ZONE_TIERS.filter(t => zoneTiers[t.key])
    if (!enabled.length) return null
    if (enabled.some(t => !zoneTierSets[t.key])) return null   // still loading some tier
    const u = new Set()
    enabled.forEach(t => zoneTierSets[t.key].forEach(x => u.add(x)))
    return u
  })()
  const zoneFilterActive = ZONE_TIERS.some(t => zoneTiers[t.key])
  const [partialDay,  setPartialDay]  = useState(false)  // include today's in-progress bar
  const [previewing,  setPreviewing]  = useState(false)  // hybrid Preview scan in flight
  const [previewInfo, setPreviewInfo] = useState(null)   // {session, note, liveBars} or null
  const [volMin,      setVolMin]      = useState(100_000) // min avg daily volume filter
  const [volMax,      setVolMax]      = useState(0)       // max avg daily volume (0 = no cap)
  const [priceMin,    setPriceMin]    = useState('')      // min last price ('' = off) — $21-89 = quality zone
  const [priceMax,    setPriceMax]    = useState('')      // max last price ('' = off)
  const [hoverPopup,  setHoverPopup]  = useState(null)   // { row, pos }
  const [expandedRows, setExpandedRows] = useState(new Set())  // tickers with open sub-row
  const [showAdvanced, setShowAdvanced] = useState(false)      // collapsible adv filters

  // ── 260523 signal filters (Pine 260523: AD-FRESH / AD-CLUSTER / WYC Phase) ──
  const [adFreshFilter,   setAdFreshFilter]   = useState(null)   // null | true
  const [adClusterFilter, setAdClusterFilter] = useState(null)   // null | true
  const [wycPhaseFilter,  setWycPhaseFilter]  = useState('')     // '' = all
  // 260523 v3.1 — swing context filter (HH/LH/HL/LL/pivot)
  const [swingTypeFilter, setSwingTypeFilter] = useState('')     // '' = all
  // 260523 v3.5 — PREBREAK + WYC additional filters
  const [prebreakTier, setPrebreakTier]   = useState('')         // ''|'prime'|'ready'|'watch'
  const [pbLvbo,       setPbLvbo]         = useState(null)       // null|true
  const [pbStopCause,  setPbStopCause]    = useState(null)
  const [pbWvfConfirm, setPbWvfConfirm]   = useState(null)
  const [pbPpRtv,      setPbPpRtv]        = useState(null)       // null|true (PP+RTV)
  const [pbFlyCdC,     setPbFlyCdC]       = useState(null)       // null|true (FLY-CD confirmed)
  const [pbFollow,     setPbFollow]       = useState(null)       // null|true (FOLLOW confirm)
  const [pbMacroPen,   setPbMacroPen]     = useState(null)       // null|true|false
  const [wycInTr,      setWycInTr]        = useState(null)
  const hoverTimer = useRef(null)

  // ── DB info + manual refresh ─────────────────────────────────────────────
  const [dbInfo,       setDbInfo]       = useState(null)   // {date_to, rows, tickers}
  const [dbRefreshing, setDbRefreshing] = useState(false)  // incremental running
  const [dbRefreshPct, setDbRefreshPct] = useState(0)
  const dbPollRef = useRef(null)

  const fetchDbInfo = useCallback(async () => {
    try {
      const s = await api.studioStats()
      // backend returns {error:..., db_path:...} on failure — guard against that
      if (s && typeof s.rows === 'number') setDbInfo(s)
    } catch {}
  }, [])

  // load DB info on mount
  useEffect(() => { fetchDbInfo() }, [fetchDbInfo])

  const triggerDbRefresh = useCallback(async () => {
    if (dbRefreshing) return
    setDbRefreshing(true)
    setDbRefreshPct(0)
    try {
      await api.studioIncremental(['sp500', 'nasdaq'])
    } catch (e) {
      setDbRefreshing(false)
      return
    }
    // poll until done
    dbPollRef.current = setInterval(async () => {
      try {
        const s = await api.studioIncrementalStatus()
        const p = s?.progress || {}
        setDbRefreshPct(p.pct ?? 0)
        if (!s?.running) {
          clearInterval(dbPollRef.current)
          setDbRefreshing(false)
          setDbRefreshPct(0)
          fetchDbInfo()          // refresh date label
          // reload Ultra scan results from DB
          fetchFromDB && fetchFromDB()
        }
      } catch { clearInterval(dbPollRef.current); setDbRefreshing(false) }
    }, 3000)
  }, [dbRefreshing, fetchDbInfo])

  useEffect(() => () => clearInterval(dbPollRef.current), [])

  // which TFs have a cache entry for current universe
  const tfCached = useMemo(
    () => Object.fromEntries(TF_OPTS.map(t => [t, !!_tsGet(t, universe)?.results?.length])),
    [universe, allResults]  // re-check when results change (after scan saves cache)
  )

  // ── DB-backed fetch — instant (~1-2 sec) from enriched Studio DB ──────────
  const fetchFromDB = useCallback(async () => {
    const seq = ++fetchSeqRef.current
    // RUNNING must be set here too, not just `scanning`. Otherwise a DB-instant fetch
    // renders "Scanning…" on the button while the state label still reads "not run yet".
    setScanning(true); setScanState('RUNNING'); setError(null)
    try {
      const unis = _uniList(universe)
      const d = await api.ultraScanFromDB(unis)
      if (seq !== fetchSeqRef.current) return
      const results = d.results || []
      const scanned = d.scanned_at
      setAllResults(results)
      setLastScan(scanned || null)
      ultraCacheSet(localTf, universe, results, scanned)
      setScanState('COMPLETE')
      setCachedFromPreviousSession(false)   // these rows came from THIS backend, now
    } catch (e) {
      setError(e.message)
      setScanState('ERROR')
    } finally {
      setScanning(false)
    }
  }, [universe, localTf])

  // Hybrid Preview scan — DB history + today's LIVE forming bar (Massive),
  // full signal suite recomputed per ticker. Feeds the SAME grid state path as
  // fetchFromDB so all columns/filters/sort work unchanged.
  const runPreviewScan = useCallback(async () => {
    const seq = ++fetchSeqRef.current
    setPreviewing(true); setError(null); setPreviewInfo(null)
    try {
      const unis = _uniList(universe)
      const d = await api.ultraPreview(unis)
      if (seq !== fetchSeqRef.current) return
      const results = d.results || []
      setAllResults(results)
      setLastScan(d.scanned_at || null)
      setPreviewInfo({
        session:  d.preview_session,
        note:     d.preview_note || null,
        liveBars: d.live_bar_count ?? null,
        elapsed:  d.elapsed_seconds ?? null,
      })
      // Don't overwrite the DB cache with preview (today-overlaid) rows.
    } catch (e) {
      setError(e.message)
    } finally {
      setPreviewing(false)
    }
  }, [universe])

  // Fetch fresh results from server after a scan completes, then cache.
  // Uses fetchSeqRef to discard responses from stale (superseded) requests.
  const fetchFreshResults = useCallback((tf, uni) => {
    const seq = ++fetchSeqRef.current
    api.ultraScanResults(uni, tf)
      .then(d => {
        if (seq !== fetchSeqRef.current) return  // superseded by a newer fetch
        const results = d.results || []
        const ls = d.last_scan
        // ULTRA results may be empty before the first scan — that's OK, don't error
        if (results.length > 0) {
          ultraCacheSet(tf, uni, results, ls)
          setAllResults(results)
          setLastScan(ls || null)
        }
      })
      .catch(e => { if (seq === fetchSeqRef.current) setError(e.message) })
  }, [])

  // Like fetchFreshResults, but MERGES only the enrichment fields into the loaded
  // rows (matched by ticker) instead of REPLACING the dataset. Used by the Profile
  // filters so toggling Capit / Capit→Atom / Atomic / Short / Mom never swaps the
  // scan snapshot — canonical scores, V2/V3 and the row set stay put; we just attach
  // the cap/atomic/mom flags the filter needs. (Fixes "scores jump / Capit→Atom count
  // changes when I click a profile filter".)
  const ENRICH_FIELDS = useMemo(() => [
    'cap_match', 'cap_age', 'cap_score', 'cap_atoms',
    'atomic_match', 'atomic_age', 'atomic_score', 'atomic_atoms', 'atomic_post_capit', 'atomic_capit_age',
    'short_match', 'short_age', 'short_score', 'short_atoms',
    'mom_match', 'mom_age', 'mom_score', 'mom_atoms',
    'vol3t5_match', 'vol3t5_age', 'vol3t5_rsi', 'vol3t5_drop',
    'vol3t9_match', 'vol3t9_age', 'vol3t9_rsi', 'vol3t9_tier',
    'vol3t12_match', 'vol3t12_age', 'vol3t12_rsi', 'vol3t12_tier',
    'prebreak_v2', 'prebreak_v3', 'prebreak_score',
  ], [])
  const fetchEnrichmentMerge = useCallback((tf, uni) => {
    api.ultraScanResults(uni, tf)
      .then(d => {
        const fresh = d.results || []
        if (!fresh.length) return
        const byTk = new Map(fresh.map(r => [r.ticker, r]))
        setAllResults(prev => prev.map(r => {
          const f = byTk.get(r.ticker)
          if (!f) return r
          const merged = { ...r }
          for (const k of ENRICH_FIELDS) if (f[k] !== undefined) merged[k] = f[k]
          // V4 is computed once per row and cached ON the row (see the useMemo below). The
          // spread above copies that cache, so a row scored BEFORE enrichment arrived kept its
          // pre-enrichment total forever (2026-09-24: ICE showed 115 right after a scan, 160 —
          // the correct, Superchart-matching value — only after a remount rebuilt the rows from
          // cache). Drop the cache so the memo re-scores this row on the enriched fields.
          delete merged.v4_score
          delete merged.v4_fired_labels
          return merged
        }))
      })
      .catch(() => {})
  }, [ENRICH_FIELDS])

  // Read from cache (IDB or localStorage) and populate allResults if empty
  const loadFromCache = useCallback(async (tf, uni) => {
    let cached
    if (getCacheBackend() === 'idb') {
      cached = await idbGet(tf, uni)
    } else {
      cached = _tsGet(tf, uni)
      if (!cached?.results?.length && uni !== 'all_us') {
        cached = _tsGet(tf, 'all_us')
      }
      if (cached?.truncated) {
        fetchFreshResults(tf, uni)
      }
    }
    if (cached?.results?.length) {
      setAllResults(cached.results)
      setLastScan(cached.lastScan || null)
    } else {
      setAllResults([])
      setLastScan(null)
    }
  }, [fetchFreshResults])

  useEffect(() => {
    if (sourceMode === 'db') {
      // DB mode — instant fetch, no cache fallback needed
      fetchFromDB()
    } else {
      loadFromCache(localTf, universe)       // instant from cache (may be stale)
      fetchFreshResults(localTf, universe)   // always refresh from server in background
    }
  }, [localTf, universe, sourceMode]) // eslint-disable-line react-hooks/exhaustive-deps

  // Lazy-batch sector fetch: after results load, fetch sectors for tickers missing them
  useEffect(() => {
    if (!allResults.length) return
    const sorted = [...allResults].sort((a, b) => (b.turbo_score ?? 0) - (a.turbo_score ?? 0))
    const missing = sorted
      .filter(r => !r.sector && !sectorMap[r.ticker])
      .slice(0, 200)
      .map(r => r.ticker)
    if (!missing.length) return
    api.tickerInfoBatch(missing)
      .then(data => {
        setSectorMap(prev => {
          const next = { ...prev }
          for (const [ticker, info] of Object.entries(data)) {
            if (info.sector) next[ticker] = info.sector
          }
          return next
        })
      })
      .catch(() => {})
  }, [allResults]) // eslint-disable-line react-hooks/exhaustive-deps

  // 📐 MTF-EMA map — fetch the DB-computed SMX/RGTI EMA-stack variants once and
  // attach to rows so the 📐 SIG chips can filter. It's a latest-daily-close
  // snapshot (tf-independent), so one fetch covers every ultra tf/universe.
  useEffect(() => {
    let dead = false
    fetch('/api/mtf-ema-scan')
      .then(r => r.json())
      .then(d => {
        if (dead) return
        const map = {}
        for (const row of (d?.rows || [])) {
          if (row.ticker && row.variants?.length) map[row.ticker] = row.variants
        }
        setMtfEmaMap(map)
        setMtfEmaAgeMap(d?.ages || {})   // {ticker: {variant: age_days}} — age-aware N filtering
      })
      .catch(() => {})
    return () => { dead = true }
  }, [])

  // ULTRA does not subscribe to the admin "sachoki:scan-cached" Turbo event —
  // that event delivers raw Turbo rows, which would overwrite ULTRA's enriched
  // rows. ULTRA refreshes via its own /api/ultra-scan/results endpoint.

  // ── Pre-market fetch: runs when allResults changes + every 15 min ─────────
  const fetchPM = useCallback(() => {
    if (!allResults.length) return
    const tickers = [...new Set(allResults.map(r => r.ticker).filter(Boolean))]
    if (!tickers.length) return
    // Fetch in chunks of 200 to stay within URL limits
    const chunks = []
    for (let i = 0; i < tickers.length; i += 200) chunks.push(tickers.slice(i, i + 200))
    Promise.all(chunks.map(c => api.premarket(c)))
      .then(results => {
        const merged = {}
        results.forEach(r => Object.assign(merged, r.data || {}))
        setPmData(merged)
      })
      .catch(() => {})
  }, [allResults])

  useEffect(() => {
    fetchPM()
    if (pmTimerRef.current) clearInterval(pmTimerRef.current)
    pmTimerRef.current = setInterval(fetchPM, 15 * 60 * 1000)
    return () => { if (pmTimerRef.current) clearInterval(pmTimerRef.current) }
  }, [fetchPM]) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => { api.getConfig().then(c => setMassiveReady(c.massive_api_ready)).catch(() => {}) }, [])

  // ── Effective score column based on selected N ────────────────────────────
  // default (N=1d) Score column = buy_score (V2-backbone + RSI + volB, two-sided veto —
  // 2026-07-03 scoring analysis); N>1 lookback variants still use the turbo_nX columns.
  const effectiveScoreCol = lookbackN >= 10 ? 'turbo_score_n10'
                          : lookbackN >= 5  ? 'turbo_score_n5'
                          : lookbackN >= 3  ? 'turbo_score_n3'
                          : 'buy_score'

  // V4 — per-signal WEIGHTS, user-directed (2026-09-23; revised from the flat first pass once
  // the user saw how many entries repeat the same information). Every signal starts at 0
  // (src/lib/v4Weights.js) and the user is filling weights in incrementally, key by key — this
  // component only sums whatever v4Weights.js currently holds. Computed once per row, mutated
  // onto allResults and cached by identity — the same in-place-cache idiom `r._ages` already
  // uses below, so a full 402-predicate pass does not re-run on every filter click. Recomputing
  // needs a fresh scan (or a hard reload) to pick up a v4Weights.js edit, same as any other code
  // change — there is no live-reload of the weight map into an already-cached row.
  useMemo(() => {
    for (const r of allResults) {
      if (r.v4_score === undefined) {
        r.v4_score = v4Score(V4_ALL_GROUPS, V4_WEIGHTS, r, 1)
        r.v4_fired_labels = v4FiredLabels(V4_ALL_GROUPS, V4_WEIGHTS, r, 1)
      }
    }
  }, [allResults])

  // ── Client-side filter + sort ──────────────────────────────────────────────
  const results = useMemo(() => {
    // Attach DB-computed MTF-EMA variants (today, for display) + per-variant AGES (last 12
    // sessions) so the 📐 SIG chips honor the "last N bars" selector like the DB signals do.
    const withVar = allResults.map(r => {
      const v = mtfEmaMap[r.ticker], a = mtfEmaAgeMap[r.ticker]
      return (v || a) ? { ...r, mtf_ema_variants: v || r.mtf_ema_variants, mtf_ema_ages: a } : r
    })
    const filtered = withVar.filter(r => {
      // score band filter (multi-select)
      const score = r[effectiveScoreCol] ?? r.turbo_score ?? 0
      if (!scoreBands.has('all') && scoreBands.size > 0) {
        const inBand = SCORE_BANDS.some(b =>
          b.key !== 'all' && scoreBands.has(b.key) && score >= b.min && score <= b.max
        )
        if (!inBand) return false
      }
      if (volMin > 0 && r.avg_vol > 0 && r.avg_vol < volMin) return false
      if (volMax > 0 && r.avg_vol > 0 && r.avg_vol > volMax) return false
      if (priceMin !== '' && +priceMin > 0 && r.last_price > 0 && r.last_price < +priceMin) return false
      if (priceMax !== '' && +priceMax > 0 && r.last_price > 0 && r.last_price > +priceMax) return false
      if (secFilter && !(sectorMap[r.ticker] || r.sector || '').toLowerCase().includes(secFilter)) return false
      if (rtbPhase && (r.rtb_phase || '0') !== rtbPhase) return false
      // 260523 filters — every bool that can flip bar-to-bar honors
      // lookbackN via <col>_age. (`wyc_phase` is the only state field —
      // it reflects current bar only and ignores N.)
      const evHit = (col) => !!r[col] && ((r[`${col}_age`] ?? 99) < lookbackN)
      const notSeenInN = (col) => (r[`${col}_age`] ?? 99) >= lookbackN
      if (adFreshFilter   === true && !evHit('ad_fresh'))   return false
      if (adClusterFilter === true && !evHit('ad_cluster')) return false
      if (wycPhaseFilter && (r.wyc_phase || 'NEUTRAL') !== wycPhaseFilter) return false
      if (swingTypeFilter) {
        const st = r.swing_type || ''
        const stAge = r.swing_type_age ?? 99
        const recent = !!st && stAge < lookbackN
        if (swingTypeFilter === 'pivot') { if (!recent) return false }
        else if (!recent || st !== swingTypeFilter) return false
      }
      // 260523 v3.5 PREBREAK + WYC additional
      if (prebreakTier === 'prime' && !evHit('prebreak_prime')) return false
      if (prebreakTier === 'ready' && !evHit('prebreak_ready')) return false
      if (prebreakTier === 'watch' && !evHit('prebreak_watch')) return false
      if (pbLvbo === true       && !evHit('pb_lvbo'))         return false
      if (pbStopCause === true  && !evHit('pb_stop_cause'))   return false
      if (pbWvfConfirm === true && !evHit('pb_wvf_confirm'))  return false
      if (pbPpRtv === true      && !evHit('pb_pp_rtv'))       return false
      if (pbFlyCdC === true     && !evHit('pb_fly_cd_c'))     return false
      if (pbFollow === true     && !evHit('pb_follow_confirm')) return false
      if (pbMacroPen === false  && !notSeenInN('pb_macro_penalty')) return false
      if (pbMacroPen === true   && !evHit('pb_macro_penalty'))      return false
      if (wycInTr === true      && !evHit('wyc_in_tr'))             return false
      if (direction === 'bull' && !r.tz_bull) return false
      if (direction === 'bear' && r.tz_bull)  return false
      if (buyFilter.size) {
        const conf = r.mtf_score_conf
        // AND semantics — every selected chip must hold (e.g. 🟢REV✓ + ▲4H = confirmed REV
        // whose 4H trigger fired today; stack as many as you like)
        if (buyFilter.has('rev')  && !(r.rev_buy && r.mtf_echo !== false)) return false
        if (buyFilter.has('brk')  && !r.brk_buy) return false
        if (buyFilter.has('conf') && !(conf != null && conf >= 1)) return false
        if (buyFilter.has('turn') && !r.turn_echo_n) return false
        if (buyFilter.has('h4')   && !r.h4_rev_today) return false
        if (buyFilter.has('lheavy') && !r.heavy_l) return false
        if (buyFilter.has('edge') && !(r.edge_n > 0)) return false
        if (buyFilter.has('edgerev') && !r.edge_rev) return false
        if (buyFilter.has('seq34') && !r.seq34) return false
        if (buyFilter.has('ctxup') && r.seq_ctx?.dir !== 'up') return false
        if (buyFilter.has('ctxdn') && r.seq_ctx?.dir !== 'down') return false
        if (buyFilter.has('veto') && !((r.rev_buy && r.mtf_echo === false) || conf === 0)) return false
        if (buyFilter.has('any')  && !((r.rev_buy && r.mtf_echo !== false) || r.brk_buy || (conf != null && conf >= 1) || r.turn_echo_n)) return false
      }

      // ▽△ Bottom-Anatomy (2026-09-09) — reads the shared anatomyStore map (ONE /api/anatomy-latest
      // call, same source as the ▽△ cell and its sort), not a row field. While the map is in flight
      // this is a no-op instead of emptying the grid; the subscribeAnatomy tick re-runs this memo.
      if (anatFilter.size && anatomyReady()) {
        const a = getAnatomy(r.ticker)          // null = no verdict on the latest bar
        const picked = ['durable', 'struct', 'shake', 'cont'].filter(k => anatFilter.has(k))
        if (picked.length) {
          if (!a) return false
          const hit = picked.some(k => k === 'durable' ? (a.v === 'rev' && a.rs)
                                     : k === 'struct'  ? a.v === 'rev'
                                     : k === 'shake'   ? a.v === 'shake'
                                     :                   a.v === 'cont')
          if (!hit) return false
        }
        if (anatFilter.has('s5') && !(a && (a.s ?? 0) >= 5)) return false
      }
      if (sweetSpotFilter && !(r.sweet_spot_active && !r.late_warning)) return false
      if (buildingFilter && r.profile_category !== 'BUILDING') return false
      if (zoneFilterActive && activeZoneSet && !activeZoneSet.has(r.ticker)) return false
      if (gannFilter && gannSet && !gannSet.has(r.ticker)) return false
      if (vbwFilter.vb || vbwFilter.w) {
        const b = r.tz_wlnbb_volume_bucket || r.vol_bucket
        if (!((vbwFilter.vb && b === 'VB') || (vbwFilter.w && b === 'W'))) return false
      }
      if (watchFilter    && r.profile_category !== 'WATCH')    return false
      if (atomicFilter   && !(r.atomic_match && r.atomic_age != null && r.atomic_age < (lookbackN || 1))) return false
      if (shortFilter    && !(r.short_match  && r.short_age  != null && r.short_age  < (lookbackN || 1))) return false
      if (capFilter      && !(r.cap_match    && r.cap_age    != null && r.cap_age    < (lookbackN || 1))) return false
      if (momFilter      && !(r.mom_match    && r.mom_age    != null && r.mom_age    < (lookbackN || 1))) return false
      if (postCapitFilter && !r.atomic_post_capit) return false
      if (vol3t5Filter   && !(r.vol3t5_match  && r.vol3t5_age  != null && r.vol3t5_age  < (lookbackN || 1))) return false
      if (vol3t9Filter   && !(r.vol3t9_match  && r.vol3t9_age  != null && r.vol3t9_age  < (lookbackN || 1))) return false
      if (vol3t12Filter  && !(r.vol3t12_match && r.vol3t12_age != null && r.vol3t12_age < (lookbackN || 1))) return false
      // ⛓ sequence: every non-empty slot must be satisfied by ITS bar. seq3 arrives
      // oldest→newest, so slot i lines up with seq3[i] and the labels read the way the
      // chart does, left to right. An empty slot constrains nothing, which is what makes
      // a two-bar rule expressible without a second UI.
      if (seqActive) {
        const bars = r.seqbars
        if (!Array.isArray(bars) || bars.length === 0) return false
        // ALIGNED TO THE END: the last element is always today, so slot SEQ_BARS-1 is
        // today whatever the array's length. A ticker listed four bars ago still
        // satisfies a rule that only constrains the recent ones — padding from the
        // front would have silently excluded exactly the young names worth finding.
        for (let i = 0; i < SEQ_BARS; i++) {
          if (seqSlots[i].size === 0) continue
          const bar = bars[bars.length - SEQ_BARS + i]
          if (!bar) return false
          const ok = [...seqSlots[i]].every(k => {
            const sig = SIG_GROUPS.find(x => !x.divider && x.key === k)
            // N is meaningless inside a sequence — each slot IS one bar, so the
            // predicate is evaluated with lookback 1 against that bar alone.
            if (sig?.custom) { try { return !!sig.custom(bar, 1) } catch { return false } }
            // no custom predicate → an age-based chip. There is no age inside a
            // sequence, so the bar carries the filter keys that were true on it.
            if (Array.isArray(bar.k)) return bar.k.includes(k)
            return !!bar[k]
          })
          if (!ok) return false
        }
      }
      if (selSigs.size > 0) {
        // parse ages once per row (cached on the object)
        if (!r._ages && r.sig_ages) {
          try { r._ages = JSON.parse(r.sig_ages) } catch { r._ages = {} }
        }
        const ages = r._ages || {}
        const ok = [...selSigs].every(k => {
          const sig = SIG_GROUPS.find(s => !s.divider && s.key === k)
          if (sig?.custom) return sig.custom(r, lookbackN)
          // age-based check takes priority: works for all N (N=1 means age<1 = current bar only)
          if (k in ages) return ages[k] < lookbackN
          // fallback for signals not tracked in sig_ages (direct row field)
          return !!r[k]
        })
        if (!ok) return false
      }
      return true
    })
    // sort: when any profile filter active, sort by profile_score then turbo_score
    const mul = sortDir === 'asc' ? 1 : -1
    if (sweetSpotFilter || buildingFilter || watchFilter) {
      filtered.sort((a, b) =>
        (b.profile_score ?? 0) - (a.profile_score ?? 0) ||
        (b[effectiveScoreCol] ?? b.turbo_score ?? 0) - (a[effectiveScoreCol] ?? a.turbo_score ?? 0)
      )
    } else {
      filtered.sort((a, b) => {
        const col = sortBy === 'turbo_score' ? effectiveScoreCol : sortBy
        // pm_chg_pct / rt_chg_pct live in pmData; gex lives in the shared gexStore —
        // both external caches, not in the row objects.
        const getVal = (r) => (col === 'pm_chg_pct' || col === 'rt_chg_pct')
          ? (pmData[r.ticker]?.[col] ?? -Infinity)
          : col === 'anat' ? anatSortVal(r.ticker)
          : col === 'gex' ? gexSortVal(r.ticker)
          : col === 'vrp' ? vrpSortVal(r.ticker)
          : (r[col] ?? 0)
        const av = getVal(a)
        const bv = getVal(b)
        if (typeof av === 'string') return mul * av.localeCompare(bv)
        return mul * (av - bv)
      })
    }
    return filtered
  }, [allResults, mtfEmaMap, mtfEmaAgeMap, pmData, scoreBands, direction, selSigs, lookbackN, sortBy, sortDir, gexTick, anatTick, anatFilter, effectiveScoreCol, volMin, volMax, priceMin, priceMax, secFilter, sectorMap, rtbPhase, sweetSpotFilter, buyFilter, buildingFilter, watchFilter, adFreshFilter, adClusterFilter, wycPhaseFilter, swingTypeFilter, prebreakTier, pbLvbo, pbStopCause, pbWvfConfirm, pbPpRtv, pbFlyCdC, pbFollow, pbMacroPen, wycInTr, zoneTiers, zoneTierSets, gannFilter, gannSet, vbwFilter, atomicFilter, shortFilter, capFilter, momFilter, postCapitFilter, vol3t5Filter, vol3t9Filter, vol3t12Filter, seqSlots, seqActive])

  const toggleSort = (col) => {
    if (sortBy === col) setSortDir(d => d === 'desc' ? 'asc' : 'desc')
    else { setSortBy(col); setSortDir('desc') }
    // 💠 GEX is async/lazy — kick a bulk fetch for every currently-filtered row so
    // the sort has values to order by (fills progressively; store re-renders on arrival).
    if (col === 'gex' || col === 'vrp') requestGexBulk(results.map(r => r.ticker))
  }

  const SortTh = ({ col, children, cls = '' }) => (
    <th
      className={`px-2 py-1.5 font-medium cursor-pointer select-none hover:text-white transition-colors ${cls}`}
      onClick={() => toggleSort(col)}>
      {children}{sortBy === col ? (sortDir === 'desc' ? ' ↓' : ' ↑') : ''}
    </th>
  )

  const toggleSig = key => {
    if (seqTarget === 'main') {
      setSelSigs(prev => { const n = new Set(prev); n.has(key) ? n.delete(key) : n.add(key); return n })
      return
    }
    setSeqSlots(prev => prev.map((set, i) => {
      if (i !== seqTarget) return set
      const n = new Set(set); n.has(key) ? n.delete(key) : n.add(key); return n
    }))
  }


  // In DB-instant mode, hide the live-only chips entirely (they have no DB
  // column → 0 matches). Also drop any divider whose whole section becomes
  // empty, plus orphaned leading/trailing/double separators. Live mode shows
  // the full list (those signals are real there).
  const visibleSigGroups = useMemo(() => {
    if (sourceMode !== 'db') return SIG_GROUPS
    const kept = SIG_GROUPS.filter(s => s.divider || !LIVE_ONLY_SIGS.has(s.key))
    const out = []
    for (let i = 0; i < kept.length; i++) {
      const s = kept[i]
      if (s.divider) {
        const nextIsChip = kept[i + 1] && !kept[i + 1].divider
        if (!nextIsChip) continue                              // empty section
        const prev = out[out.length - 1]
        if (prev && prev.divider && !prev.label && !s.label) continue  // double dot
      }
      out.push(s)
    }
    while (out.length && out[out.length - 1].divider) out.pop()  // trailing
    return out
  }, [sourceMode])

  const togglePicked = (ticker, e) => {
    e.stopPropagation()
    setPickedTickers(prev => {
      const n = new Set(prev)
      n.has(ticker) ? n.delete(ticker) : n.add(ticker)
      return n
    })
  }

  // CSV cell sanitiser.
  // Numeric values pass through unchanged (negative numbers must NOT be
  // prefixed with apostrophe — that's a regression that breaks Excel/Sheets
  // numeric columns). Only strings starting with =,+,-,@ get the prefix,
  // and only when the string is not a parseable number.
  const _csvCell = (v) => {
    if (v == null) return ''
    if (Array.isArray(v))     return _csvCell(v.join(';'))
    if (typeof v === 'number') return Number.isFinite(v) ? String(v) : ''
    if (typeof v === 'boolean') return v ? 'true' : 'false'
    let s = String(v)
    // If the string is actually a number (including negatives like "-139.4"),
    // leave it as-is — it's a numeric value, not a formula.
    const isNumeric = s !== '' && !isNaN(Number(s)) && /^-?\d/.test(s)
    if (!isNumeric && /^[=+\-@]/.test(s)) s = "'" + s
    return s.includes(',') || s.includes('"') || s.includes('\n')
      ? `"${s.replace(/"/g, '""')}"` : s
  }

  // Display-column generators — mirror exactly what each table cell renders
  // so the CSV reflects what's visible on screen, badge-for-badge.
  const _displayRtb = (r) => {
    if (!r.rtb_phase || r.rtb_phase === '0') return ''
    return `${r.rtb_phase} ${(r.rtb_total ?? 0).toFixed(0)}`
  }
  const _displayTz = (r) => r.tz_sig || ''
  const _displayGog = (r) => {
    const parts = []
    if (r.gog_tier) parts.push(r.gog_tier)
    parts.push(...ctxTokens(r))
    if ((r.signal_score ?? 0) > 0) parts.push(`SCORE=${r.signal_score}`)
    if (r.already_extended) parts.push('EXT')
    return parts.join(' | ')
  }
  const _displayVabs = (r) => {
    const out = []
    if (r.vol_spike_20x) out.push('V×20')
    else if (r.vol_spike_10x) out.push('V×10')
    else if (r.vol_spike_5x)  out.push('V×5')
    if (r.best_sig)   out.push('BEST★')
    if (r.strong_sig && !r.best_sig) out.push('STR')
    if (r.vbo_up)     out.push('VBO↑')
    if (r.vbo_dn)     out.push('VBO↓')
    if (r.abs_sig)    out.push('ABS')
    if (r.climb_sig)  out.push('CLB')
    if (r.load_sig)   out.push('LD')
    if (r.d_blast_bull)  out.push('ΔΔ↑')
    if (r.d_surge_bull && !r.d_blast_bull) out.push('Δ↑')
    if (r.d_strong_bull) out.push('B/S↑')
    if (r.d_absorb_bull) out.push('Ab↑')
    if (r.d_div_bull)    out.push('T↓')
    if (r.d_cd_bull && !r.d_div_bull) out.push('cd↑')
    if (r.d_spring)      out.push('dSPR')
    if (r.d_vd_div_bull && !r.d_spring) out.push('NS')
    if (r.rs_strong)     out.push('RS+')
    else if (r.rs)       out.push('RS')
    const PREUP = [['preup66','P66'],['preup55','P55'],['preup89','P89'],
                    ['preup3','P3'],['preup2','P2'],['preup50','P50']]
    for (const [k, lbl] of PREUP) if (r[k]) { out.push(lbl); break }
    const PREDN = [['predn66','D66'],['predn55','D55'],['predn89','D89'],
                    ['predn3','D3'],['predn2','D2'],['predn50','D50']]
    for (const [k, lbl] of PREDN) if (r[k]) { out.push(lbl); break }
    for (const v of (r.mtf_ema_variants || [])) out.push('📐' + (v === 'ORANGE' ? 'ORG' : v))
    return out.join(' | ')
  }
  const _displayWyck = (r) => {
    const out = []
    if (r.ns) out.push('NS')
    if (r.sq) out.push('SQ')
    if (r.sc) out.push('SC')
    if (r.bc) out.push('BC')
    if (r.nd) out.push('ND')
    return out.join(' | ')
  }
  const _displayCombo = (r) => {
    const out = []
    if (r.rocket)    out.push('🚀')
    if (r.buy_2809)  out.push('BUY')
    if (r.sig3g)     out.push('3G')
    if (r.rtv)       out.push('RTV')
    if (r.hilo_buy)  out.push('HILO↑')
    if (r.atr_brk)   out.push('ATR↑')
    if (r.bb_brk)    out.push('BB↑')
    if (r.va)        out.push('VA')
    if (r.bias_up)   out.push('↑BIAS')
    if (r.hilo_sell) out.push('HILO↓')
    if (r.bias_down) out.push('↓BIAS')
    if (r.um_2809)    out.push('UM')
    if (r.svs_2809)   out.push('SVS')
    if (r.conso_2809) out.push('CON')
    // PT5 badges — classification only; alias overlap never strengthens a PT5
    if (r.pt5_strong) out.push('PT5+')
    else if (r.pt5_h1_any && r.pt5) out.push('PT5·1H')
    else if (r.pt5_m15_any && r.pt5) out.push('PT5·15')
    else if (r.pt5) out.push('PT5')
    if (r.pt5_xr_volw_va) out.push('XR:VOL_W↔VA')
    if (r.mt5_strong) out.push('MT5+')
    // L-BAL agreement marks (★ ★★ ★★★ ○○○ XXX). BOTH counting modes, suffixed V / a — they
    // disagree on 55.6 % of sessions, so exporting "whichever the switch was on" made the column
    // depend on hidden UI state.
    for (const m of String(r.lbal_marks || '').split(' ')) if (m) out.push(m + 'V')
    for (const m of String(r.lbal_all_marks || '').split(' ')) if (m) out.push(m + 'a')
    // L-VX graded daily L34 / L46 (tier ≥ V; the plain label is already the L badge)
    if (r.lvx_label && r.lvx_tier >= 1) out.push(r.lvx_label)
    // OVD daily-map tokens (OB/RC/CD/HO ·30/·60 · NM?)
    for (const t of String(r.ovdmap_tokens || '').split(' ')) if (t) out.push(t)
    // VOL7 — the level pair when M5/M6, plus every mark
    if (r.vol7_mr >= 5) out.push(r.vol7_label)
    for (const t of String(r.vol7_marks || '').split(' ')) if (t) out.push(t)
    // one badge per family that marked this bar — never merged into a single score
    for (const [f, n] of [['PT9','pt9'], ['PT3','pt3'], ['PT1','pt1']]) {
      if (r[n + '_strong']) out.push(f + '+')
      else if (r[n + '_h1_any'] && r[n]) out.push(f + '·1H')
      else if (r[n + '_m15_any'] && r[n]) out.push(f + '·15')
      else if (r[n]) out.push(f)
    }
    if (r.cd) out.push('CD')
    else if (r.ca) out.push('CA')
    else if (r.cw) out.push('CW')
    for (const k of ['g1','g2','g4','g6','g11'])
      if (r[k]) out.push(k.toUpperCase())
    if (r.seq_bcont) out.push('SBC')
    if (r.tz_sig) out.push(r.tz_sig)
    if (r.tz_bull_flip) out.push('TZ→3')
    else if (r.tz_attempt) out.push('TZ→2')
    if (r.smx)            out.push('SMX')
    if (r.rgti_ll)        out.push('LL')
    if (r.rgti_up)        out.push('UP')
    if (r.rgti_upup)      out.push('↑↑')
    if (r.rgti_upupup)    out.push('↑↑↑')
    if (r.rgti_orange)    out.push('ORG')
    if (r.rgti_green)     out.push('GRN')
    if (r.rgti_greencirc) out.push('GC')
    if (r.para_plus) out.push('PARA+')
    else if (r.para_start) out.push('PARA')
    if (r.para_prep)   out.push('PREP')
    if (r.para_retest) out.push('RTEST')
    if (r.akan_sig) out.push('A')
    if (r.smx_sig)  out.push('SM')
    if (r.nnn_sig)  out.push('N')
    if (r.mx_sig)   out.push('MX')
    if (r.gog_sig)  out.push('GOG')
    if (r.fly_abcd) out.push('ABCD')
    else {
      if (r.fly_cd) out.push('CD')
      if (r.fly_bd) out.push('BD')
      if (r.fly_ad) out.push('AD')
    }
    return out.join(' | ')
  }
  const _displayLsigUltra = (r) => {
    const out = []
    if (r.fri34) out.push('FRI34')
    if (r.fri43) out.push('FRI43')
    if (r.fri64) out.push('FRI64')
    if (r.l34  && !r.fri34) out.push('L34')
    if (r.l43  && !r.fri43) out.push('L43')
    if (r.l64  && !r.fri64) out.push('L64')
    if (r.l22)       out.push('L22')
    if (r.l555)      out.push('L555')
    if (r.only_l2l4) out.push('L2L4')
    if (r.blue)         out.push('BL')
    if (r.cci_ready)    out.push('CCI')
    if (r.cci_0_retest) out.push('CCI0R')
    if (r.cci_blue_turn)out.push('CCIB')
    if (r.bo_up) out.push('BO↑')
    if (r.bo_dn) out.push('BO↓')
    if (r.bx_up) out.push('BX↑')
    if (r.bx_dn) out.push('BX↓')
    if (r.be_up) out.push('BE↑')
    if (r.be_dn) out.push('BE↓')
    if (r.fuchsia_rh) out.push('RH')
    if (r.fuchsia_rl) out.push('RL')
    if (r.pre_pump)   out.push('PP')
    if (r.x2g_wick) out.push('X2G')
    if (r.x2_wick)  out.push('X2')
    if (r.x1g_wick) out.push('X1G')
    if (r.x1_wick)  out.push('X1')
    if (r.x3_wick)  out.push('X3')
    if (r.wick_bull) out.push('WK↑')
    if (r.best_long) out.push('BEST↑')
    else if (r.fbo_bull) out.push('FBO↑')
    if (r.fbo_bear) out.push('FBO↓')
    if (r.eb_bull)  out.push('EB↑')
    if (r.eb_bear)  out.push('EB↓')
    if (r.bf_buy)   out.push('4BF')
    if (r.bf_sell)  out.push('4BF↓')
    if (r.ultra_3up) out.push('3↑')
    if (r.sig_l88)   out.push('L88')
    else if (r.sig_260308) out.push('260308')
    return out.join(' | ')
  }
  const _displayCategory = (r) => r.profile_category || ''
  const _displayProfile = (r) => {
    const parts = []
    if (r.profile_score != null) parts.push(`PF=${r.profile_score}`)
    if (r.profile_category)      parts.push(r.profile_category)
    if (r.profile_name)          parts.push(r.profile_name)
    if (r.sweet_spot_active)     parts.push('SWEET')
    if (r.late_warning)          parts.push('LATE')
    return parts.join(' | ')
  }

  // ULTRA export: CSV including Turbo fields + enrichment columns (read-only)
  // ── TradingView watchlist export ───────────────────────────────────────────
  // Plain ticker symbols (deduped, current sort order) as a comma-separated .txt
  // — the format TradingView's "Import watchlist" accepts. Respects the picked
  // selection if any, otherwise the full filtered list.
  const exportTradingView = () => {
    const src = pickedTickers.size > 0
      ? results.filter(r => pickedTickers.has(r.ticker))
      : results
    const seen = new Set()
    const syms = []
    for (const r of src) {
      const t = String(r.ticker || '').trim().toUpperCase()
      if (t && !seen.has(t)) { seen.add(t); syms.push(t) }
    }
    if (!syms.length) return
    const blob = new Blob([syms.join(',')], { type: 'text/plain' })
    const url  = URL.createObjectURL(blob)
    const a    = document.createElement('a')
    const date = new Date().toISOString().slice(0, 10)
    const parts = [universe, localTf.toUpperCase()]
    if (direction !== 'all') parts.push(direction.toUpperCase())
    if (pickedTickers.size > 0) parts.push(`picked${syms.length}`)
    parts.push(date)
    a.href = url
    a.download = `ultra_${parts.join('_')}_tradingview.txt`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
    setTvExported(true)
    setTimeout(() => setTvExported(false), 2000)
  }

  const exportTickers = () => {
    const src = pickedTickers.size > 0
      ? results.filter(r => pickedTickers.has(r.ticker))
      : results

    // Display columns — one per visible badge group, joined by " | "
    const DISPLAY_FIELDS = [
      'turbo_rtb_display', 'turbo_tz_display', 'turbo_gog_display',
      'turbo_vabs_display', 'turbo_wyck_display', 'turbo_combo_display',
      'turbo_lsig_ultra_display',
      'turbo_category_display', 'turbo_profile_display',
      // ULTRA Score + compact pullback / rare labels (v2 calibration adds
      // band_v2/priority/regime_bonus/caps so CSV consumers see the same
      // fields the live UI reads).
      'ultra_score', 'ultra_score_band', 'ultra_score_band_v2',
      'ultra_score_priority', 'ultra_score_regime_bonus',
      'ultra_score_caps_applied', 'ultra_score_cap_reason',
      'ultra_score_reasons',
      'pullback_display_compact', 'rare_reversal_display_compact',
    ]

    // Core / category fields visible in the table
    const CORE_FIELDS = [
      'ticker', 'turbo_score', 'turbo_score_n3', 'turbo_score_n5', 'turbo_score_n10',
      'buy_score', 'buy_tag',
      'rtb_total', 'rtb_phase', 'tz_sig', 'tz_bull',
      'signal_score', 'profile_score', 'profile_category', 'profile_name',
      'sweet_spot_active', 'late_warning', 'gog_tier',
      'rsi', 'cci', 'last_price', 'change_pct',
      // RT% (today's real-time regular-session move) + PM% — from Massive snapshot
      'rt_chg_pct', 'rt_price', 'pm_chg_pct',
      'avg_vol', 'vol_bucket',
      // PreBreakout v3 (additive cluster score + reasons)
      'prebreak_v3', 'prebreak_v3_reasons',
      'sector', 'data_source',
      // BETA Score
      'beta_score', 'beta_raw', 'beta_setup', 'beta_momentum',
      'beta_excess', 'beta_zone', 'beta_auto_buy',
    ]

    // Raw signal fields behind every visible Turbo badge (boolean / numeric).
    // Order grouped by family for readability — does not affect correctness.
    const RAW_TURBO_FIELDS = [
      // GOG / setup
      'gog_sig','gog_score','gog_g1p','gog_g2p','gog_g3p',
      'gog_g1l','gog_g2l','gog_g3l','gog_g1c','gog_g2c','gog_g3c',
      'gog_gog1','gog_gog2','gog_gog3',
      'smx_sig','akan_sig','nnn_sig','mx_sig','already_extended',
      // VABS / Wyckoff
      'best_sig','strong_sig','vbo_up','vbo_dn','abs_sig','climb_sig','load_sig',
      'ns','nd','sc','bc','sq','va',
      'vol_spike_5x','vol_spike_10x','vol_spike_20x',
      // Combo / 2809 / trend
      'buy_2809','rocket','sig3g','rtv','hilo_buy','hilo_sell',
      'atr_brk','bb_brk','bias_up','bias_down','cons_atr',
      'um_2809','svs_2809','conso_2809',
      'pt5','pt5_class','pt5_h1_any','pt5_m15_any','pt5_strong',
      'pt5_h1_family','pt5_m15_clusters','pt5_m15_n','pt5_xr_volw_va',
      'pt9','pt9_class','pt9_h1_any','pt9_m15_any','pt9_strong','pt9_h1_family','pt9_m15_n',
      'pt3','pt3_class','pt3_h1_any','pt3_strong','pt3_h1_family',
      'pt1','pt1_class','pt1_h1_any','pt1_strong','pt1_h1_family',
      'pt_any','pt_any_h1','pt_any_m15','pt_any_strong','pt_families','pt_n_families',
      'mt5','mt5_class','mt5_h1','mt5_m15','mt5_conf','mt5_strong',
      'lbal','lbal_mode','lbal_marks','lbal_text','lbal_udn','lbal_udn_c','lbal_star','lbal_conflict',
      'lbal_half_up','lbal_half_dn','lbal_nn','lbal_n_pos','lbal_n_neg','lbal_n_pos_c','lbal_n_neg_c',
      'lbal_all_marks','lbal_all_text','lbal_all_udn','lbal_all_udn_c','lbal_all_star','lbal_all_conflict',
      'lbal_all_half_up','lbal_all_half_dn','lbal_all_nn',
      'lvx','lvx_fam','lvx_tier','lvx_label','lvx_text','lvx_v','lvx_lv15','lvx_lv1h',
      'lvx_l34_v','lvx_l34_vl','lvx_l34_vh','lvx_l34_vx','lvx_l46_v','lvx_l46_vl','lvx_l46_vh','lvx_l46_vx',
      'ovdmap','ovdmap_tokens','ovdmap_text','ovdmap_ob30','ovdmap_ob60','ovdmap_rc30','ovdmap_rc60',
      'ovdmap_cd30','ovdmap_cd60','ovdmap_ho30','ovdmap_ho60','ovdmap_nm','ovdmap_rv_o60','ovdmap_rv_c60',
      'ovdmap_reclaim60','ovdmap_event_k','ovdmap_handoff60',
      'vol7','vol7_mr','vol7_sg','vol7_ratio','vol7_cons','vol7_jump','vol7_label','vol7_marks','vol7_text','vol7_trans',
      'vol7_prior_move','vol7_m0','vol7_m5','vol7_m6','vol7_up2','vol7_dn2','vol7_up3','vol7_dn3','vol7_sigma_plus',
      'vol7_mr_plus','vol7_eq','vol7_vb2','vol7_shift_up','vol7_shift_dn',
      'ca','cd','cw','seq_bcont','any_f',
      'f1','f2','f3','f4','f5','f6','f7','f8','f9','f10','f11',
      // B
      'b1','b2','b3','b4','b5','b6','b7','b8','b9','b10','b11',
      // G
      'g1','g2','g4','g6','g11',
      // T/Z
      'tz_state','tz_bull_flip','tz_attempt','tz_weak_bull','tz_weak_bear',
      // WLNBB / L-Sig
      'fri34','fri43','fri64','l34','l43','l64','l22','l555','only_l2l4',
      'blue','cci_ready','cci_0_retest','cci_blue_turn',
      'fuchsia_rh','fuchsia_rl','pre_pump',
      // Breakout / Ultra-v2
      'eb_bull','eb_bear','fbo_bull','fbo_bear','bf_buy','bf_sell',
      'ultra_3up','ultra_3dn','best_long','best_short',
      'bo_up','bo_dn','bx_up','bx_dn','be_up','be_dn',
      'sig_260308','sig_l88',
      // Delta / order-flow
      'd_strong_bull','d_strong_bear','d_absorb_bull','d_absorb_bear',
      'd_div_bull','d_div_bear','d_cd_bull','d_cd_bear',
      'd_surge_bull','d_surge_bear','d_blast_bull','d_blast_bear',
      'd_vd_div_bull','d_vd_div_bear','d_spring','d_upthrust',
      'd_flip_bull','d_flip_bear','d_orange_bull',
      'd_blast_bull_red','d_blast_bear_grn',
      'd_surge_bull_red','d_surge_bear_grn',
      // Wick
      'wick_bull','wick_bear','x2g_wick','x2_wick','x1g_wick','x1_wick','x3_wick',
      // RTB
      'rtb_build','rtb_turn','rtb_ready','rtb_bonus3','rtb_late',
      'rtb_transition','rtb_phase_age',
      // RGTI / SMX / PARA / FLY
      'rgti_ll','rgti_up','rgti_upup','rgti_upupup','rgti_orange','rgti_green','rgti_greencirc',
      'smx',
      'para_prep','para_start','para_plus','para_retest',
      'fly_abcd','fly_cd','fly_bd','fly_ad',
      // PREUP / PREDN
      'preup66','preup55','preup89','preup3','preup2','preup50',
      'predn66','predn55','predn89','predn3','predn2','predn50',
      // RS
      'rs','rs_strong',
    ]

    // ULTRA enrichment fields — unchanged
    const ULTRA_FIELDS = [
      'ultra_enriched',
      'ultra_has_turbo', 'ultra_has_tz_wlnbb', 'ultra_has_tz_intel',
      'ultra_has_pullback', 'ultra_has_rare_reversal',
      'tz_wlnbb_t_signal', 'tz_wlnbb_z_signal', 'tz_wlnbb_l_signal',
      'tz_wlnbb_preup_signal', 'tz_wlnbb_predn_signal',
      'tz_wlnbb_lane1_label', 'tz_wlnbb_lane3_label',
      'tz_wlnbb_volume_bucket', 'tz_wlnbb_wick_suffix',
      'tz_wlnbb_bar_body_wick', 'tz_wlnbb_bar_gap_range',
      'tz_wlnbb_bar_line5', 'tz_wlnbb_ne_suffix',
      'tz_intel_role', 'tz_intel_quality', 'tz_intel_action', 'tz_intel_score',
      'tz_intel_matched_status', 'tz_intel_matched_med10d_pct', 'tz_intel_matched_fail10d_pct',
      'abr_category', 'abr_med10d_pct', 'abr_fail10d_pct',
      'abr_context_type', 'abr_action_hint', 'abr_conflict_flag', 'abr_confirmation_flag',
      'pullback_evidence_tier', 'pullback_pullback_stage', 'pullback_pattern_key',
      'pullback_pattern_length', 'pullback_score',
      'pullback_median_10d_return', 'pullback_win_rate_10d', 'pullback_fail_rate_10d',
      'pullback_is_currently_active', 'pullback_current_pattern_completion',
      'rare_evidence_tier', 'rare_base4_key', 'rare_extended5_key', 'rare_extended6_key',
      'rare_pattern_length', 'rare_score',
      'rare_median_10d_return', 'rare_fail_rate_10d',
      'rare_is_currently_active', 'rare_current_pattern_completion',
    ]

    const COLS = [...DISPLAY_FIELDS, ...CORE_FIELDS, ...RAW_TURBO_FIELDS, ...ULTRA_FIELDS]

    const flatten = (r) => {
      const u = r.ultra_sources || {}
      const w = r.tz_wlnbb || {}
      const i = r.tz_intel || {}
      const a = r.abr      || {}
      const p = r.pullback || {}
      const x = r.rare_reversal || {}
      const flat = {}

      // Display columns
      flat.turbo_rtb_display        = _displayRtb(r)
      flat.turbo_tz_display         = _displayTz(r)
      flat.turbo_gog_display        = _displayGog(r)
      flat.turbo_vabs_display       = _displayVabs(r)
      flat.turbo_wyck_display       = _displayWyck(r)
      flat.turbo_combo_display      = _displayCombo(r)
      flat.turbo_lsig_ultra_display = _displayLsigUltra(r)
      flat.turbo_category_display   = _displayCategory(r)
      flat.turbo_profile_display    = _displayProfile(r)

      // ULTRA Score fields (computed on the backend) + compact tier displays
      flat.ultra_score              = r.ultra_score ?? ''
      flat.ultra_score_band         = r.ultra_score_band ?? ''
      flat.ultra_score_band_v2      = r.ultra_score_band_v2
        ?? ultraBandV2Label(r.ultra_score, r.ultra_score_band) ?? ''
      flat.ultra_score_priority     = r.ultra_score_priority
        ?? ultraPriorityLabel(r.ultra_score) ?? ''
      flat.ultra_score_regime_bonus = r.ultra_score_regime_bonus ?? ''
      flat.ultra_score_caps_applied = Array.isArray(r.ultra_score_caps_applied)
        ? r.ultra_score_caps_applied.join(' ')
        : (r.ultra_score_caps_applied ?? '')
      flat.ultra_score_cap_reason   = r.ultra_score_cap_reason ?? ''
      flat.ultra_score_reasons      = r.ultra_score_reasons ?? ''
      // v3 reweighted ranker (oversold + price-zone + earners + 🏆RS/🎯cluster/🎋TLS)
      flat.ultra_score_v3        = r.ultra_score_v3 ?? ''
      flat.ultra_score_v3_band   = r.ultra_score_v3_band ?? ''
      flat.ultra_score_v3_reasons = Array.isArray(r.ultra_score_v3_reasons)
        ? r.ultra_score_v3_reasons.join(' ')
        : (r.ultra_score_v3_reasons ?? '')
      flat.score_hits = r.score_hits ?? ''
      flat.score_hits_which = Array.isArray(r.score_hits_which) ? r.score_hits_which.join(' ') : ''
      flat.div_buy    = r.div_buy ?? ''
      flat.div_deep   = r.div_deep ? 1 : 0
      flat.div_top    = r.div_top ?? ''
      flat.div_rsi_lo = r.div_rsi_lo ?? ''
      flat.div_rsi_hi = r.div_rsi_hi ?? ''
      flat.buy_flag = r.buy_flag ?? ''
      flat.mtf_score_conf = r.mtf_score_conf ?? ''
      flat.turn_echo_n = r.turn_echo_n ?? ''
      flat.h4_rev_today = r.h4_rev_today ? 1 : 0
      flat.heavy_l = r.heavy_l ? 1 : 0
      flat.edges = (r.edges ?? []).join(' ')
      flat.edge_n = r.edge_n ?? 0
      flat.rank_pct = r.rank_pct ?? ''
      flat.rank_edge = r.rank_edge ?? ''
      flat.rank_fam = r.rank_fam ?? ''
      flat.edge_rev = r.edge_rev ? 1 : 0
      // ⏱ ATR time-to-target forecast (2026-07-26): backend attaches atr_pct (ATR14/close)
      flat.atr_pct = r.atr_pct != null ? +(r.atr_pct * 100).toFixed(1) : ''
      { const _f = r.atr_pct != null ? atrForecast(r.atr_pct) : null
        flat.tt10 = _f ? _f.up10.days : ''      // typical days to +10%
        flat.tt10_hit = _f ? _f.up10.hit : ''   // hit-rate %
        flat.ttdn10 = _f ? _f.dn10.days : '' }  // typical days to −10% (stop timing)
      flat.no_vol_event = r.no_vol_event ? 1 : 0   // ⛔ no intraday volume event today
      // SHAPE × CONTEXT (2026-09-10) — descriptive; four sealed families, k = 21, 0 BUILD.
      // shape_lstup_veto is the ONE cell with two-window evidence and it is a VETO.
      // shape_by_fam / shape_by_den (🎯 / 🔁) measured MONOTONICALLY WORSE, so read them as
      // context, never as strength.
      flat.shape_code   = r.shape_code ?? ''
      flat.shape_label  = r.shape_label ?? ''
      flat.shape_grade  = r.shape_grade ?? ''
      flat.shape_vr     = r.shape_vr != null ? +Number(r.shape_vr).toFixed(2) : ''
      flat.shape_rsi    = r.shape_rsi != null ? +Number(r.shape_rsi).toFixed(1) : ''
      flat.shape_band   = r.shape_band ?? ''
      flat.shape_pos20  = r.shape_pos20 != null ? +Number(r.shape_pos20).toFixed(3) : ''
      flat.shape_touches = r.shape_touches ?? ''
      flat.shape_floor  = r.shape_floor ? 1 : 0
      flat.shape_key    = r.shape_key ? 1 : 0
      flat.shape_rs     = r.shape_rs ? 1 : 0
      flat.shape_absorb = r.shape_absorb ? 1 : 0
      flat.shape_dry    = r.shape_dry ? 1 : 0
      flat.shape_sweet  = r.shape_sweet ? 1 : 0
      flat.shape_dir_up = r.shape_dir_up ? 1 : 0
      flat.shape_dir_dn = r.shape_dir_dn ? 1 : 0
      flat.shape_cl_fam  = r.shape_cl_fam ?? ''
      flat.shape_cl_bars = r.shape_cl_bars ?? ''
      flat.shape_by_fam  = r.shape_by_fam ? 1 : 0
      flat.shape_by_den  = r.shape_by_den ? 1 : 0
      flat.shape_mark    = r.shape_mark ?? ''
      flat.shape_legs    = r.shape_legs ?? ''
      flat.shape_knife_veto = r.shape_veto ? 1 : 0
      flat.shape_lstup_veto = r.shape_lstup_veto ? 1 : 0
      // PRICE × VOLUME (2026-09-22) — descriptive; PV_MULTI_V1 sealed k = 9 → 0 BUILD /
      // 4 VETO_CANDIDATE / 5 NULL, every cell negative in MINE. Both price sources are exported
      // because they agree on only 52.7% of firing sessions. pv_veto_* is recorded, NOT applied.
      flat.pv_close   = r.pv_c ?? ''
      flat.pv_ohlc4   = r.pv_o ?? ''
      flat.pv_agree   = r.pv_agree ? 1 : 0
      flat.pv_veto_close = r.pv_veto_c ? 1 : 0
      flat.pv_veto_ohlc4 = r.pv_veto_o ? 1 : 0
      // VOL ECHO (2026-09-27) — descriptive; ve_qr_rel_veto = the confirmed QR_REL_V1 veto shape,
      // recorded, NOT applied. SPK is hindsight and is not exported from a same-day scan.
      flat.ve_text      = r.ve_text ?? ''
      flat.ve_echo      = r.ve_echo ? 1 : 0
      flat.ve_echo_gap  = r.ve_echo_gap ?? ''
      flat.ve_echo_col  = r.ve_echo_col ?? ''
      flat.ve_q         = r.ve_q ? 1 : 0
      flat.ve_r         = r.ve_r ? 1 : 0
      flat.ve_rel_up    = r.ve_rel_up ? 1 : 0
      flat.ve_rel_dn    = r.ve_rel_dn ? 1 : 0
      flat.ve_bo        = r.ve_bo ? 1 : 0
      flat.ve_bov       = r.ve_bov ? 1 : 0
      flat.ve_bd        = r.ve_bd ? 1 : 0
      flat.ve_bdv       = r.ve_bdv ? 1 : 0
      flat.ve_zone_pos  = r.ve_zone_pos ?? ''
      flat.ve_qr_rel_veto = r.ve_qr_rel_veto ? 1 : 0
      // TURN·58 + ⟲ROW (2026-09-28) — descriptive turn-zone gauges, not a buy signal.
      flat.turn58_n   = r.turn58_cand ? (r.turn58_n ?? '') : ''
      flat.rs_text    = r.rs_text ?? ''
      flat.rs_nconf   = r.rs_nconf ?? ''
      flat.rs_pair    = r.rs_pair ? 1 : 0
      flat.seq34 = r.seq34 ? r.seq34.seq : ''
      flat.seq34_win = r.seq34?.win ?? ''
      flat.seq34_ps_med = r.seq34?.ps_med ?? ''
      flat.seq34_dsr = r.seq34?.dsr ?? ''
      flat.seq_ctx = r.seq_ctx ? `${r.seq_ctx.dir}:${r.seq_ctx.seq}` : ''
      flat.seq_ctx_up = r.seq_ctx?.up ?? ''
      flat.seq_ctx_sig = r.seq_ctx?.sig ?? ''
      flat.conf_score = r.conf_score ?? ''
      flat.conf_top = r.conf_top ?? ''
      flat.conf_ext = r.conf_ext ?? ''
      flat.conf_ext_top = r.conf_ext_top ?? ''
      flat.pullback_display_compact      = pullbackCompact(r.pullback)
      flat.rare_reversal_display_compact = rareCompact(r.rare_reversal)

      // Pass-through core + raw fields. Numerics stay numeric so _csvCell
      // doesn't mangle them.
      for (const k of CORE_FIELDS)      flat[k] = r[k]
      for (const k of RAW_TURBO_FIELDS) flat[k] = r[k]

      // RT%/PM% live in pmData (external Massive cache), not the row — inject them
      const _pm = pmData[r.ticker] || {}
      flat.rt_chg_pct = _pm.rt_chg_pct ?? ''
      flat.rt_price   = _pm.rt_price ?? ''
      flat.pm_chg_pct = _pm.pm_chg_pct ?? ''

      // ULTRA flags / enrichment slots
      flat.ultra_enriched           = !!r.ultra_enriched
      flat.ultra_has_turbo          = !!u.has_turbo
      flat.ultra_has_tz_wlnbb       = !!u.has_tz_wlnbb
      flat.ultra_has_tz_intel       = !!u.has_tz_intel
      flat.ultra_has_pullback       = !!u.has_pullback
      flat.ultra_has_rare_reversal  = !!u.has_rare_reversal
      for (const [k, v] of Object.entries(w)) flat[`tz_wlnbb_${k}`] = v
      flat.tz_intel_role                = i.role
      flat.tz_intel_quality             = i.quality
      flat.tz_intel_action              = i.action
      flat.tz_intel_score               = i.score
      flat.tz_intel_matched_status      = i.matched_status
      flat.tz_intel_matched_med10d_pct  = i.matched_med10d_pct
      flat.tz_intel_matched_fail10d_pct = i.matched_fail10d_pct
      flat.abr_category         = a.category
      flat.abr_med10d_pct       = a.med10d_pct
      flat.abr_fail10d_pct      = a.fail10d_pct
      flat.abr_context_type     = a.context_type
      flat.abr_action_hint      = a.action_hint
      flat.abr_conflict_flag    = !!a.conflict_flag
      flat.abr_confirmation_flag= !!a.confirmation_flag
      flat.pullback_evidence_tier              = p.evidence_tier
      flat.pullback_pullback_stage             = p.pullback_stage
      flat.pullback_pattern_key                = p.pattern_key
      flat.pullback_pattern_length             = p.pattern_length
      flat.pullback_score                      = p.score
      flat.pullback_median_10d_return          = p.median_10d_return
      flat.pullback_win_rate_10d               = p.win_rate_10d
      flat.pullback_fail_rate_10d              = p.fail_rate_10d
      flat.pullback_is_currently_active        = !!p.is_currently_active
      flat.pullback_current_pattern_completion = p.current_pattern_completion
      flat.rare_evidence_tier              = x.evidence_tier
      flat.rare_base4_key                  = x.base4_key
      flat.rare_extended5_key              = x.extended5_key
      flat.rare_extended6_key              = x.extended6_key
      flat.rare_pattern_length             = x.pattern_length
      flat.rare_score                      = x.score
      flat.rare_median_10d_return          = x.median_10d_return
      flat.rare_fail_rate_10d              = x.fail_rate_10d
      flat.rare_is_currently_active        = !!x.is_currently_active
      flat.rare_current_pattern_completion = x.current_pattern_completion
      return flat
    }

    const lines = [COLS.join(',')]
    for (const r of src) {
      const flat = flatten(r)
      lines.push(COLS.map(c => _csvCell(flat[c])).join(','))
    }
    const blob = new Blob([lines.join('\n')], { type: 'text/csv' })
    const url  = URL.createObjectURL(blob)
    const a    = document.createElement('a')
    const date = new Date().toISOString().slice(0, 10)
    a.href     = url

    const parts = [universe, localTf.toUpperCase()]
    if (direction !== 'all') parts.push(direction.toUpperCase())
    if (!scoreBands.has('all') && scoreBands.size > 0) {
      parts.push([...scoreBands].join('+'))
    }
    if (pickedTickers.size > 0) parts.push(`picked${pickedTickers.size}`)
    parts.push(date)
    a.download = `ultra_${parts.join('_')}.csv`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
    setExported(true)
    setTimeout(() => setExported(false), 2000)
  }

  const handleRowEnter = useCallback((e, row) => {
    clearTimeout(hoverTimer.current)
    const rect = e.currentTarget.getBoundingClientRect()
    hoverTimer.current = setTimeout(() => {
      setHoverPopup({ row, pos: { x: rect.right, y: rect.top + rect.height / 2 } })
    }, 350)
  }, [])

  const handleRowLeave = useCallback(() => {
    clearTimeout(hoverTimer.current)
    setHoverPopup(null)
  }, [])

  // cleanup on unmount
  useEffect(() => () => {
    if (pollIvRef.current) clearInterval(pollIvRef.current)
    clearTimeout(hoverTimer.current)
  }, [])

  // ── Poll until done ────────────────────────────────────────────────────────
  const _stopPoll = () => {
    if (pollIvRef.current) { clearInterval(pollIvRef.current); pollIvRef.current = null }
    if (pollToRef.current) { clearTimeout(pollToRef.current); pollToRef.current = null }
  }

  // ULTRA scan progress: phase + per-source state + enrich stage
  const [phase, setPhase]       = useState(null)
  const [phases, setPhases]     = useState({})
  const [sources, setSources]   = useState({})
  const [warnings, setWarnings] = useState([])
  const [stage,  setStage]      = useState(null)   // 'turbo' | 'enrich' | null
  const [enriching, setEnriching] = useState(false)
  const [progressPct,    setProgressPct]    = useState(0)
  const [etaSeconds,     setEtaSeconds]     = useState(null)
  const [elapsedSeconds, setElapsedSeconds] = useState(0)

  // ── Explicit ULTRA scan state ─────────────────────────────────────────────
  // Replaces the implicit `scanning` boolean, which could not distinguish "no scan
  // has ever run" from "a scan is running". After a backend restart the old model
  // left a surviving tab showing "Scanning… 0%" while the backend was idle and had
  // no run at all. The BACKEND is authoritative; localStorage never decides this.
  //   NOT_RUN | RUNNING | COMPLETE | ERROR
  const [scanState, setScanState] = useState('NOT_RUN')
  const [cachedFromPreviousSession, setCachedFromPreviousSession] = useState(false)
  const pollToRef = useRef(null)

  // ── Reconcile against the backend on mount ────────────────────────────────
  // Runs before the UI commits to any state. A tab that survived a backend restart
  // must not keep showing a scan that is not happening, and cached rows in
  // localStorage are results from a PREVIOUS backend session — they are displayable
  // but they are not evidence that a current run completed.
  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const s = await api.ultraScanStatus()
        if (cancelled) return
        if (s?.running) {
          setScanState('RUNNING'); setScanning(true); _poll()
        } else if (s?.error) {
          setScanState('ERROR'); setError(s.error); setScanning(false)
        } else if (s?.completed_at || (s?.turbo_total || 0) > 0) {
          // A run finished in THIS backend session — its results are current, not cached.
          // completed_at is what separates "ran and finished" from "never ran"; both
          // report running=false, and collapsing them would label a completed scan
          // "not run yet".
          setScanState('COMPLETE'); setScanning(false); setEnriching(false)
          setCachedFromPreviousSession(false)
        } else {
          // Backend has no run for this session. Whatever the browser remembers,
          // this is NOT_RUN — no spinner, and 0% is not presented as progress.
          setScanState('NOT_RUN'); setScanning(false); setEnriching(false)
          setProgressPct(0); setEtaSeconds(null); setElapsedSeconds(0)
          setCachedFromPreviousSession(!!_tsGet(localTf, universe)?.results?.length)
        }
      } catch {
        if (cancelled) return
        setScanState('ERROR'); setScanning(false)
        setError('Backend unreachable — press Run ULTRA Scan once it is back.')
      }
    })()
    return () => { cancelled = true; _stopPoll() }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const _poll = () => {
    _stopPoll()  // kill any previous poll before starting a new one
    const activeTf = localTf
    const uni      = universe
    pollIvRef.current = setInterval(() => {
      api.ultraScanStatus()
        .then(s => {
          setPhase(s.phase || null)
          setPhases(s.phases || {})
          setWarnings(s.warnings || [])
          setSources(s.sources || {})
          setStage(s.stage || null)
          setProgressPct(Number.isFinite(s.progress_pct) ? s.progress_pct : 0)
          setEtaSeconds(Number.isFinite(s.eta_seconds) ? s.eta_seconds : null)
          setElapsedSeconds(Number.isFinite(s.elapsed_seconds) ? s.elapsed_seconds : 0)
          if (!s.running) {
            _stopPoll(); setScanning(false); setEnriching(false)
            if (s.error) { setError(s.error); setScanState('ERROR') }
            else { setScanState('COMPLETE'); fetchFreshResults(activeTf, uni) }
          }
        })
        // Backend unreachable: classify and stop. Never sit in RUNNING forever.
        .catch(() => {
          _stopPoll(); setScanning(false); setEnriching(false)
          setScanState('ERROR')
          setError('Backend unreachable while polling — press Run ULTRA Scan to retry.')
        })
    }, 2000)
    // A poll timeout is NOT "still scanning". Ask the backend what is actually true and
    // classify from its answer. The old code assumed completion and called
    // fetchFreshResults, which is how a dead or never-started scan could look finished.
    clearTimeout(pollToRef.current)
    pollToRef.current = setTimeout(() => {
      _stopPoll()
      api.ultraScanStatus()
        .then(s => {
          if (s?.running) { setScanState('RUNNING'); _poll(); return }   // genuinely slow
          setScanning(false); setEnriching(false)
          if (s?.error) { setScanState('ERROR'); setError(s.error) }
          else { setScanState('COMPLETE'); fetchFreshResults(activeTf, uni) }
        })
        .catch(() => {
          setScanning(false); setEnriching(false)
          setScanState('ERROR'); setError('Scan status unknown — backend unreachable.')
        })
    }, 600_000)
  }

  // Stage 2: enrich a subset of tickers. If user has rows checked (via the
  // checkbox column) we enrich those; otherwise we enrich whatever is
  // currently visible after filters. Never enriches the full universe.
  const enrich = () => {
    if (scanning || enriching) return
    // DB mode: data is already fully enriched in Studio DB — no live enrich needed.
    // Just re-fetch the latest snapshot from DB.
    if (sourceMode === 'db') {
      fetchFromDB()
      return
    }
    const targetTickers = pickedTickers.size > 0
      ? results.filter(r => pickedTickers.has(r.ticker)).map(r => r.ticker)
      : results.map(r => r.ticker)
    if (!targetTickers.length) {
      setError('No tickers to enrich — run ULTRA Scan first or adjust filters.')
      return
    }
    setEnriching(true); setError(null)
    setWarnings([]); setPhase(null)
    setProgressPct(0); setEtaSeconds(null); setElapsedSeconds(0)
    // Reset Phase 2 pills to 'pending' so the UI immediately reflects intent
    setPhases(prev => ({
      ...prev,
      stock_stat:      { state: 'pending', message: '' },
      tz_wlnbb:        { state: 'pending', message: '' },
      tz_intelligence: { state: 'pending', message: '' },
      pullback:        { state: 'pending', message: '' },
      rare_reversal:   { state: 'pending', message: '' },
      merge:           { state: 'pending', message: '' },
    }))
    api.ultraScanEnrich({
      universe, tf: localTf, tickers: targetTickers,
      direction, minPrice: 0, maxPrice: 1e9, minVolume: volMin,
    })
      .then(() => _poll())
      .catch(e => {
        setEnriching(false)
        const msg = e?.detail || e?.message || String(e)
        if (msg.includes('409') || msg.toLowerCase().includes('already running')) {
          setError('__stuck__')
        } else {
          setError(msg)
        }
      })
  }

  const scan = () => {
    if (scanning) return  // guard against double-trigger
    // DB mode: instant re-fetch from Studio DB (no long-running scan)
    if (sourceMode === 'db') {
      fetchFromDB()
      return
    }
    // Live mode: traditional 30-60 min scan.
    // RUNNING is entered only AFTER the backend ACCEPTS the trigger. Setting it
    // optimistically is what produced a ten-minute spinner for a scan that never
    // started — e.g. triggering while the backend was still coming up after a boot.
    setError(null); setWarnings([]); setSources({}); setPhases({}); setPhase(null)
    setProgressPct(0); setEtaSeconds(null); setElapsedSeconds(0)
    api.ultraScanTrigger(localTf, universe, {
      lookbackN, partialDay, minVolume: volMin,
      minStoreScore: getCacheBackend() === 'idb' ? 0 : 5,
    })
      .then(() => {
        setScanState('RUNNING'); setScanning(true)
        setCachedFromPreviousSession(false)
        _poll()
      })
      .catch(e => {
        setScanning(false); setScanState('ERROR')
        const msg = e?.detail || e?.message || String(e)
        if (msg.includes('409') || msg.toLowerCase().includes('already running')) {
          setError('__stuck__')
        } else {
          setError(msg)
        }
      })
  }

  // When N>1, override per-signal booleans with age-window check so badges
  // show signals that fired within the last N bars, not just the last bar.
  const withAges = (raw) => {
    if (lookbackN === 1) return raw
    if (!raw._ages && raw.sig_ages) {
      try { raw._ages = JSON.parse(raw.sig_ages) } catch { raw._ages = {} }
    }
    const ov = {}
    for (const [k, a] of Object.entries(raw._ages || {})) ov[k] = a < lookbackN ? 1 : 0
    return { ...raw, ...ov }
  }

  return (
    <div className="flex flex-col h-full bg-md-surface text-md-on-surface text-xs" onMouseLeave={handleRowLeave}>
      {hoverPopup && (
        <MiniChartPopup row={hoverPopup.row} tf={localTf} pos={hoverPopup.pos} onClose={() => setHoverPopup(null)} />
      )}

      {/* ── Row -1: Source mode toggle (Live vs DB) ── */}
      <div className="flex flex-wrap items-center gap-1.5 px-3 py-2 border-b border-white/[0.07] bg-md-surface-con/30">
        <span className="text-md-on-surface-var text-xs w-16 shrink-0">Source</span>
        <button onClick={() => switchSource('db')}
          title="Instant (~1-2s) read from enriched Studio DB. Updated daily at 17:00 ET."
          className={`px-2.5 py-1 rounded text-xs font-medium transition-colors border
            ${sourceMode === 'db'
              ? 'bg-emerald-700/30 text-emerald-300 border-emerald-700/60'
              : 'text-md-on-surface-var border-md-outline-var hover:text-md-on-surface'}`}>
          💾 DB (instant)
        </button>
        <button onClick={() => switchSource('live')}
          title="Full live scan from data source (~30-60 min). Use for intraday refresh."
          className={`px-2.5 py-1 rounded text-xs font-medium transition-colors border
            ${sourceMode === 'live'
              ? 'bg-orange-700/30 text-orange-300 border-orange-700/60'
              : 'text-md-on-surface-var border-md-outline-var hover:text-md-on-surface'}`}>
          ⚡ Live (slow)
        </button>
        <span className="text-[10px] text-md-on-surface-var/70 ml-2">
          {sourceMode === 'db'
            ? `📊 1-2 sec query from Studio DB (${(dbInfo && typeof dbInfo.rows === 'number') ? `${(dbInfo.rows/1e6).toFixed(2)}M bars` : '…'}, last updated ${dbInfo?.date_to ?? '…'})`
            : '⏱ 30-60 min live scan — fresh today\'s bar'}
        </span>

        {/* DB manual refresh button — uses studio.incremental_delta which is
            validated 100% identical to the original CSV import for Pine score
            columns. Safe to click after market close (16:00 ET). */}
        {sourceMode === 'db' && (
          <button
            onClick={triggerDbRefresh}
            disabled={dbRefreshing}
            title="Append yesterday's bar(s) to the DB (SP500 ~20min, +NASDAQ ~25min more)"
            className={`ml-auto flex items-center gap-1 px-2 py-0.5 rounded text-[10px] font-mono border transition-colors
              ${dbRefreshing
                ? 'bg-amber-900/30 text-amber-300 border-amber-700/50 cursor-wait'
                : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-emerald-300 hover:border-emerald-700/50'}`}>
            <span className={dbRefreshing ? 'animate-spin inline-block' : ''}>🔄</span>
            {dbRefreshing
              ? `Updating… ${dbRefreshPct > 0 ? dbRefreshPct.toFixed(0)+'%' : ''}`
              : 'Update DB'}
          </button>
        )}
      </div>

      {/* ── Row 0: Universe selector ── */}
      <div className="flex flex-wrap items-center gap-1.5 px-3 py-2 border-b border-white/[0.07] bg-md-surface-con/50">
        <span className="text-md-on-surface-var text-xs w-16 shrink-0">Universe</span>
        {UNIVERSES.map(u => (
          <button key={u.key}
            onClick={() => { setUniverse(u.key); setAllResults([]); setLastScan(null); try { localStorage.setItem('sachoki_ultra_uni', u.key) } catch {} }}
            title={u.desc}
            className={`px-2.5 py-1 rounded text-xs font-medium transition-colors border
              ${universe === u.key
                ? `${u.cls} border-current bg-md-surface-high`
                : 'text-md-on-surface-var border-md-outline-var hover:text-md-on-surface hover:border-gray-500'}`}>
            {u.label}
          </button>
        ))}
        <span className="text-md-on-surface-var/70 text-xs ml-1">
          {(universe === 'nasdaq' || universe === 'russell2k' || universe === 'all_us') && massiveReady === false && <span className="text-red-400">· MASSIVE_API_KEY not set (will use fallback list)</span>}
          {(universe === 'nasdaq' || universe === 'russell2k' || universe === 'all_us') && massiveReady === true  && <span className="text-green-500">· Massive API ready</span>}
        </span>
      </div>

      {/* ── Row 1: TF + Scan + Direction + Score ── */}
      <div className="flex flex-wrap items-center gap-2 px-3 py-2 border-b border-white/[0.07]">

        {/* TF selector — cached TFs show a green dot */}
        <div className="flex gap-0.5 border border-md-outline-var rounded p-0.5">
          {TF_OPTS.map(t => (
            <button key={t} onClick={() => { setLocalTf(t); setLastScan(null); try { localStorage.setItem('sachoki_ultra_tf', t) } catch {} }}
              title={tfCached[t] ? `${t.toUpperCase()} — cached (instant)` : `${t.toUpperCase()} — no cache, scan first`}
              className={`relative px-2 py-0.5 rounded text-xs font-medium transition-colors
                ${localTf === t ? 'bg-blue-600 text-white' : 'text-md-on-surface-var hover:text-white'}`}>
              {t.toUpperCase()}
              {tfCached[t] && (
                <span className="absolute -top-0.5 -right-0.5 w-1.5 h-1.5 rounded-full bg-green-400" />
              )}
            </button>
          ))}
        </div>

        {/* Stage 1: Turbo-only scan button */}
        <button onClick={scan} disabled={scanning || enriching}
          title="Stage 1 — runs Turbo only. Use Enrich after to fill TZ/WLNBB/Intel/Pullback/Rare for the visible/selected subset."
          className={`px-3 py-1 rounded text-xs font-semibold transition-colors
            ${scanning ? 'bg-gray-700 text-md-on-surface-var cursor-not-allowed'
                       : 'bg-fuchsia-600 hover:bg-fuchsia-500 text-white'}`}>
          {scanning
            ? <span className="animate-pulse">🧬 Scanning…</span>
            : scanState === 'NOT_RUN' ? '🧬 Run ULTRA Scan' : '🧬 ULTRA Scan'}
        </button>

        {/* Explicit scan state. NOT_RUN is a real state, not a 0%-progress scan —
            after a backend restart this is what a surviving tab must show. */}
        {scanState === 'NOT_RUN' && (
          <span className="text-[11px] text-md-on-surface-var px-2"
                title="The backend has no ULTRA run for this session. Press Run ULTRA Scan.">
            not run yet
            {cachedFromPreviousSession && (
              <span className="ml-1 text-amber-400/80">
                · showing cached results from a previous backend session
              </span>
            )}
          </span>
        )}
        {scanState === 'ERROR' && !error && (
          <span className="text-[11px] text-red-400 px-2">scan error — retry</span>
        )}

        {/* Hybrid Preview scan — DB history + TODAY's live forming bar (Massive).
            Recomputes the full signal suite so you can act on today's signals
            before the close / premarket gap. */}
        <button onClick={runPreviewScan} disabled={scanning || enriching || previewing}
          title="Preview — DB history + TODAY's live forming bar from Massive (recomputes all signals). Use shortly before close to act before the premarket gap. ~10-20s for S&P500/NASDAQ. Falls back to DB scan when the market is closed."
          className={`px-3 py-1 rounded text-xs font-semibold transition-colors
            ${previewing ? 'bg-gray-700 text-md-on-surface-var cursor-not-allowed'
                         : 'bg-emerald-600 hover:bg-emerald-500 text-white'}`}>
          {previewing
            ? <span className="animate-pulse">⚡ Preview…</span>
            : '⚡ Preview'}
        </button>
        {previewInfo && (
          <span className={`text-[11px] px-2 py-0.5 rounded border ${
            previewInfo.session === 'open'
              ? 'border-emerald-500/50 text-emerald-300 bg-emerald-900/20'
              : 'border-amber-500/50 text-amber-300 bg-amber-900/20'}`}
            title={previewInfo.note || ''}>
            {previewInfo.session === 'open'
              ? `⚡ live today · ${previewInfo.liveBars ?? '?'} bars${previewInfo.elapsed != null ? ` · ${previewInfo.elapsed}s` : ''}`
              : '🕒 market closed — DB bars'}
          </span>
        )}

        {/* Stage 2: enrich the visible / selected subset
            (in DB mode this stage is unnecessary — DB already has full enrichment) */}
        {sourceMode !== 'db' && (() => {
          const enrichCount = pickedTickers.size > 0 ? pickedTickers.size : results.length
          const enrichLabel = pickedTickers.size > 0
            ? `✨ Enrich ${enrichCount} selected`
            : `✨ Enrich ${enrichCount} visible`
          return (
            <button onClick={enrich} disabled={scanning || enriching || enrichCount === 0}
              title="Stage 2 — generates an ULTRA-private subset stock_stat for these tickers (extracted from canonical when present), then runs TZ/WLNBB + TZ Intel + Pullback + Rare Reversal."
              className={`px-3 py-1 rounded text-xs font-semibold transition-colors
                ${enriching ? 'bg-gray-700 text-md-on-surface-var cursor-not-allowed'
                  : enrichCount === 0
                    ? 'bg-md-surface-high text-md-on-surface-var/70 cursor-not-allowed border border-md-outline-var'
                    : pickedTickers.size > 0
                      ? 'bg-amber-600 hover:bg-amber-500 text-black'
                      : 'bg-emerald-700 hover:bg-emerald-600 text-white'}`}>
              {enriching
                ? <span className="animate-pulse">✨ Enriching…</span>
                : enrichLabel}
            </button>
          )
        })()}

        {/* Partial-day preview toggle — include today's open bar */}
        <button onClick={() => setPartialDay(p => !p)}
          title="Include today's in-progress daily bar (scan during market hours for an early read)"
          className={`px-2.5 py-1 rounded text-xs font-medium transition-colors border
            ${partialDay
              ? 'border-amber-400 text-amber-300 bg-amber-900/30'
              : 'border-md-outline-var text-md-on-surface-var hover:text-md-on-surface hover:border-gray-500'}`}>
          ~Preview
        </button>

        {/* Export button */}
        <button onClick={exportTickers} disabled={results.length === 0}
          title={pickedTickers.size > 0 ? `Copy ${pickedTickers.size} selected tickers` : 'Copy all visible tickers (TradingView watchlist)'}
          className={`px-2.5 py-1 rounded text-xs font-medium transition-colors border
            ${exported
              ? 'border-lime-500 text-lime-300 bg-lime-900/30'
              : results.length === 0
                ? 'border-md-outline-var text-md-on-surface-var/70 cursor-not-allowed'
                : pickedTickers.size > 0
                  ? 'border-yellow-500 text-yellow-300 bg-yellow-900/20 hover:border-yellow-400'
                  : 'border-gray-600 text-md-on-surface hover:border-gray-400 hover:text-white'}`}>
          {exported
            ? '✓ Copied'
            : pickedTickers.size > 0
              ? `⬇ Export (${pickedTickers.size})`
              : '⬇ Export'}
        </button>

        {/* TradingView watchlist export — ticker symbols only (.txt, comma-sep) */}
        <button onClick={exportTradingView} disabled={results.length === 0}
          title={pickedTickers.size > 0
            ? `Export ${pickedTickers.size} selected tickers as a TradingView watchlist (.txt)`
            : 'Export all filtered tickers as a TradingView watchlist (.txt, comma-separated)'}
          className={`px-2.5 py-1 rounded text-xs font-medium transition-colors border
            ${tvExported
              ? 'border-lime-500 text-lime-300 bg-lime-900/30'
              : results.length === 0
                ? 'border-md-outline-var text-md-on-surface-var/70 cursor-not-allowed'
                : 'border-sky-600 text-sky-300 hover:border-sky-400 hover:text-white'}`}>
          {tvExported ? '✓ TV list' : '📈 TV list'}
        </button>
        {/* Clear selection */}
        {pickedTickers.size > 0 && (
          <button onClick={() => setPickedTickers(new Set())}
            className="px-2 py-0.5 rounded text-xs text-md-on-surface-var hover:text-red-400 transition-colors"
            title="Clear row selection">
            ✕ deselect
          </button>
        )}

        {/* Direction */}
        <div className="flex gap-0.5">
          {DIR_OPTS.map(d => (
            <button key={d.key} onClick={() => setDirection(d.key)}
              className={`px-2 py-0.5 rounded text-xs transition-colors
                ${direction === d.key ? 'bg-indigo-600 text-white' : 'bg-md-surface-high text-md-on-surface-var hover:text-white'}`}>
              {d.label}
            </button>
          ))}
        </div>

        {/* Score bands — multi-select */}
        <div className="flex gap-0.5 ml-1">
          {SCORE_BANDS.map(b => {
            const active = scoreBands.has(b.key)
            return (
              <button key={b.key}
                onClick={() => {
                  setScoreBands(prev => {
                    const next = new Set(prev)
                    if (b.key === 'all') {
                      return new Set(['all'])
                    }
                    next.delete('all')
                    if (next.has(b.key)) {
                      next.delete(b.key)
                      if (next.size === 0) next.add('all')
                    } else {
                      next.add(b.key)
                    }
                    return next
                  })
                }}
                className={`px-2 py-0.5 rounded text-xs transition-colors
                  ${active ? 'bg-amber-600 text-black font-semibold' : 'bg-md-surface-high text-md-on-surface-var hover:text-white'}`}>
                {b.label}
              </button>
            )
          })}
        </div>

        {/* N= lookback selector — client-side, no rescan needed */}
        <div className="flex items-center gap-0.5 ml-1" title="Signal lookback window — no rescan needed">
          <span className="text-md-on-surface-var text-xs mr-0.5">N=</span>
          {[1, 3, 5, 10].map(n => (
            <button key={n} onClick={() => setLookbackN(n)}
              className={`px-2 py-0.5 rounded text-xs transition-colors
                ${lookbackN === n ? 'bg-indigo-700 text-white font-semibold' : 'bg-md-surface-high text-md-on-surface-var hover:text-white'}`}
              title={n === 1 ? 'Current bar only' : `Signal fired in last ${n} bars`}>
              {n}d
            </button>
          ))}
        </div>

        {/* Volume filter */}
        <div className="flex items-center gap-0.5 ml-1" title="Avg daily volume filter">
          <span className="text-md-on-surface-var text-xs mr-0.5">Vol</span>
          {[
            { label: 'All',   min: 0,         max: 0         },
            { label: '<100K', min: 0,         max: 100_000   },
            { label: '100K+', min: 100_000,   max: 0         },
            { label: '500K+', min: 500_000,   max: 0         },
            { label: '1M+',   min: 1_000_000, max: 0         },
            { label: '5M+',   min: 5_000_000, max: 0         },
          ].map(({ label, min, max }) => {
            const active = volMin === min && volMax === max
            return (
              <button key={label}
                onClick={() => { setVolMin(min); setVolMax(max) }}
                className={`px-2 py-0.5 rounded text-xs transition-colors
                  ${active ? 'bg-cyan-700 text-white font-semibold' : 'bg-md-surface-high text-md-on-surface-var hover:text-white'}`}
                title={label === 'All' ? 'No volume filter' : label === '<100K' ? 'Avg volume < 100K' : `Avg volume ≥ ${min.toLocaleString()}`}>
                {label}
              </button>
            )
          })}
        </div>

        {/* Price range filter */}
        <div className="flex items-center gap-1 ml-2" title="Last-price band (e.g. 21–89 = the quality zone; blank = off)">
          <span className="text-md-on-surface-var text-xs">$</span>
          <input type="number" value={priceMin} onChange={e => setPriceMin(e.target.value)}
            placeholder="min" min="0"
            className={`w-14 px-1.5 py-0.5 rounded text-xs bg-md-surface-high border ${priceMin !== '' ? 'border-cyan-600 text-white' : 'border-transparent text-md-on-surface-var'} focus:outline-none focus:border-cyan-500`} />
          <span className="text-md-on-surface-var/50 text-xs">–</span>
          <input type="number" value={priceMax} onChange={e => setPriceMax(e.target.value)}
            placeholder="max" min="0"
            className={`w-14 px-1.5 py-0.5 rounded text-xs bg-md-surface-high border ${priceMax !== '' ? 'border-cyan-600 text-white' : 'border-transparent text-md-on-surface-var'} focus:outline-none focus:border-cyan-500`} />
          {(priceMin !== '' || priceMax !== '') && (
            <button onClick={() => { setPriceMin(''); setPriceMax('') }}
              className="text-xs text-md-on-surface-var hover:text-white px-0.5" title="Clear price filter">✕</button>
          )}
        </div>

        {/* Stats + stale warning */}
        <span className="ml-auto text-md-on-surface-var/70 shrink-0 flex items-center gap-1.5">
          {partialDay && <span className="text-amber-400 font-medium">~preview</span>}
          {results.length} / {allResults.length}
          {lastScan && (() => {
            const ageH = (Date.now() - new Date(lastScan).getTime()) / 3_600_000
            return (
              <span className={ageH > 2 ? 'text-yellow-500' : 'text-md-on-surface-var/70'}>
                {ageH > 2 ? '⚠ ' : ''}{lastScan.slice(0,16).replace('T',' ')}
                {ageH > 2 && ` (${Math.floor(ageH)}h ago)`}
              </span>
            )
          })()}
        </span>
      </div>

      {/* ── Advanced Filters toggle (SIG / Sector / RTB Phase) ── */}
      <div className="flex items-center gap-2 px-3 py-1.5 border-b border-white/[0.07] bg-md-surface-con/20">
        <button
          onClick={() => setShowAdvanced(a => !a)}
          className={`px-2.5 py-0.5 rounded text-xs font-medium shrink-0 transition-colors border ${
            showAdvanced || selSigs.size > 0 || secFilter || rtbPhase
              ? 'bg-indigo-900/50 text-indigo-300 border-indigo-600'
              : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
          }`}>
          Advanced Filters {showAdvanced ? '▴' : '▾'}{selSigs.size > 0 ? ` · ${selSigs.size} sig` : ''}{secFilter ? ' · sector' : ''}{rtbPhase ? ` · RTB:${rtbPhase}` : ''}
        </button>
        {(selSigs.size > 0 || secFilter || rtbPhase) && (
          <button onClick={() => { setSelSigs(new Set()); setSecFilter(''); setRtbPhase('') }}
            className="px-2 py-0.5 rounded text-xs shrink-0 bg-red-900/40 text-red-400 hover:bg-red-900/60">
            ✕ clear adv
          </button>
        )}
      </div>

      {/* ── Advanced Filters (collapsible) ── */}
      {showAdvanced && (
        <div className="border-b border-white/[0.07] bg-md-surface-con/30">
          <div className="flex flex-wrap items-center gap-x-1 gap-y-1 px-3 py-2 border-b border-white/[0.07]/30">
            <span className="text-md-on-surface-var text-xs shrink-0 mr-0.5">SIG</span>
            <button onClick={() => setSelSigs(new Set())}
              className={`px-2 py-0.5 rounded text-xs shrink-0 ${selSigs.size === 0 ? 'bg-blue-600 text-white' : 'bg-md-surface-high text-md-on-surface-var hover:text-white'}`}>
              All
            </button>
            {/* ⛓ SEQUENCE — pick the bar a chip lands on. The grid below is not
                duplicated: the same chips, routed to a different set. */}
            <div className="w-full flex items-center gap-1.5 mb-1.5 pb-1.5 border-b border-white/[0.07]">
              <span className="text-[10px] text-md-on-surface-var/70 font-semibold uppercase tracking-wide">⛓ seq</span>
              {[['main', 'ANY bar (N)'],
                ...Array.from({length: SEQ_BARS}, (_, i) =>
                  [i, i === SEQ_BARS - 1 ? 'today' : `bar −${SEQ_BARS - 1 - i}`])
               ].map(([t, lbl]) => {
                const n = t === 'main' ? selSigs.size : seqSlots[t].size
                const on = seqTarget === t
                return (
                  <button key={String(t)} onClick={() => setSeqTarget(t)}
                    title={t === 'main'
                      ? 'The existing behaviour: a chip matches if it fired anywhere in the last N bars. Order is not checked.'
                      : 'Chips clicked now apply to THIS bar only. Leave a slot empty and it constrains nothing, so a two-bar rule is a three-slot rule with one blank.'}
                    className={`px-2 py-0.5 rounded text-xs shrink-0 border transition-colors ${
                      on ? 'bg-indigo-800 text-indigo-100 border-indigo-400'
                         : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'}`}>
                    {lbl}{n > 0 ? ` · ${n}` : ''}
                  </button>
                )
              })}
              {seqActive && (
                <>
                  <span className="text-[10px] text-md-on-surface-var/60 ml-1">
                    {seqSlots.map((x, i) => x.size ? (i === SEQ_BARS - 1 ? '0' : `−${SEQ_BARS - 1 - i}`) : '·').join(' ')}
                  </span>
                  <button onClick={clearSeq}
                    className="px-2 py-0.5 rounded text-xs bg-red-900/40 text-red-400 hover:bg-red-900/60">
                    ✕ seq
                  </button>
                </>
              )}
            </div>
            {visibleSigGroups.map((s, i) =>
              s.divider
                ? (s.label
                    ? <span key={`div-${i}`} className="text-[10px] text-md-on-surface-var/70 select-none px-1 self-center font-semibold uppercase tracking-wide">{s.label}</span>
                    : <span key={`div-${i}`} className="text-gray-700 select-none px-0.5 self-center">·</span>)
                : (
                  <button key={s.key} onClick={() => toggleSig(s.key)}
                    title={s.hint}
                    className={`px-2 py-0.5 rounded text-xs shrink-0 transition-colors
                      ${(seqTarget === 'main' ? selSigs : seqSlots[seqTarget]).has(s.key)
                          ? `${s.cls} ${seqTarget === 'main' ? 'bg-gray-700' : 'bg-indigo-800 ring-1 ring-indigo-400'} font-semibold`
                          : 'bg-md-surface-high text-md-on-surface-var hover:text-white'}`}>
                    {s.label}
                  </button>
                )
            )}
            {selSigs.size > 0 && (
              <button onClick={() => setSelSigs(new Set())}
                className="ml-2 px-2 py-0.5 rounded text-xs shrink-0 bg-red-900/40 text-red-400 hover:bg-red-900/60">
                ✕ clear
              </button>
            )}
          </div>

          {/* ── 260523 filters: AD-FRESH / AD-CLUSTER / WYC Phase ── */}
          <div className="flex flex-wrap items-center gap-x-1 gap-y-1 px-3 py-1.5 border-b border-white/[0.07]/30">
            <span className="text-md-on-surface-var text-xs shrink-0 mr-0.5 w-16">260523</span>
            <button onClick={() => setAdFreshFilter(adFreshFilter ? null : true)}
              title="AD-FRESH: Z1G/Z2G → T4/T6/T2G/T2 in lower 50% of 20-bar range"
              className={`px-2 py-0.5 rounded text-xs shrink-0 transition-colors border ${
                adFreshFilter
                  ? 'bg-fuchsia-900/50 text-fuchsia-200 border-fuchsia-600 font-semibold'
                  : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
              }`}>
              AD-FRESH ★
            </button>
            <button onClick={() => setAdClusterFilter(adClusterFilter ? null : true)}
              title="AD-CLUSTER: 2+ AD-FRESH within 8-bar window — highest conviction"
              className={`px-2 py-0.5 rounded text-xs shrink-0 transition-colors border ${
                adClusterFilter
                  ? 'bg-cyan-900/60 text-cyan-200 border-cyan-500 font-semibold'
                  : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
              }`}>
              AD-CLUSTER ★★
            </button>
            <span className="text-md-on-surface-var text-xs shrink-0 ml-2 mr-1">WYC:</span>
            {['', 'SPRING', 'UTAD', 'SOS', 'ACC_TR', 'DIST_TR', 'MARKUP', 'MKDN'].map(p => (
              <button key={p || 'all'} onClick={() => setWycPhaseFilter(p)}
                className={`px-2 py-0.5 rounded text-xs shrink-0 transition-colors ${
                  wycPhaseFilter === p
                    ? 'bg-teal-900/60 text-teal-200 font-semibold'
                    : 'bg-md-surface-high text-md-on-surface-var hover:text-white'
                }`}>
                {p || 'All'}
              </button>
            ))}
            {(adFreshFilter || adClusterFilter || wycPhaseFilter) && (
              <button
                onClick={() => { setAdFreshFilter(null); setAdClusterFilter(null); setWycPhaseFilter('') }}
                className="ml-2 px-2 py-0.5 rounded text-xs shrink-0 bg-red-900/40 text-red-400 hover:bg-red-900/60">
                ✕ clear 260523
              </button>
            )}
          </div>

          {/* ── 260523 v3.1: HH/LH/HL/LL swing filter ── */}
          <div className="flex flex-wrap items-center gap-x-1 gap-y-1 px-3 py-1.5 border-b border-white/[0.07]/30">
            <span className="text-md-on-surface-var text-xs shrink-0 mr-0.5 w-16">Swing</span>
            {[
              { label: 'Any',           value: '',      cls: '' },
              { label: 'HL (bullish)',  value: 'HL',    cls: 'bg-green-900/60 text-green-200' },
              { label: 'LL (bounce)',   value: 'LL',    cls: 'bg-emerald-900/60 text-emerald-200' },
              { label: 'HH (top)',      value: 'HH',    cls: 'bg-orange-900/60 text-orange-200' },
              { label: 'LH (bearish)',  value: 'LH',    cls: 'bg-red-900/60 text-red-200' },
              { label: 'Any pivot',     value: 'pivot', cls: 'bg-blue-900/60 text-blue-200' },
            ].map(opt => (
              <button key={opt.value || 'all'} onClick={() => setSwingTypeFilter(opt.value)}
                title={`Empirical SP500 1D: HL win 77%, LL win 76%, HH win 25%, LH win 27%`}
                className={`px-2 py-0.5 rounded text-xs shrink-0 transition-colors ${
                  swingTypeFilter === opt.value
                    ? `${opt.cls} font-semibold`
                    : 'bg-md-surface-high text-md-on-surface-var hover:text-white'
                }`}>
                {opt.label}
              </button>
            ))}
          </div>

          {/* ── 260523 PREBREAK + WYC additional ── */}
          <div className="flex flex-wrap items-center gap-x-1 gap-y-1 px-3 py-1.5 border-b border-white/[0.07]/30">
            <span className="text-md-on-surface-var text-xs shrink-0 mr-0.5 w-16">PREBREAK</span>
            {[
              { label: 'Any',    value: '',      cls: '' },
              { label: 'PRIME★', value: 'prime', cls: 'bg-lime-900/60 text-lime-200' },
              { label: 'READY',  value: 'ready', cls: 'bg-orange-900/60 text-orange-200' },
              { label: 'WATCH',  value: 'watch', cls: 'bg-yellow-900/60 text-yellow-200' },
            ].map(opt => (
              <button key={opt.value || 'all'} onClick={() => setPrebreakTier(opt.value)}
                title="PREBREAK score tier (≥45 / ≥28 / ≥18)"
                className={`px-2 py-0.5 rounded text-xs shrink-0 transition-colors ${
                  prebreakTier === opt.value
                    ? `${opt.cls} font-semibold`
                    : 'bg-md-surface-high text-md-on-surface-var hover:text-white'
                }`}>
                {opt.label}
              </button>
            ))}
            <button onClick={() => setPbLvbo(pbLvbo ? null : true)}
              title="LRC → LVBO: volume compression then bull breakout"
              className={`px-2 py-0.5 rounded text-xs shrink-0 border ${
                pbLvbo ? 'bg-teal-900/60 text-teal-200 border-teal-500 font-semibold'
                       : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
              }`}>LVBO</button>
            <button onClick={() => setPbStopCause(pbStopCause ? null : true)}
              title="W-PHASE: STOP+CAUSE (Wyckoff accumulation context)"
              className={`px-2 py-0.5 rounded text-xs shrink-0 border ${
                pbStopCause ? 'bg-teal-900/60 text-teal-200 border-teal-500 font-semibold'
                            : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
              }`}>W-PHASE</button>
            <button onClick={() => setPbWvfConfirm(pbWvfConfirm ? null : true)}
              title="WVF spike (capitulation volume from line5 VX token)"
              className={`px-2 py-0.5 rounded text-xs shrink-0 border ${
                pbWvfConfirm ? 'bg-blue-900/60 text-blue-200 border-blue-500 font-semibold'
                             : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
              }`}>WVF</button>
            <button onClick={() => setPbPpRtv(pbPpRtv ? null : true)}
              title="PP+RTV: Williams pivot + return-to-value within 1.5% of EMA20"
              className={`px-2 py-0.5 rounded text-xs shrink-0 border ${
                pbPpRtv ? 'bg-yellow-900/60 text-yellow-200 border-yellow-500 font-semibold'
                        : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
              }`}>PP+RTV</button>
            <button onClick={() => setPbFlyCdC(pbFlyCdC ? null : true)}
              title="FLY-CD-C: confirmed FLY-CD (8-bar low + close>EMA9 + bull + prev-bull)"
              className={`px-2 py-0.5 rounded text-xs shrink-0 border ${
                pbFlyCdC ? 'bg-lime-900/60 text-lime-200 border-lime-500 font-semibold'
                         : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
              }`}>FLY-CD-C</button>
            <button onClick={() => setPbFollow(pbFollow ? null : true)}
              title="FOLLOW: follow-spring (T9/T3/T1G) → confirm (T4/T6/T2G/T2) breakout above spring high"
              className={`px-2 py-0.5 rounded text-xs shrink-0 border ${
                pbFollow ? 'bg-green-900/60 text-green-200 border-green-500 font-semibold'
                         : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
              }`}>FOLLOW</button>
            <span className="text-md-on-surface-var text-xs shrink-0 ml-2 mr-1">Macro:</span>
            {[
              { label: 'Any',        value: null,  cls: '' },
              { label: 'No penalty', value: false, cls: 'bg-green-900/60 text-green-200' },
              { label: 'Penalty',    value: true,  cls: 'bg-red-900/60 text-red-200' },
            ].map(opt => (
              <button key={String(opt.value)} onClick={() => setPbMacroPen(opt.value)}
                className={`px-2 py-0.5 rounded text-xs shrink-0 ${
                  pbMacroPen === opt.value ? `${opt.cls} font-semibold`
                                          : 'bg-md-surface-high text-md-on-surface-var hover:text-white'
                }`}>{opt.label}</button>
            ))}
            <span className="text-md-on-surface-var text-xs shrink-0 ml-2 mr-1">WYC+:</span>
            <button onClick={() => setWycInTr(wycInTr ? null : true)}
              title="In Trading Range (ACC_TR or DIST_TR)"
              className={`px-2 py-0.5 rounded text-xs shrink-0 border ${
                wycInTr ? 'bg-gray-700/80 text-gray-200 border-gray-500 font-semibold'
                        : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
              }`}>In TR</button>
            {(prebreakTier || pbLvbo || pbStopCause || pbWvfConfirm || pbPpRtv || pbFlyCdC || pbFollow ||
              pbMacroPen !== null || wycInTr) && (
              <button onClick={() => {
                setPrebreakTier(''); setPbLvbo(null); setPbStopCause(null);
                setPbWvfConfirm(null); setPbPpRtv(null); setPbFlyCdC(null); setPbFollow(null);
                setPbMacroPen(null); setWycInTr(null);
              }} className="ml-2 px-2 py-0.5 rounded text-xs shrink-0 bg-red-900/40 text-red-400 hover:bg-red-900/60">
                ✕ clear PREBREAK
              </button>
            )}
          </div>

          {/* Sector filter */}
          <div className="flex flex-wrap items-center gap-x-1 gap-y-1 px-3 py-1.5 border-b border-white/[0.07]/30">
            <span className="text-md-on-surface-var text-xs shrink-0 mr-0.5 w-16">Sector</span>
        {[
              { label: 'All',  val: '',            cls: 'text-md-on-surface-var' },
              { label: 'XLC',  val: 'communicat',  cls: 'text-blue-300' },
              { label: 'XLY',  val: 'cyclical',    cls: 'text-orange-300' },
              { label: 'XLP',  val: 'defensive',   cls: 'text-green-300' },
              { label: 'XLE',  val: 'energy',      cls: 'text-yellow-300' },
              { label: 'XLF',  val: 'financ',      cls: 'text-cyan-300' },
              { label: 'XLV',  val: 'health',      cls: 'text-red-300' },
              { label: 'XLI',  val: 'industrial',  cls: 'text-sky-300' },
              { label: 'XLB',  val: 'material',    cls: 'text-lime-300' },
              { label: 'XLRE', val: 'real estate', cls: 'text-amber-300' },
              { label: 'XLK',  val: 'tech',        cls: 'text-violet-300' },
              { label: 'XLU',  val: 'utilities',   cls: 'text-teal-300' },
            ].map(s => (
              <button key={s.val} onClick={() => setSecFilter(s.val)}
                className={`px-2 py-0.5 rounded text-xs shrink-0 transition-colors
                  ${secFilter === s.val
                    ? `${s.cls} bg-gray-700 font-semibold`
                    : 'bg-md-surface-high text-md-on-surface-var hover:text-white'}`}>
                {s.label}
              </button>
            ))}
            {secFilter && Object.keys(sectorMap).length === 0 && allResults.some(r => !r.sector) && (
              <span className="ml-1 text-md-on-surface-var/70 text-xs animate-pulse">
                — loading sectors…
              </span>
            )}
          </div>

          {/* RTB Phase filter */}
          <div className="flex flex-wrap items-center gap-x-1 gap-y-1 px-3 py-1.5">
            <span className="text-md-on-surface-var text-xs shrink-0 mr-0.5 w-16">RTB Phase</span>
        {[
              { label: 'All',    val: '', cls: 'text-md-on-surface-var' },
              { label: 'A — Build',    val: 'A', cls: 'text-md-on-surface',
                title: 'Build phase: base forming, drying volume, accumulation — no turn yet' },
              { label: 'B — Turn',     val: 'B', cls: 'text-sky-300',
                title: 'Turn phase: first real reversal bar, momentum changed — not yet breakout-ready' },
              { label: 'C — Ready',    val: 'C', cls: 'text-lime-300',
                title: 'Ready phase: prime pre-breakout stalking zone (1-3 bars before breakout)' },
              { label: 'D — Late',     val: 'D', cls: 'text-orange-300',
                title: 'Late/breakout-live: move already launching — chase risk high' },
            ].map(s => (
              <button key={s.val} onClick={() => setRtbPhase(s.val)}
                title={s.title}
                className={`px-2 py-0.5 rounded text-xs shrink-0 transition-colors
                  ${rtbPhase === s.val
                    ? `${s.cls} bg-gray-700 font-semibold ring-1 ring-gray-500`
                    : 'bg-md-surface-high text-md-on-surface-var hover:text-white'}`}>
                {s.label}
              </button>
            ))}
            {rtbPhase && (
              <span className="ml-2 text-md-on-surface-var/70 text-xs">
                {results.length} ticker{results.length !== 1 ? 's' : ''}
              </span>
            )}
          </div>
        </div>
      )}

      {/* ── BUY / MTF-confirmation filter row (validated 2026-07-19, project_mtf_confirmation) ── */}
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1 px-3 py-1.5 border-b border-white/[0.07] bg-md-surface-con/20">
        <span className="text-md-on-surface-var text-xs shrink-0 mr-0.5 w-16">BUY</span>
        {[['rev', '🟢 REV✓', 'REV-buy WITH intraday echo (+1.09…+1.17%, 6/6yr)'],
          ['brk', '🔵 BRK', 'BRK-buy — RSI>50 cross + low turbo (+0.57%, weaker)'],
          ['conf', '1-3 conf', '1D buy_score≥60 AND ≥1 intraday TF confirms ≥60 (+1.3…+1.6%, 5-6/6yr)'],
          ['turn', '①②③ turn', 'loose up-turn with ≥1 intraday REV-echo (+0.7…+1.05%, 5/6yr)'],
          ['h4', '▲ 4H', 'a 4H REV-trigger fired inside the latest daily bar — early entry existed (+0.84pp, 6/6yr)'],
          ['lheavy', 'L-heavy', 'latest bar = RED L34 (close<open, absorbed weakness) — the triple-confluence leg: 🟢REV✓ + ▲4H + red-L34 = +2.05%/PF1.32/5-6yr, TRAIN +2.27 ≈ TEST +1.85 (era-balanced). Green L34 is excluded: on reversal bars it is the trap type (−1.01%, PF 0.87). ⚠ On deep buy_score≥60 days heavy-L HURTS (knife volume) — stack it with REV, not with bare score-days'],
          ['edge', 'EDGE✓', 'a validated Edge-board setup fired TODAY (edge_replay masks, backtest-identical): CAP/QZC/D+L1/G3/⚡G3A/ATM/SPR/Z11/L43/WSH/H1B/ENG/ZRT/HB15/RTB/P55/PAR/🎯3-4 — see the EDGE column for which'],
          ['edgerev', 'EDGE🟢', 'PREMIUM combo (validated 2026-07-20): a location-reversal edge (QZC/D+L1/RTB/P55) fired TODAY on the same bar as 🟢REV — QZC +1.65→+2.69% (median flips positive), D+L1 +1.60→+3.46% (TRAIN turns +), RTB +2.04% 6/6yr, P55 +1.89%. Only these four: WSH/ZRT are HURT by REV, and CAP/Z11/L43/⚡G3A never coincide with it (different bar anatomy)'],
          ['seq34', '🧬SEQ', 'a frozen-OOS-verified 2-4-bar robust sequence completed TODAY (tier OOS_VERIFIED, OOS path-sim win≥55%, ps_med>0; bright chip ≥60; rules mined 2021-23, verified 2024-26). 🏆 on the chip = DSR≥0.6 selection-proof (the only fully-trustable tier). Same engine as the Robust Seqs tab'],
          ['ctxup', '⤴CTX', 'the BUY-signal fire has a BOOSTER preceding-sequence context: the 2-4 bars before it historically LIFT that signal\'s fwd-20 up% (era-consistent cells only, buyseq_context.json). Hover the ⤴ chip for the sequence and numbers'],
          ['ctxdn', '⤵CTX', 'SUPPRESSOR preceding-sequence context — the bars before this fire historically LOWER the signal\'s up% (chop/weak-T chains). Consider skipping or demanding extra confluence'],
          ['any', '✅ any buy', 'any of: 🟢REV✓ / 🔵BRK / conf≥1 / ①②③'],
          ['veto', '⚠️ veto', 'the validated SKIP group: REV without echo (−1.07%) or score-conf 0/3 (−2.10%)']].map(([k, label, title]) => (
          <button key={k}
            onClick={() => setBuyFilter(f => { const x = new Set(f); x.has(k) ? x.delete(k) : x.add(k); return x })}
            title={title}
            className={`px-2.5 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
              buyFilter.has(k)
                ? (k === 'veto'
                  ? 'bg-red-900/60 text-red-300 border-red-600 ring-1 ring-red-500'
                  : 'bg-cyan-900/60 text-cyan-200 border-cyan-600 ring-1 ring-cyan-500')
                : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
            }`}>
            {label}
          </button>
        ))}
      </div>

      {/* ── ▽△ Bottom-Anatomy filter row (2026-09-09) — the ▽△ column was sortable but unfilterable.
             Same verdicts as the Superchart ▽△ row; source = the shared anatomyStore latest-bar map. ── */}
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1 px-3 py-1.5 border-b border-white/[0.07] bg-md-surface-con/20">
        <span className="text-md-on-surface-var text-xs shrink-0 mr-0.5 w-16" title="▽△ Bottom-Anatomy — nested 1D→1H→15m DETECTOR of the latest bar's shape (necessary, not sufficient: it says 'this looks like a bottom', not 'buy'). Verdict chips are OR'd; ≥5 ANDs on top.">▽△</span>
        {[['durable', '🔻💪 durable', 'bg-amber-700/70 text-amber-100 border-amber-500 ring-1 ring-amber-400',
           '🔻💪 = anatomy REVERSAL + 🏆RS-intact (close/SPY > EMA200) — the durable/tradeable subset. RS is the discriminator that separates a real bottom from a mid-range absorption pause (🧊 coil-floor logic). This is the one to start from.'],
          ['struct', '🔻 bottom', 'bg-orange-900/70 text-orange-300 border-orange-600 ring-1 ring-orange-500',
           '🔻 = STRUCTURAL bottom-anatomy: at a genuine floor (held tight-coil, or low-in-range with a real key level / absorption) + multi-TF Z-absorption + intraday reversal. ~1.37× enriched for real swing lows at 76% recall — but NO RS gate, so lower precision. Includes the 🔻💪 rows (pick 🔻💪 alone for just the durable ones).'],
          ['shake', '🌀 shakeout', 'bg-violet-900/70 text-violet-200 border-violet-600 ring-1 ring-violet-500',
           '🌀 = SPRING / terminal shakeout — bearish day at a held floor with a WEAK close, but a late hi-vol T-reversal on 1H/15m (the tell the daily bar hides). The 🔻 detector misses these by construction (low-late + weak-close). Intraday-only signal: base −1.57 → with the 1H tell −0.48 vs random −2.52 — a less-bad cell, NOT a buy.'],
          ['cont', '🔺 markup', 'bg-green-900/70 text-green-300 border-green-600 ring-1 ring-green-500',
           '🔺 = CONTINUATION / markup: upper-range close, momentum, higher-low, not at a floor. Context for "this is already running", the opposite pole from 🔻.'],
          ['s5', '≥5 score', 'bg-cyan-900/60 text-cyan-200 border-cyan-600 ring-1 ring-cyan-500',
           'anatomy score ≥5 of 8 (location 0-2 + absorption 0-3 + reversal 0-3). ANDs with the verdict chips — a deeper-evidence subset of whichever verdict you picked.']].map(([k, label, on, title]) => (
          <button key={k}
            onClick={() => setAnatFilter(f => { const x = new Set(f); x.has(k) ? x.delete(k) : x.add(k); return x })}
            title={title}
            className={`px-2.5 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
              anatFilter.has(k) ? on : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'}`}>
            {label}
          </button>
        ))}
        {anatFilter.size > 0 && (
          <span className="ml-1 text-md-on-surface-var/70 text-xs">
            {results.length} ticker{results.length !== 1 ? 's' : ''}
            {!anatomyReady() && <span className="ml-1 text-amber-400">· loading ▽△…</span>}
          </span>
        )}
      </div>

      {/* ── Row 5: Profile Sweet Spot filter ── */}
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1 px-3 py-1.5 border-b border-white/[0.07] bg-md-surface-con/20">
        <span className="text-md-on-surface-var text-xs shrink-0 mr-0.5 w-16">Profile</span>
        <button
          onClick={() => setSweetSpotFilter(f => !f)}
          title="Show only tickers in sweet_spot_active=true AND late_warning=false. Sorted by profile_score DESC, then turbo_score DESC."
          className={`px-2.5 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
            sweetSpotFilter
              ? 'bg-green-900/60 text-green-300 border-green-600 ring-1 ring-green-500'
              : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
          }`}>
          ⭐ Sweet Spot
        </button>
        <button
          onClick={() => setBuildingFilter(f => !f)}
          title="Show only tickers with profile_category = BUILDING. Sorted by profile_score DESC."
          className={`px-2.5 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
            buildingFilter
              ? 'bg-yellow-900/60 text-yellow-300 border-yellow-600 ring-1 ring-yellow-500'
              : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
          }`}>
          ↑ Building
        </button>
        <button
          onClick={() => setWatchFilter(f => !f)}
          title="Show only tickers with profile_category = WATCH. Sorted by profile_score DESC."
          className={`px-2.5 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
            watchFilter
              ? 'bg-gray-700 text-md-on-surface border-gray-500 ring-1 ring-gray-400'
              : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
          }`}>
          👁 Watch
        </button>
        <span className="text-md-on-surface-var text-xs shrink-0 ml-2">🎯 HV-Zone:</span>
        {ZONE_TIERS.map(t => {
          const on = zoneTiers[t.key]
          const tierSet = zoneTierSets[t.key]
          // Static Tailwind classes (no string interpolation — JIT requires literals).
          const onCls = {
            sky:  'bg-sky-900/60 text-sky-300 border-sky-600 ring-1 ring-sky-500',
            teal: 'bg-teal-900/60 text-teal-300 border-teal-600 ring-1 ring-teal-500',
            cyan: 'bg-cyan-900/60 text-cyan-300 border-cyan-600 ring-1 ring-cyan-500',
          }[t.color]
          return (
            <button
              key={t.key}
              onClick={async () => {
                const next = !on
                setZoneTiers(s => ({ ...s, [t.key]: next }))
                if (next && !zoneTierSets[t.key]) {
                  setZoneRetestBusy(true)
                  try {
                    const q = new URLSearchParams({ vol_min: t.vmin })
                    if (t.vmax) q.set('vol_max', t.vmax)
                    const r = await fetch(`/api/zone-retest/tickers?${q}`).then(x => x.json())
                    setZoneTierSets(s => ({ ...s, [t.key]: new Set(r.tickers || []) }))
                  } catch { /* leave set empty */ }
                  finally { setZoneRetestBusy(false) }
                }
              }}
              title={`Volume spike ${t.label} on a bullish bar 8-90 days back; price LEFT zone upward and is now back inside [low,high].`}
              className={`px-2 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
                on ? onCls : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
              }`}>
              {zoneRetestBusy && on && !tierSet ? '⏳' : ''} {t.label}
              {on && tierSet && <span className="ml-1 text-[10px] opacity-80">{tierSet.size}</span>}
            </button>
          )
        })}
        <button
          onClick={async () => {
            const next = !gannFilter
            setGannFilter(next)
            if (next && !gannSet) {
              setGannBusy(true)
              try {
                const r = await fetch('/api/gann-zones/tickers').then(x => x.json())
                setGannSet(new Set(r.tickers || []))
              } catch { /* keep empty */ } finally { setGannBusy(false) }
            }
          }}
          title="Gann zones: tickers whose CURRENT close sits inside the [low,high] of the highest-high bar OR the lowest-low bar in the last 90 days. Magnet S/R levels in Gann's framework."
          className={`px-2 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
            gannFilter
              ? 'bg-amber-900/60 text-amber-300 border-amber-600 ring-1 ring-amber-500'
              : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
          }`}>
          {gannBusy ? '⏳' : '📐'} Gann
          {gannFilter && gannSet && <span className="ml-1 text-[10px] opacity-80">{gannSet.size}</span>}
        </button>
        <button
          onClick={() => setVbwFilter(f => ({ ...f, vb: !f.vb }))}
          title="Volume class VB (Very Big): the latest bar's volume is ≥ 2×mean + 1·std (TZ_WLNBB Bollinger bucket) — exceptional interest."
          className={`px-2 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
            vbwFilter.vb
              ? 'bg-red-900/60 text-red-300 border-red-600 ring-1 ring-red-500'
              : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
          }`}>
          🔊 VB
        </button>
        <button
          onClick={() => setVbwFilter(f => ({ ...f, w: !f.w }))}
          title="Volume class W (Weak): the latest bar's volume is < mean − 1·std (TZ_WLNBB Bollinger bucket) — dried-up / low interest."
          className={`px-2 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
            vbwFilter.w
              ? 'bg-slate-600/70 text-slate-100 border-slate-400 ring-1 ring-slate-400'
              : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
          }`}>
          🔉 W
        </button>
        <button
          onClick={() => {
            const v = !atomicFilter; setAtomicFilter(v)
            if (v) setShortFilter(false)   // Atomic (long) & Short are opposite directions — mutually exclusive
            // cached results may predate the atomic-age enrichment → pull fresh (fast, no re-scan)
            if (v && !allResults.some(r => r.atomic_age != null)) fetchEnrichmentMerge(localTf, universe)
          }}
          title="⚛ Atomic: 5-year-validated 'weak-close gap-up' edge — a bull T-signal that closes WEAK (close=O, below prior body) on a gap-up bar. Orthogonal to turbo_score (which is anti-predictive at the high end). Backtest +0.84 sp500 / +0.70 r2k, positive 5/6 years."
          className={`px-2 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
            atomicFilter
              ? 'bg-fuchsia-900/60 text-fuchsia-200 border-fuchsia-500 ring-1 ring-fuchsia-500'
              : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
          }`}>
          ⚛ Atomic{atomicFilter ? ` ${results.filter(r => r.atomic_match && r.atomic_age != null && r.atomic_age < (lookbackN || 1)).length}` : ''}
        </button>
        <button
          onClick={() => {
            const v = !shortFilter; setShortFilter(v)
            if (v) { setAtomicFilter(false); setCapFilter(false) }   // Short (short) vs Atomic/Capit (long) — mutually exclusive
            // cached results may predate the short-age enrichment → pull fresh (fast, no re-scan)
            if (v && !allResults.some(r => r.short_age != null)) fetchEnrichmentMerge(localTf, universe)
          }}
          title="⚡ Blow-off Short: 5-year-validated fade edge — a volume CLIMAX bar (V×5/V×10) that closes WEAK (close=O) = exhaustion → short the fade. +10.8…+13.4 clip25-lift, 6/6 years (WHAT_ACTUALLY_WORKS.md). ⚠️ short tail risk: ~1/20 trade is a >50% squeeze — size small, hard stop."
          className={`px-2 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
            shortFilter
              ? 'bg-red-900/60 text-red-200 border-red-500 ring-1 ring-red-500'
              : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
          }`}>
          ⚡ Short{shortFilter ? ` ${results.filter(r => r.short_match && r.short_age != null && r.short_age < (lookbackN || 1)).length}` : ''}
        </button>
        <button
          onClick={() => {
            const v = !capFilter; setCapFilter(v)
            if (v) setShortFilter(false)   // Capit (long) vs Short — opposite directions
            if (v && !allResults.some(r => r.cap_age != null)) fetchEnrichmentMerge(localTf, universe)
          }}
          title="💥 Capitulation Bounce — the one edge that survived rigorous gap-aware path-sim (+4.6% cost-adj, 5/6 yrs) while every breakout/momentum signal failed. CORE: L34/L46 VSA bar + RSI<30 + CCI<-100 (aligned deep oversold = Wyckoff selling-climax spring; CCI is the knife-guard). REFINED: 20-bar drawdown sweet-spot is a -45..-10% flush (+1.12, 6/6); a >-60% collapse is a falling knife (-4.65, 0/6) → excluded. RED flush + ~2x vol + coil (BLUE/FRI64) add. ⚠️ EXIT = HOLD ~15-20 days, NO stop — a tight stop cuts the bounce (validated: hold +4.6% vs -15%-stop +1.5%); sit through ~-7% MAE; diversify (a minority still knife)."
          className={`px-2 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
            capFilter
              ? 'bg-amber-900/60 text-amber-200 border-amber-500 ring-1 ring-amber-500'
              : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
          }`}>
          💥 Capit{capFilter ? ` ${results.filter(r => r.cap_match && r.cap_age != null && r.cap_age < (lookbackN || 1)).length}` : ''}
        </button>
        <button
          onClick={() => {
            const v = !momFilter; setMomFilter(v)
            if (v) setShortFilter(false)   // Momentum (long) vs Short — opposite directions
            if (v && !allResults.some(r => r.mom_age != null)) fetchEnrichmentMerge(localTf, universe)
          }}
          title="🚀 Momentum Zone-Dense: the markup mirror of Capit (L_LINE_DISCOVERIES.md) — buy the COIL inside an uptrend, not the breakout. A dense L34/L46 accumulation churn (≥6 in last 10 bars) + RSI 40-65 (markup band) + squeeze & load (the coil) + price flat/rising. median fwd_10d +1.15, 5/6 years. NB: density-only or chasing strength (L3 markup, price already +10%) does NOT work — the edge is the compression, not the chase."
          className={`px-2 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
            momFilter
              ? 'bg-violet-900/60 text-violet-200 border-violet-500 ring-1 ring-violet-500'
              : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
          }`}>
          🚀 Mom{momFilter ? ` ${results.filter(r => r.mom_match && r.mom_age != null && r.mom_age < (lookbackN || 1)).length}` : ''}
        </button>
        <button
          onClick={() => {
            const v = !postCapitFilter; setPostCapitFilter(v)
            if (v) setShortFilter(false)   // Capit→Atom (long) vs Short — opposite directions
            // cached results may predate the post-capit enrichment → pull fresh (fast, no re-scan)
            if (v && !allResults.some(r => r.atomic_age != null)) fetchEnrichmentMerge(localTf, universe)
          }}
          title="🔥 Capit→Atom confluence — a weak-close gap-up (Atomic) that FOLLOWS a recent B+ capitulation on the same ticker (≤15d). The capitulation confirms the bottom; the gap-up is the continuation entry. Validated this session: rich+capit≤10d win 67%, med +4.2% vs +1.4% baseline; survives price-control, dedup (510 tickers) and ex-cluster. The premium subset of the Atomic edge — also rescues good <$16 names that the price floor alone would drop."
          className={`px-2 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
            postCapitFilter
              ? 'bg-orange-900/60 text-orange-200 border-orange-400 ring-1 ring-orange-400'
              : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
          }`}>
          🔥 Capit→Atom{postCapitFilter ? ` ${results.filter(r => r.atomic_post_capit).length}` : ''}
        </button>
        <button
          onClick={() => {
            const v = !vol3t5Filter; setVol3t5Filter(v)
            if (v && !allResults.some(r => r.vol3t5_age != null)) fetchEnrichmentMerge(localTf, universe)
          }}
          title="📈 T5·Vol↑↑↑: T5 bar + 3-bar rising volume + RSI drop 2-10pt. Absorption with volume recovery. SP500: exp+0.79% med+0.64% win53%. RSI drop sweet spot: 2-10pt (bigger=panic, smaller=noise)."
          className={`px-2 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
            vol3t5Filter
              ? 'bg-teal-900/60 text-teal-200 border-teal-500 ring-1 ring-teal-500'
              : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
          }`}>
          📈 T5·Vol{vol3t5Filter ? ` ${results.filter(r => r.vol3t5_match && r.vol3t5_age != null && r.vol3t5_age < (lookbackN || 1)).length}` : ''}
        </button>
        <button
          onClick={() => {
            const v = !vol3t9Filter; setVol3t9Filter(v)
            if (v && !allResults.some(r => r.vol3t9_age != null)) fetchEnrichmentMerge(localTf, universe)
          }}
          title="📈 T9·Vol↑↑↑: T9 bar + 3-bar rising volume + RSI 25-40. Premium=RSI30-35 (exp+1.43%). T9 is always RSI-rising (momentum), volume recovery confirms. SP500: med+0.74% win53%."
          className={`px-2 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
            vol3t9Filter
              ? 'bg-sky-900/60 text-sky-200 border-sky-500 ring-1 ring-sky-500'
              : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
          }`}>
          📈 T9·Vol{vol3t9Filter ? ` ${results.filter(r => r.vol3t9_match && r.vol3t9_age != null && r.vol3t9_age < (lookbackN || 1)).length}` : ''}
        </button>
        <button
          onClick={() => {
            const v = !vol3t12Filter; setVol3t12Filter(v)
            if (v && !allResults.some(r => r.vol3t12_age != null)) fetchEnrichmentMerge(localTf, universe)
          }}
          title="📈 T12·Vol↑↑↑: T12 bar + 3-bar rising volume + RSI drop 2-10pt. Premium=RSI30-35+2bar RSI fall (med+0.70% win51.8%). T12+RSI30-35: exp+4.22%, win49.5%. Best absorption+recovery combo."
          className={`px-2 py-0.5 rounded text-xs font-semibold shrink-0 transition-colors border ${
            vol3t12Filter
              ? 'bg-violet-900/60 text-violet-200 border-violet-500 ring-1 ring-violet-500'
              : 'bg-md-surface-high text-md-on-surface-var border-md-outline-var hover:text-white'
          }`}>
          📈 T12·Vol{vol3t12Filter ? ` ${results.filter(r => r.vol3t12_match && r.vol3t12_age != null && r.vol3t12_age < (lookbackN || 1)).length}` : ''}
        </button>
        {(sweetSpotFilter || buildingFilter || watchFilter) && (
          <span className="text-xs text-md-on-surface-var">
            {results.length} ticker{results.length !== 1 ? 's' : ''} · sorted by Pf Score
          </span>
        )}
        {(sweetSpotFilter || buildingFilter || watchFilter) && (
          <button onClick={() => { setSweetSpotFilter(false); setBuildingFilter(false); setWatchFilter(false) }}
            className="ml-1 px-2 py-0.5 rounded text-xs shrink-0 bg-red-900/40 text-red-400 hover:bg-red-900/60">
            ✕ clear
          </button>
        )}
        <span className="ml-auto text-md-on-surface-var/70 text-xs">
          Profile score is additive context only — does not replace canonical score
          {(universe === 'nasdaq' || universe === 'russell2k' || universe === 'all_us') &&
            <span className="ml-1 text-amber-600"> · NASDAQ profile is experimental</span>}
          {universe === 'split' &&
            <span className="ml-1 text-sky-700"> · SPLIT lifecycle: D-7→D+90 window</span>}
          {universe === 'zone' &&
            <span className="ml-1 text-emerald-600"> · ZONE: close inside an active HV zone (≥5× vol, ≤90d old)</span>}
        </span>
      </div>

      {/* Source-status badges (informational; do not control row visibility) */}
      {(Object.keys(sources).length > 0 || warnings.length > 0) && (
        <div className="px-4 py-1.5 border-b border-white/[0.07] bg-md-surface-con/40 flex flex-wrap items-center gap-2 text-[11px]">
          {['turbo', 'tz_wlnbb', 'tz_intelligence', 'pullback', 'rare_reversal'].map(k => {
            const s = sources[k]
            if (!s) return null
            const cls = s.ok
              ? 'bg-emerald-900/50 text-emerald-200 border-emerald-700/60'
              : 'bg-red-900/30 text-red-300 border-red-700/40'
            return (
              <span key={k} className={`px-1.5 py-0.5 rounded border ${cls}`}>
                {k}: {s.ok ? `ok (${s.count})` : 'unavailable'}
              </span>
            )
          })}
          {warnings.length > 0 && (
            <span className="text-amber-300 ml-1" title={warnings.join('\n')}>
              ⚠ {warnings.length} warning{warnings.length > 1 ? 's' : ''}
            </span>
          )}
        </div>
      )}

      {/* Progress / error */}
      {(scanning || enriching) && (
        <div className="px-4 py-1.5 border-b border-white/[0.07] bg-fuchsia-950/30 text-fuchsia-300">
          <div className="animate-pulse">
            🧬 ULTRA — {UNIVERSES.find(u => u.key === universe)?.label ?? universe} ({localTf.toUpperCase()})
            {' · '}{enriching ? 'Stage 2: enriching subset' : 'Stage 1: Turbo'}
            {phase ? ` · phase: ${phase}` : ''}
          </div>
          {/* ── Progress bar ─────────────────────────────────────────────── */}
          {(() => {
            const fmtTime = (s) => {
              if (s == null || !Number.isFinite(s)) return '—'
              if (s < 60) return `${Math.round(s)}s`
              const m = Math.floor(s / 60); const ss = Math.round(s - m * 60)
              return ss === 0 ? `${m}m` : `${m}m${ss.toString().padStart(2, '0')}s`
            }
            const pct = Math.max(0, Math.min(100, progressPct || 0))
            return (
              <div className="mt-1.5">
                <div className="h-1.5 rounded bg-white/10 overflow-hidden">
                  <div
                    className="h-full bg-fuchsia-400 transition-all duration-500 ease-out"
                    style={{ width: `${pct}%` }}
                  />
                </div>
                <div className="flex justify-between mt-0.5 text-[10px] text-md-on-surface-var">
                  <span>{pct.toFixed(0)}%</span>
                  <span>elapsed {fmtTime(elapsedSeconds)} · ETA {fmtTime(etaSeconds)}</span>
                </div>
              </div>
            )
          })()}
          {Object.keys(phases).length > 0 && (() => {
            // Group pills by pipeline phase so the user can see the
            // dependency-aware execution at a glance.
            const PHASE_GROUPS = [
              { label: 'Phase 1 (parallel)', keys: ['turbo', 'stock_stat'] },
              { label: 'Phase 2 (parallel)', keys: ['tz_wlnbb', 'tz_intelligence', 'pullback', 'rare_reversal'] },
              { label: 'Phase 3',            keys: ['merge'] },
            ]
            const stateCls = (s) =>
                s === 'ok'      ? 'text-emerald-400'
              : s === 'running' ? 'text-fuchsia-300'
              : s === 'error'   ? 'text-red-400'
              : s === 'skipped' ? 'text-amber-300'
              : 'text-md-on-surface-var'
            return (
              <div className="flex flex-col gap-1 mt-1 text-[10px]">
                {PHASE_GROUPS.map(g => (
                  <div key={g.label} className="flex flex-wrap items-center gap-1">
                    <span className="text-md-on-surface-var/70 mr-1">{g.label}:</span>
                    {g.keys.map(p => {
                      const ph = phases[p]
                      if (!ph) return null
                      return (
                        <span key={p} className={`px-1.5 py-0.5 border border-md-outline-var rounded ${stateCls(ph.state)}`}
                              title={ph.message || ph.state}>
                          {p}: {ph.state}
                        </span>
                      )
                    })}
                  </div>
                ))}
              </div>
            )
          })()}
        </div>
      )}
      {error && (
        <div className="px-4 py-1.5 text-md-error border-b border-white/[0.07] flex items-center gap-3">
          {error === '__stuck__'
            ? <span>Another ULTRA scan is in progress — wait for it to finish, then try again</span>
            : error}
        </div>
      )}

      {/* ── Table (ScannerDataGrid) ── */}
      <ScannerDataGrid
        results={results.map(r => {
          const pm = pmData[r.ticker]
          return { ...withAges(r), pm_chg_pct: pm?.pm_chg_pct ?? null, pm_price: pm?.pm_price ?? null }
        })}
        onSelectTicker={onSelectTicker}
        onWatchlistToggle={_pwlToggle}
        localTf={localTf}
        pickedTickers={pickedTickers}
        onTogglePicked={togglePicked}
        sortBy={sortBy}
        sortDir={sortDir}
        onSort={toggleSort}
        isLoading={(scanning || enriching) && results.length === 0}
        effectiveScoreCol={effectiveScoreCol}
        universe={universe}
        variant="ultra"
        pmData={pmData}
        allPicked={results.length > 0 && results.every(r => pickedTickers.has(r.ticker))}
        onPickAll={checked => {
          if (checked) setPickedTickers(new Set(results.map(r => r.ticker)))
          else setPickedTickers(new Set())
        }}
        handleRowEnter={handleRowEnter}
        handleRowLeave={handleRowLeave}
      />
    </div>
  )
}
