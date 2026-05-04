"""TZ/WLNBB Analyzer — version and parameters."""
TZ_WLNBB_VERSION = "2026-05-05-tz-wlnbb-v1"

# Engulfing defaults
USE_WICK = False
MIN_BODY_RATIO = 1.0

# Doji threshold (body/range)
DOJI_THRESH = 0.05

# WLNBB Bollinger Band period
WLNBB_MA_PERIOD = 20

# T priority order (highest priority first)
T_PRIORITY = ["T4", "T6", "T1G", "T2G", "T1", "T2", "T9", "T10", "T3", "T11", "T5"]
T_PRIORITY_RANK = {s: i+1 for i, s in enumerate(T_PRIORITY)}

# Z priority order
Z_PRIORITY = ["Z4", "Z6", "Z1G", "Z2G", "Z1", "Z2", "Z8", "Z9", "Z10", "Z3", "Z11", "Z5", "Z12", "Z7"]
Z_PRIORITY_RANK = {s: i+1 for i, s in enumerate(Z_PRIORITY)}

# PREUP priority
PREUP_PRIORITY = ["P66", "P55", "P89", "P3", "P2", "P50"]
# PREDN priority
PREDN_PRIORITY = ["D66", "D55", "D89", "D3", "D2", "D50"]

# Known signals registry
KNOWN_T_SIGNALS = set(T_PRIORITY)
KNOWN_Z_SIGNALS = set(Z_PRIORITY)
KNOWN_L_SIGNALS = {"L1","L2","L3","L4","L5","L6","L34","L43","L64","L22"}
KNOWN_PREUP_SIGNALS = set(PREUP_PRIORITY)
KNOWN_PREDN_SIGNALS = set(PREDN_PRIORITY)
ALL_KNOWN_SIGNALS = KNOWN_T_SIGNALS | KNOWN_Z_SIGNALS | KNOWN_L_SIGNALS | KNOWN_PREUP_SIGNALS | KNOWN_PREDN_SIGNALS
