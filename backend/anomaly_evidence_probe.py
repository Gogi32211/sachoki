"""Evidence collection for MASSIVE_SECURITY_LINEAGE_ANOMALY_QUALIFICATION_V1.

Read-only against every canonical artifact. Fetches SEC primary documents for the 14
candidates and extracts the sentences that actually state a trading date, archiving bytes
and digests. Classification happens afterwards, from the quotes — not in here.

The distinction this exists to protect: "Massive served a bar" is a vendor observation.
"the shares commenced regular-way trading on the NYSE on X" is a listing fact. Only the
second can move a coverage boundary, so only the second is collected as primary.
"""
from __future__ import annotations
import hashlib, html, json, os, re, sys, time                           # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import requests                                                         # noqa: E402

UA = {"User-Agent": "Sachoki Quant Research research@sachoki.local"}
ARC = "/Volumes/QUANT_RESEARCH/artifacts/provenance/anomaly_qualification"
EXEC = "MASSIVE_SECURITY_LINEAGE_V7_CANDIDATE_EXECUTION_V1.json"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
BRANCH_A = ["SW", "WBD", "CEG", "Q", "KVUE", "PSKY", "SNDK", "FDXF", "TKO", "RDDT", "HONA"]
BRANCH_B = ["GEHC", "SOLV", "VLTO"]
FORMS = ("8-A12B", "8-K", "424B4", "424B3", "425", "10-12B", "10-12B/A", "8-K/A", "6-K",
         "S-1", "S-1/A", "POS AM", "FWP")
PAT = re.compile(
    r"[^.]{0,240}(?:regular[- ]way|when[- ]issued|commenc\w+ trading|beg[ia]n\w* trading|"
    r"first day of trading|will trade on|expected to begin trading|listed and traded)"
    r"[^.]{0,300}\.", re.I)
_out: dict = {}


def sha_save(name, raw):
    os.makedirs(ARC, exist_ok=True)
    d = hashlib.sha256(raw).hexdigest()
    with open(os.path.join(ARC, f"{name}_{d[:8]}.htm"), "wb") as f:
        f.write(raw)
    return d


def text_of(raw):
    t = re.sub(r"(?s)<[^>]+>", " ", raw.decode("utf-8", "replace"))
    return re.sub(r"\s+", " ", html.unescape(t))


def subs(cik):
    r = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json",
                     headers=UA, timeout=90)
    if r.status_code != 200:
        return []
    j = r.json()["filings"]["recent"]
    return [dict(form=j["form"][i], filed=j["filingDate"][i],
                 acc=j["accessionNumber"][i], doc=j["primaryDocument"][i])
            for i in range(len(j["form"]))]


def probe(tk, cik, lo, hi):
    rows = [f for f in subs(cik) if lo <= f["filed"] <= hi and f["form"] in FORMS]
    rows.sort(key=lambda x: (x["filed"], FORMS.index(x["form"])))
    hits, seen = [], 0
    for f in rows:
        if seen >= 6:
            break
        url = (f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
               f"{f['acc'].replace('-','')}/{f['doc']}")
        try:
            r = requests.get(url, headers=UA, timeout=120)
        except Exception:
            continue
        if r.status_code != 200 or not r.content:
            continue
        seen += 1
        t = text_of(r.content)
        ms = [m.group(0).strip() for m in PAT.finditer(t)]
        ms = [m for m in ms if re.search(r"20\d\d|\b\d{1,2},\s*20\d\d", m)][:4]
        if ms:
            dg = sha_save(f"{tk}_{f['form'].replace('/','')}_{f['filed'].replace('-','')}",
                          r.content)
            hits.append(dict(form=f["form"], filed=f["filed"], accession=f["acc"],
                             document=f["doc"], sha256=dg, quotes=ms))
        time.sleep(0.15)
    return dict(filings_scanned=seen, candidates=len(rows), hits=hits)


def main():
    ex = json.load(open(EXEC))
    v6 = {s["current_ticker"]: s for s in json.load(open(LINEAGE))["securities"]}
    find = {f["current_ticker"]: f for f in ex["full_503_audit"]["findings"]}
    import pandas as pd
    for tk in BRANCH_A + BRANCH_B:
        f = find[tk]
        eb = f["probe"]["earliest_with_bars"]
        lo = str((pd.Timestamp(eb) - pd.Timedelta(days=50)).date())
        hi = str((pd.Timestamp(f["v6_coverage_start"]) + pd.Timedelta(days=12)).date())
        res = probe(tk, v6[tk]["cik"], lo, hi)
        _out[tk] = dict(cik=v6[tk]["cik"], earliest_massive_bar=eb,
                        v6_start=f["v6_coverage_start"], window=[lo, hi], **res)
        print(f"\n===== {tk}  cik={v6[tk]['cik']}  bars≥{eb}  v6={f['v6_coverage_start']}")
        for h in res["hits"]:
            print(f"  [{h['form']} {h['filed']} sha={h['sha256'][:8]}]")
            for q in h["quotes"]:
                print(f"     · {q[:300]}")
        if not res["hits"]:
            print("  (no statement matched)")
    with open("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
              "4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/anomaly_evidence.json",
              "w") as fh:
        json.dump(_out, fh, indent=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
