import json, pathlib
from collections import Counter
from graph import SOURCES, NAVER_QUERIES

path = pathlib.Path("store/metrics.jsonl")
rows = [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()] \
       if path.exists() else []

if not rows:
    print("아직 기록이 없습니다. run.py를 한 번 이상 돌린 뒤 다시 실행하세요.")
else:
    print(f"쌓인 실행 기록: {len(rows)}줄\n")
    pub = Counter()
    for r in rows:
        pub.update(r.get("by_source", {}))
    print(f"{'소스':<12}{'발행 기여':>9}")
    print("-" * 23)
    for name, _ in list(SOURCES) + NAVER_QUERIES:  # 기여가 0인 소스도 보여야 한다
        print(f"{name:<12}{pub.get(name, 0):>9}")

    print("\n수집 → 선별 → 취재 → 발행")
    for r in rows[-5:]:
        print(f"  {r['collected']:>4} → {r['picked']:>3} → {r['drafted']:>3}"
              f" → {r['published']:>3}  ({r.get('delivery', '')})")

    last = rows[-1]
    if last.get("select_reasons"):
        print("\n[선별 근거 - 최신 실행]")
        for s in last["select_reasons"]:
            print(f"  [{s['source']}/{s.get('outlet', '')}] {s['title'][:44]}")
            print(f"    → {s.get('reason')} (event: {s.get('event')})")
    if last.get("verify_notes"):
        print("\n[검수 기록 - 최신 실행]")
        for n in last["verify_notes"]:
            print(f"  {n['action']}: {(n.get('title') or '')[:44]} — {n.get('why', '')[:80]}")
