import operator, feedparser, requests, re, os, json, pathlib, time, trafilatura, yaml
from datetime import datetime, timedelta, timezone
from typing import Annotated, TypedDict
from langgraph.graph import StateGraph, START, END
from langgraph.types import Send
from openai import OpenAI
from pydantic import BaseModel, Field

try:
    env = dict(l.strip().split("=", 1) for l in open("keys.env", encoding="utf-8")
               if "=" in l and not l.strip().startswith("#"))
except FileNotFoundError:
    env = {}                                     # CI에선 워크플로 env로 들어온다
for k in ("OPENAI_API_KEY", "DISCORD_WEBHOOK_URL",
           "NAVER_CLIENT_ID", "NAVER_CLIENT_SECRET"):
    if k in env and env[k]:
        os.environ[k] = env[k]
client = OpenAI()
MODEL = os.environ.get("MODEL", "gpt-4.1-mini")

UA = {"User-Agent": "Mozilla/5.0 (newsletter-agent-course)"}
# 직접 언론사 RSS 5곳 — 기사 URL이 곧 원문이라 본문 확보·검증이 된다.
# 포털 조사 결과: Google뉴스RSS는 본문 링크가 서버 요청에 400 차단,
# MSN·Daum은 공개 RSS 없음, Naver는 Search API 키 필요 → REPORT 2절에 실측 기록.
SOURCES = [
    ("하우징헤럴드", "https://www.housingherald.co.kr/rss/allArticle.xml"),
    ("한경부동산",   "https://www.hankyung.com/feed/realestate"),
    ("동아경제",     "http://rss.donga.com/economy.xml"),
    ("매경",         "https://www.mk.co.kr/rss/30000001/"),
    ("연합경제",     "https://www.yna.co.kr/rss/economy.xml"),
]
TOPICS = ["개발·정비", "거래·시세", "정책·규제", "세금·대출"]

# Naver 뉴스검색 API 6번째 경로. 무료 한도 25,000건/일 — 80%(20,000건) 도달 시 사용 중단.
NAVER_API = "https://openapi.naver.com/v1/search/news.json"
NAVER_DAILY_LIMIT, NAVER_STOP_RATIO = 25000, 0.8
NAVER_QUERIES = [
    ("Naver-개발", "서울 재개발 재건축"),
    ("Naver-거래", "서울 아파트 실거래"),
    ("Naver-정책", "부동산 정책 대출"),
    ("Naver-세금", "부동산 세금 양도세"),
    ("Naver-경기", "경기 GTX 신도시"),
]

def _naver_usage_path():
    p = pathlib.Path("store/naver_usage.json")
    p.parent.mkdir(exist_ok=True)
    return p

def naver_calls_left():
    """오늘 남은 호출 수 (80% 한도 기준). ponytail: Naver는 잔여량 헤더가 없어 로컬 카운터가 전부다."""
    today = datetime.now(KST).strftime("%Y-%m-%d")
    try:
        u = json.loads(_naver_usage_path().read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        u = {}
    if u.get("date") != today:
        return int(NAVER_DAILY_LIMIT * NAVER_STOP_RATIO)
    return int(NAVER_DAILY_LIMIT * NAVER_STOP_RATIO) - u.get("calls", 0)

def naver_charge(n=1):
    today = datetime.now(KST).strftime("%Y-%m-%d")
    try:
        u = json.loads(_naver_usage_path().read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        u = {}
    if u.get("date") != today:
        u = {"date": today, "calls": 0}
    u["calls"] += n
    _naver_usage_path().write_text(json.dumps(u), encoding="utf-8")

def naver_search(label, query, cutoff):
    """쿼리당 1호출(display=100, 날짜순). (호출횟수, 기사목록)."""
    import email.utils, urllib.parse
    from urllib.parse import urlparse
    headers = {"X-Naver-Client-Id": os.environ["NAVER_CLIENT_ID"],
               "X-Naver-Client-Secret": os.environ["NAVER_CLIENT_SECRET"]}
    params = {"query": query, "display": 100, "sort": "date"}
    r = requests.get(NAVER_API, headers=headers, params=params, timeout=20)
    r.raise_for_status()
    naver_charge(1)
    items = []
    for it in r.json().get("items", []):
        try:
            at = email.utils.parsedate_to_datetime(it.get("pubDate", ""))
        except (TypeError, ValueError):
            continue
        if not at or at < cutoff:
            continue
        if at.tzinfo is None:
            at = at.replace(tzinfo=KST)
        url = it.get("originallink") or it.get("link", "")  # 원문 직결 우선
        title = re.sub(r"</?b>", "", it.get("title", "")).strip()
        outlet = urlparse(url).netloc.replace("www.", "")
        items.append({"title": title, "url": url, "source": label,
                      "outlet": outlet, "at": at,
                      "summary": strip_tags(it.get("description", ""))[:300],
                      "rss_full": strip_tags(it.get("description", ""))})
    return 1, items

KST = timezone(timedelta(hours=9))

def strip_tags(s):
    return re.sub(r"<[^>]+>", "", s or "").strip()

def published_at(entry):
    t = entry.get("published_parsed") or entry.get("updated_parsed")
    return datetime(*t[:6], tzinfo=timezone.utc) if t else None

class Brief(TypedDict):
    hours:     int                              # 수집 시간 창(시간)
    collected: list                             # ① 수집한 기사
    picked:    list                             # ② 선별해 남긴 기사 (+reason/event)
    drafted:   Annotated[list, operator.add]    # ③ 취재한 초안 — 워커들이 나눠 채운다
    verified:  list                             # ④ 검수 통과분
    verify_notes: list                          # 검수 실패·재생성 기록 (metrics 근거)
    log:       Annotated[list, operator.add]    # 무슨 일이 있었는지

def collect(s: dict) -> dict:                  # ① 자료 수집
    cutoff = datetime.now(timezone.utc) - timedelta(hours=s["hours"])
    items, dead, seen, per_source = [], [], set(), {}
    for name, url in SOURCES:
        try:
            feed = feedparser.parse(requests.get(url, headers=UA, timeout=20).content)
        except Exception:
            dead.append(name)                   # 한 곳이 죽어도 나머지는 계속
            continue
        n = 0
        for e in feed.entries:
            at = published_at(e)
            if not at or at < cutoff:           # 시간 창 밖이거나 날짜가 없으면 버린다
                continue
            key = e.link.split("?")[0].rstrip("/")       # 추적용 꼬리표를 떼고 비교
            if key in seen:
                continue
            seen.add(key)
            outlet = (e.get("source") or {}).get("title", "") or ""
            title = re.sub(r"\s*-\s*[^-]+$", "", e.title).strip()
            full = strip_tags(e.get("summary", ""))
            items.append({"title": title or e.title, "url": e.link,
                          "source": name, "outlet": outlet, "at": at,
                          "summary": full[:300], "rss_full": full})
            n += 1
        per_source[name] = n
    # ⑥ Naver 뉴스검색 API (키 없으면 스킵, 80% 한도 도달 시 중단)
    if not (os.environ.get("NAVER_CLIENT_ID") and os.environ.get("NAVER_CLIENT_SECRET")):
        dead.append("Naver(키 없음)")
    elif naver_calls_left() < len(NAVER_QUERIES):
        dead.append(f"Naver(80% 한도 도달, 잔여 {naver_calls_left()})")
    else:
        for label, q in NAVER_QUERIES:
            try:
                _, found = naver_search(label, q, cutoff)
            except Exception:
                dead.append(label)              # 한 쿼리가 죽어도 나머지는 계속
                continue
            n = 0
            for it in found:
                key = it["url"].split("?")[0].rstrip("/")
                if key in seen:
                    continue
                seen.add(key)
                items.append(it)
                n += 1
            per_source[label] = n
    # 소스 라운드로빈 재배열: 연합경제(120건/72h) 쏠림 방지, 예선 각 묶음에 소스가 고루 섞인다
    by_src, order, mixed = {}, list(per_source), []
    for it in items:
        by_src.setdefault(it["source"], []).append(it)
    for i in range(max((len(v) for v in by_src.values()), default=0)):
        for nm in order:
            if nm in by_src and i < len(by_src[nm]):
                mixed.append(by_src[nm][i])
    items = mixed
    detail = ", ".join(f"{k} {v}" for k, v in per_source.items())
    return {"collected": items,
            "log": [f"① 수집   {s['hours']}시간 창 · {len(items)}건 ({detail})"
                    + (f" · 응답 없음 {dead}" if dead else "")]}

class Pick(BaseModel):
    index: int = Field(description="후보 목록에서의 번호")
    reason: str = Field(description="왜 골랐는지 한 문장")
    event: str = Field(description="개발·정비 / 거래·시세 / 정책·규제 / 세금·대출 중 하나. 같은 사건이면 같은 라벨")

class Shortlist(BaseModel):
    picks: list[Pick]

BATCH, TARGET, PRE = 40, 5, 8  # 예선 묶음 크기, 최종 발행 건수, 묶음당 생존 수

CFG = yaml.safe_load(pathlib.Path("audience.yaml").read_text(encoding="utf-8"))

def build_criteria(cfg):                        # 설정 → 프롬프트 문단
    out = [f"독자는 {cfg['독자']['누구']}입니다.",
           f"이미 아는 것: {cfg['독자']['이미_아는_것']}",
           "", "중요도 기준 (위에 있을수록 우선):"]
    out += [f"- {x}" for x in cfg["중요도_기준"]]
    out += ["", "버릴 것:"]
    out += [f"- {x}" for x in cfg["버릴_것"]]
    return "\n".join(out)

CRITERIA = build_criteria(CFG)
SYS      = (f"당신은 서울·경기 부동산 전문가입니다. 독자는 {CFG['독자']['누구']}입니다.\n"
            "반드시 한국어로 쓰세요. 과장·호들갑 표현은 쓰지 마세요. 금액·면적·일정은 원문 그대로 옮기세요.")

def ask_picks(items, n):
    listing = "\n".join(f"{i}. [{it.get('outlet') or it['source']}] {it['title']}" for i, it in enumerate(items))
    sys = (f"{CRITERIA}\n\n아래 목록에서 중요한 순서대로 {n}건을 고르세요.\n"
           "event는 반드시 [개발·정비 / 거래·시세 / 정책·규제 / 세금·대출] 중 하나로만 고르고, "
           "같은 사건을 다룬 기사에는 같은 event 라벨을 붙이세요.")
    out = client.chat.completions.parse(
        model=MODEL, temperature=0,
        messages=[{"role": "system", "content": sys},
                  {"role": "user", "content": listing}],
        response_format=Shortlist).choices[0].message.parsed
    valid = []
    for p in out.picks:
        if not (0 <= p.index < len(items)):
            continue                                     # 없는 번호는 버린다
        if p.event not in TOPICS:
            p.event = "정책·규제"                        # 카테고리 이탈 방지
        valid.append(p)
    return valid

def _with_pick_meta(items, picks):
    out = []
    for p in picks:
        it = dict(items[p.index])
        it["reason"], it["event"] = p.reason, p.event
        out.append(it)
    return out

def select(s: dict) -> dict:                   # ② 중요도 선별 (예선→본선)
    items = s["collected"]
    if not items:
        return {"picked": [], "log": ["② 선별   수집 0건 → 선별 스킵"]}
    survivors = []
    for i in range(0, len(items), BATCH):       # 예선 — 묶음마다 8건
        chunk = items[i:i + BATCH]
        survivors += _with_pick_meta(chunk, ask_picks(chunk, min(PRE, len(chunk))))
    if len(survivors) <= TARGET:                # 후보가 적으면 본선 생략
        finals = survivors
    else:
        finals = _with_pick_meta(survivors, ask_picks(survivors, TARGET))
    # 같은 event 1건만 (중복 사건 제거 — 먼저 뽑힌 순위 우선)
    seen_ev, deduped = set(), []
    for it in finals:
        if it.get("event") in seen_ev:
            continue
        seen_ev.add(it.get("event"))
        deduped.append(it)
    return {"picked": deduped,
            "log": [f"② 선별   {len(items)} → 예선 {len(survivors)} → {len(deduped)}건"]}

class Draft(BaseModel):
    headline: str = Field(description="20자 내외의 한국어 헤드라인. 지역(서울·경기 구/동)을 앞에 붙인다")
    summary:  str = Field(description="세 문장 요약. ~합니다체, 과장 없이 건조하게. 금액·면적·일정은 숫자 그대로")
    why:      str = Field(description="서울·경기 실수요자 관점의 전문가 코멘트 한 문장 (이번 주 체크 포인트·주의할 점)")

class ReportIn(TypedDict):                     # 워커가 받는 것은 기사 하나뿐
    item: dict

def extract_body(url, rss="", tries=2):
    """fetch 재시도 → requests 폴백 → RSS 전문 순으로 본문 확보.
    via를 함께 반환해 취재 경로를 로그에 남긴다."""
    time.sleep(1)  # 연속 fetch 예절 지연 (봇 차단 회피)
    for _ in range(tries):
        try:
            d = trafilatura.fetch_url(url)
            t = trafilatura.extract(d) if d else None
            if t and len(t) >= 600:
                return t, "fetch"
        except Exception:
            continue
    try:
        r = requests.get(url, headers=UA, timeout=25)
        t = trafilatura.extract(r.text) if r.ok else None
        if t and len(t) >= 600:
            return t, "requests"
    except Exception:
        t = None
    if rss and len(rss) >= 600:  # RSS에 전문이 실린 피드 최후 수단
        return rss, "rss-full"
    return t, "fail"

def draft(body, hint=""):
    user = body[:6000] + (f"\n\n[수정 지시] {hint}" if hint else "")
    return client.chat.completions.parse(
        model=MODEL, temperature=0,
        messages=[{"role": "system", "content": SYS},
                  {"role": "user", "content": user}],
        response_format=Draft).choices[0].message.parsed

def fan_report(s: dict):                      # 기사 수만큼 워커를 펼친다
    return [Send("report", {"item": it}) for it in s["picked"]]

def report(s: ReportIn) -> dict:               # ③ 요약 — 기사 한 건을 맡는다
    it = s["item"]
    body, via = extract_body(it["url"], it.get("rss_full", ""))
    if not body or len(body) < 600:            # G1 기준선
        return {"drafted": [],
                "log": [f"   취재 제외 {it['source']} · 본문 {len(body or '')}자({via})"]}
    d = draft(body)
    return {"drafted": [{**it, "body": body[:6000], "via": via, **d.model_dump()}],
            "log": [f"   취재 완료 {it['source']} · {len(body)}자({via})"]}

class Verdict(BaseModel):
    ok:       bool      = Field(description="요약이 원문에 근거하면 true")
    problems: list[str] = Field(description="근거 없는 부분. 없으면 빈 목록")

SYS_CHECK = ("요약이 원문에서 뒷받침되는지 판정하세요.\n"
             "헤드라인과 요약만 보고 판단하고, 번역이나 단위 환산은 문제가 아닙니다.")

def check(d):
    user = (f"[원문]\n{d['body'][:5000]}\n\n"
            f"[헤드라인]\n{d['headline']}\n\n[요약]\n{d['summary']}")
    return client.chat.completions.parse(
        model=MODEL, temperature=0,
        messages=[{"role": "system", "content": SYS_CHECK},
                  {"role": "user", "content": user}],
        response_format=Verdict).choices[0].message.parsed

def numeric_hallucinations(summary, body):
    """요약 속 숫자 중 원문에 없는 것 — ponytail: regex 1줄 검수, LLM 이전 1차 필터."""
    nums = set(re.findall(r"\d[\d,\.]*", summary or ""))
    body_flat = (body or "").replace(",", "")
    return [n for n in nums if n.replace(",", "") not in body_flat]

def verify(s: dict) -> dict:                  # ④ 검수 (규칙→LLM→재생성→스킵)
    kept, notes = [], []
    for d in s["drafted"]:
        text = f"{d['summary']}\n{d.get('why', '')}"  # 인사이트(why)의 숫자까지 검증
        bad_nums = numeric_hallucinations(text, d["body"])
        v = check(d)
        problems = ([f"숫자 불일치: {bad_nums}"] if bad_nums else []) + v.problems
        if not problems and v.ok:
            kept.append(d)
            continue
        # 1회 재생성 (문제점 전달)
        hint = "다음 지적을 반영해 다시 쓰세요: " + "; ".join(problems)
        try:
            d2 = {**d, **draft(d["body"], hint).model_dump()}
        except Exception as ex:
            notes.append({"title": d.get("title"), "action": "skip",
                          "why": f"재생성 호출 실패: {ex}"})
            continue
        bad2 = numeric_hallucinations(f"{d2['summary']}\n{d2.get('why', '')}", d2["body"])
        v2 = check(d2)
        problems2 = ([f"숫자 불일치: {bad2}"] if bad2 else []) + v2.problems
        if not problems2 and v2.ok:
            kept.append(d2)
            notes.append({"title": d.get("title"), "action": "regenerated", "why": "; ".join(problems)})
        else:
            notes.append({"title": d.get("title"), "action": "skip", "why": "; ".join(problems2 or problems)})
    dropped = len(s["drafted"]) - len(kept)
    return {"verified": kept, "verify_notes": notes,
            "log": [f"④ 검수   {len(s['drafted'])} → {len(kept)}건"
                    + (f" · 재생성 {[n['title'][:20] for n in notes if n['action']=='regenerated']}"
                       if any(n["action"] == "regenerated" for n in notes) else "")
                    + (f" · 스킵 {dropped}건" if dropped else "")]}

COLORS = {"개발·정비": 0x0B6E77, "거래·시세": 0x4C7C9C,
          "정책·규제": 0x8F5606, "세금·대출": 0x2E7D5B}
DEFAULT = 0x5F7476
TITLE_MAX, DESC_MAX, EMBED_MAX, TOTAL_MAX = 256, 4096, 10, 5800   # 6000에서 여유를 둔다

def build_embeds(run_id, lead, articles):
    if not articles:                                   # 조용한 날에도 한 장은 보낸다
        return [{"title": f"🏠 {run_id}", "color": DEFAULT,
                 "description": "오늘은 조용합니다."}]
    embeds = [{"title": f"🏠 {run_id} · 서울·경기 부동산 브리핑", "description": lead, "color": DEFAULT}]
    for i, a in enumerate(articles, 1):
        desc = a["summary"]
        if a.get("why"):
            desc += f"\n\n💡 **{a['why']}**"
        embeds.append({
            "title":       f"{i}. {a['headline']}"[:TITLE_MAX],
            "description": desc[:DESC_MAX],
            "url":         a["url"],
            "color":       COLORS.get(a.get("topic", ""), DEFAULT),
            "footer":      {"text": f"{a['source']} · {a['when']}"},
        })
    total = lambda es: sum(len(e.get("title", "")) + len(e.get("description", ""))
                           + len(e.get("footer", {}).get("text", "")) for e in es)
    cut = 0
    while len(embeds) > EMBED_MAX or total(embeds) > TOTAL_MAX:
        embeds.pop()                                   # 뒤에서부터 덜어낸다
        cut += 1
    if cut:                                            # 잘렸으면 리드에 표시
        embeds[0]["description"] += f"\n(외 {cut}건 분량 초과로 생략)"
    return embeds

def send(run_id, lead, articles, webhook=None, dry_run=True, retries=2):
    payload = {"username": "서울경기 부동산 브리핑", "embeds": build_embeds(run_id, lead, articles)}
    if dry_run or not webhook:
        print(f"[dry-run] embed {len(payload['embeds'])}개 - "
              f"{len(json.dumps(payload, ensure_ascii=False))}자, 전송 생략")
        return "dry-run"
    last = ""
    for attempt in range(retries + 1):
        try:
            r = requests.post(webhook, json=payload, timeout=20)
            if r.status_code in (200, 204):
                print("발행: 성공")
                return "sent"
            last = f"{r.status_code} {r.text[:120]}"
        except Exception as ex:
            last = str(ex)[:120]
        time.sleep(2 ** attempt)                      # 지수 백오프
    print(f"발행: 실패 {last}")
    pathlib.Path("store/failures.jsonl").parent.mkdir(exist_ok=True)
    with open("store/failures.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps({"run_id": run_id, "error": last}, ensure_ascii=False) + "\n")
    return "failed"

def make_lead(arts):
    if not arts:
        return ""
    srcs = ", ".join(dict.fromkeys(a["source"] for a in arts))
    return f"오늘은 {len(arts)}건을 골랐습니다. ({srcs})"

def publish(s: dict) -> dict:                 # ⑤ 발행
    arts = [{"headline": a["headline"], "summary": a["summary"], "why": a["why"],
             "url": a["url"], "source": a.get("outlet") or a["source"], "topic": a.get("event", ""),
             "when": a["at"].astimezone(KST).strftime("%m-%d %H:%M")} for a in s["verified"]]
    today = datetime.now(KST).strftime("%Y-%m-%d")
    status = send(today, make_lead(arts), arts,
                  webhook=os.environ.get("DISCORD_WEBHOOK_URL"),
                  dry_run=os.environ.get("DRY_RUN", "1") == "1")   # 기본은 보내지 않음
    label = f"{len(arts)}건" if arts else "조용합니다"
    return {"log": [f"⑤ 발행   {label} · {status}"]}

def build():
    g = StateGraph(Brief)
    for name in ("collect", "select", "report", "verify", "publish"):
        g.add_node(name, globals()[name])
    g.add_edge(START, "collect")
    g.add_edge("collect", "select")
    g.add_conditional_edges("select", fan_report, ["report"])
    g.add_edge("report", "verify")
    g.add_edge("verify", "publish")
    g.add_edge("publish", END)
    return g

# 24h: 신규 5개 소스 실측 합산 약 95건/24h → 예선 3묶음·본선이 동작하는 최소 창
INIT = {"hours": 24,
        "collected": [], "picked": [], "drafted": [], "verified": [],
        "verify_notes": [], "log": []}

def run():                                     # 돌리고, 한 줄 남긴다
    out = build().compile().invoke(INIT)
    row = {"run_id":    datetime.now(KST).strftime("%Y-%m-%d %H:%M"),
            "collected": len(out["collected"]),
            "picked":    len(out["picked"]),
            "drafted":   len(out["drafted"]),
            "published": len(out["verified"]),
            "hours":     out["hours"],
            "model":     MODEL,
            "by_source": {},
            "select_reasons": [{"title": a["title"][:60], "source": a["source"],
                                "outlet": a.get("outlet", ""),
                                "reason": a.get("reason"), "event": a.get("event")}
                               for a in out["picked"]],
            "verify_notes": out.get("verify_notes", []),
            "delivery":  (out["log"][-1] if out["log"] else ""),
            "log":       out["log"]}
    for a in out["verified"]:
        row["by_source"][a["source"]] = row["by_source"].get(a["source"], 0) + 1
    path = pathlib.Path("store/metrics.jsonl")
    path.parent.mkdir(exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    return out
