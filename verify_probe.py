from graph import numeric_hallucinations, check

body = "Mecka raised 60 million dollars led by Framework Ventures at about 500 million valuation."
good = "Framework Ventures가 주도한 6천만 달러 투자 이후 기업 가치는 약 5억 달러에 달합니다."
bad = "Framework Ventures가 주도한 8천만 달러 투자 이후 기업 가치는 약 9억 달러에 달합니다."

print("정상요약 숫자검수:", numeric_hallucinations(good, body))
print("변조요약 숫자검수:", numeric_hallucinations(bad, body))

v = check({"body": body, "headline": "Mecka 투자 유치", "summary": bad})
print("LLM 변조판정 ok =", v.ok, "| problems =", v.problems)
v2 = check({"body": body, "headline": "Mecka 투자 유치", "summary": good})
print("LLM 정상판정 ok =", v2.ok, "| problems =", v2.problems)
