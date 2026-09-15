import os, sys
from graph import run                 # graph.py 에 있는 그 run()

if __name__ == "__main__":
    if "--dry-run" in sys.argv:
        os.environ["DRY_RUN"] = "1"
    if "--send" in sys.argv:
        os.environ["DRY_RUN"] = "0"   # 실제 디스코드 발행
    if "--hours" in sys.argv:
        from graph import INIT
        INIT["hours"] = int(sys.argv[sys.argv.index("--hours") + 1])
    out = run()
    for line in out["log"]:
        print(line)                    # 이 출력이 Actions 로그에 그대로 남는다
