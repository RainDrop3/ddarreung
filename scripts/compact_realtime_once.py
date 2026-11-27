"""실시간 스냅샷 compaction을 지금 1회 실행한다 (수동 확인용).

실행: python scripts/compact_realtime_once.py
"""
import logging

from dotenv import load_dotenv

from ddareungi.compaction import compact_realtime

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

for result in compact_realtime():
    print(result)
