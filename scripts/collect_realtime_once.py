"""실시간 스냅샷을 지금 시점으로 1회 수집해 landing에 올린다 (수동 확인용).

실행: python scripts/collect_realtime_once.py
"""
import logging

from dotenv import load_dotenv

from ddareungi.realtime import collect_realtime

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

print(collect_realtime())
