"""대여소 마스터 동기화를 지금 1회 실행한다 (수동 확인용).

실행: python scripts/sync_stations_once.py
"""
import logging

from dotenv import load_dotenv

from ddareungi.stations import sync_stations

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

print(sync_stations())
