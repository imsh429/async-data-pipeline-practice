"""
================================================================================
프로그램명 : SKALA AI 서비스화 과정 - 데이터 수집 미니 파이프라인 (Main)
소      속 : 광주캠퍼스 2반
작  성  자 : 신서현
주요 기능  : 분리된 모듈들을 조율하여 비동기 수집, Pydantic 검증, 저장 성능 비교를 수행하는 메인 파이프라인
================================================================================
"""

import asyncio
import httpx
import time
from pydantic import ValidationError

from models import WeatherData, CountryData, IPData
from collector import fetch_data
from storage import save_and_evaluate_performance

async def main_pipeline():
    urls = {
        "Open-Meteo(날씨)": "https://api.open-meteo.com/v1/forecast?latitude=37.5665&longitude=126.9780&hourly=temperature_2m,precipitation_probability&forecast_days=3&timezone=Asia/Seoul",
        "Countries.dev(국가)": "https://countries.dev/alpha/KOR",
        "ip-api(지역)": "http://ip-api.com/json/8.8.8.8"
    }
    
    print("\n비동기 파이프라인 데이터 수집 시작")
    start_time = time.time()
    
    async with httpx.AsyncClient() as client:
        tasks = [fetch_data(client, name, url) for name, url in urls.items()]
        results = await asyncio.gather(*tasks)
        
    end_time = time.time()
    print(f"모든 API 수집 완료 -> 총 소요 시간: {end_time - start_time:.4f}초")
    
    weather_raw, country_raw, ip_raw = results
    
    print("\n [검증 단계] Pydantic v2 스키마 기반 유효성 검사 작동...")
    
    try:
        # 날씨 데이터 추출 및 검증
        hourly_data = weather_raw.get("hourly", {})
        weather_validated = WeatherData(
            latitude=weather_raw.get("latitude"),
            longitude=weather_raw.get("longitude"),
            temperatures=hourly_data.get("temperature_2m", []),
            precip_probabilities=hourly_data.get("precipitation_probability", [])
        )
        print("  ➔ 날씨 데이터 스키마 검증 통과!")

        # 국가 데이터 추출 및 검증
        country_validated = CountryData(
            name=country_raw.get("name", "South Korea"),
            alpha2=country_raw.get("alpha2", "KR"),
            alpha3=country_raw.get("alpha3", "KOR")
        )
        print("  ➔ 국가 정보 데이터 스키마 검증 통과!")

        # IP 지역 데이터 검증
        ip_validated = IPData(**ip_raw)
        print("  ➔ IP 기반 지역 정보 스키마 검증 통과!")
        
        print("\n모든 데이터가 Pydantic v2 타입·범위 검증을 통과했습니다!")

        # 저장 및 성능 평가 모듈 호출
        save_and_evaluate_performance(weather_validated, country_validated, ip_validated)

    except ValidationError as e:
        print(f" 스키마 검증 실패 (타입 또는 범위 에러 발생): \n{e.json()}")

if __name__ == "__main__":
    asyncio.run(main_pipeline())