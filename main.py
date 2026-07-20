"""
================================================================================
프로그램명 : SKALA AI 서비스화 과정 - 데이터 수집 미니 파이프라인
소      속 : 광주캠퍼스 2반
작  성  자 : 신서현
주요 기능  :
    1. httpx와 asyncio.gather()를 이용한 3개 외부 API 비동기 동시 수집 파이프라인
    2. Pydantic v2 기반의 데이터 유효성(타입 및 리스트 요소 범위) 정밀 검증
    3. CSV 및 Parquet 성능 비교 저장 및 복합적 예외 처리 (네트워크, 검증, I/O)
    4. 프로덕션 급 이중 로깅 시스템 가동 (콘솔 출력 + pipeline.log 파일 기록)
수정 기록  :
    1. 2026. 07. 20 초기 코드 작성
    2. 모듈화 수행
================================================================================
"""

import asyncio
import time
import logging
import pandas as pd
import httpx

from pydantic import ValidationError
from models import WeatherData, CountryData, IPData
from collector import fetch_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("pipeline.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def main_pipeline():
    urls = {
        "Open-Meteo(날씨)": "https://api.open-meteo.com/v1/forecast?latitude=37.5665&longitude=126.9780&hourly=temperature_2m,precipitation_probability&forecast_days=3&timezone=Asia/Seoul",
        "Countries.dev(국가)": "https://countries.dev/alpha/KOR",
        "ip-api(지역)": "http://ip-api.com/json/8.8.8.8"
    }
    
    logger.info("비동기 파이프라인 데이터 수집 가동")
    start_time = time.time()
    
    async with httpx.AsyncClient() as client:
        tasks = [fetch_data(client, name, url) for name, url in urls.items()]
        results = await asyncio.gather(*tasks)
        
    end_time = time.time()
    print(f"모든 API 수집 완료 -> 총 소요 시간: {end_time - start_time:.4f}초")
    
    weather_raw, country_raw, ip_raw = results
    
    
    if not weather_raw or not country_raw or not ip_raw:
            logger.error("일부 API 수집 실패로 인해 파이프라인을 안전하게 조기 종료합니다.")
            return
        
    logger.info("[검증 단계] Pydantic v2 스키마 기반 유효성 검사 시작")   
     
    try:
        # 날씨 데이터 추출 및 검증
        hourly_data = weather_raw.get("hourly", {})
        weather_validated = WeatherData(
            latitude=weather_raw.get("latitude"),
            longitude=weather_raw.get("longitude"),
            temperatures=hourly_data.get("temperature_2m", []),
            precip_probabilities=hourly_data.get("precipitation_probability", [])
        )
        logger.info("  ➔ 날씨 데이터 스키마 검증 통과!")

        # 국가 데이터 추출 및 검증
        country_validated = CountryData(
            name=country_raw.get("name", ""),
            alpha2=country_raw.get("alpha2Code", ""),
            alpha3=country_raw.get("alpha3Code", "")
        )
        logger.info("  ➔ 국가 정보 데이터 스키마 검증 통과!")

        # IP 지역 데이터 검증
        ip_validated = IPData(**ip_raw)
        logger.info("  ➔ IP 기반 지역 정보 스키마 검증 통과!")
        
        logger.info("모든 데이터가 Pydantic v2 타입·범위 검증을 완료했습니다.")

        # -------------------------------------------------------------
        # CSV 및 Parquet 저장 성능 비교측정 
        # -------------------------------------------------------------
        logger.info("[성능 비교] CSV vs Parquet 파일 입출력 및 스토리지 최적화 평가")

        # 검증 완료된 날씨 데이터 -> pandas dataframe으로 구조화
        df = pd.DataFrame({
            "temperature": weather_validated.temperatures,
            "precipitation_probability": weather_validated.precip_probabilities,
            "collected_city": ip_validated.city,
            "country_code": country_validated.alpha3
        })

        csv_file = "weather_pipeline.csv"
        parquet_file = "weather_pipeline.parquet"

        # csv 쓰기 성능 측정
        t0 = time.perf_counter()
        df.to_csv(csv_file, index=False, encoding="utf-8")
        csv_write_time = time.perf_counter() - t0
        
        # Parquet 쓰기 성능 측정 (압축 엔진 pyarrow 가동)
        t0 = time.perf_counter()
        df.to_parquet(parquet_file, index=False, engine="pyarrow")
        parquet_write_time = time.perf_counter() - t0
        
        # CSV 읽기 성능 측정
        t0 = time.perf_counter()
        _ = pd.read_csv(csv_file)
        csv_read_time = time.perf_counter() - t0
        
        # Parquet 읽기 성능 측정
        t0 = time.perf_counter()
        _ = pd.read_parquet(parquet_file, engine="pyarrow")
        parquet_read_time = time.perf_counter() - t0

        # 결과 화면 출력
        print("-" * 60)
        print("포맷 종류   | 쓰기 속도(초)         | 읽기 속도(초)")
        print("-" * 60)
        print(f"CSV         | {csv_write_time:.6f}초            | {csv_read_time:.6f}초")
        print(f"Parquet     | {parquet_write_time:.6f}초            | {parquet_read_time:.6f}초")
        print("-" * 60)
        
        write_winner = "CSV" if csv_write_time < parquet_write_time else "Parquet"
        read_winner = "CSV" if csv_read_time < parquet_read_time else "Parquet"
        
        logger.info(f"[분석 결과] 이번 측정에서는 쓰기 {write_winner}, 읽기 {read_winner}가 더 빨랐습니다.")
        logger.info("(데이터 규모가 작아 Parquet의 압축·메타데이터 오버헤드가 상대적으로 크게 작용했을 수 있습니다.)")

    except ValidationError as e:
        logger.error(f"스키마 검증 실패 (타입 또는 범위 에러 발생): \n{e.json()}")
    except Exception as e:
        logger.error(f"파이프라인 실행 중 예기치 못한 시스템/적재 에러 발생: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main_pipeline())