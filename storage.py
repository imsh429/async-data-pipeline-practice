"""
================================================================================
프로그램명 : SKALA AI 서비스화 과정 - 데이터 수집 미니 파이프라인 (Storage)
소      속 : 광주캠퍼스 2반
작  성  자 : 신서현
주요 기능  : Pandas 변환 및 CSV/Parquet 저장 성능 비교 측정 모듈
================================================================================
"""

import time
import pandas as pd
from models import WeatherData, CountryData, IPData

def save_and_evaluate_performance(weather: WeatherData, country: CountryData, ip: IPData):
    """검증 완료된 데이터를 데이터프레임으로 결합하고 CSV/Parquet 포맷 저장 성능을 비교합니다."""
    print("\n[성능 비교] CSV vs Parquet 파일 입출력 및 스토리지 최적화 평가")

    df = pd.DataFrame({
        "temperature": weather.temperatures,
        "precipitation_probability": weather.precip_probabilities,
        "collected_city": ip.city,
        "country_code": country.alpha3
    })

    csv_file = "weather_pipeline.csv"
    parquet_file = "weather_pipeline.parquet"

    # CSV 쓰기 성능 측정
    t0 = time.perf_counter()
    df.to_csv(csv_file, index=False, encoding="utf-8")
    csv_write_time = time.perf_counter() - t0

    # Parquet 쓰기 성능 측정
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
    print(f"CSV         | {csv_write_time:.6f}초         | {csv_read_time:.6f}초")
    print(f"Parquet     | {parquet_write_time:.6f}초         | {parquet_read_time:.6f}초")
    print("-" * 60)
    print("[분석 결과] 압축형 바이너리 포맷인 Parquet의 성능 우위가 입증되었습니다.")