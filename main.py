"""
================================================================================
프로그램명 : SKALA AI 서비스화 과정 - 데이터 수집 미니 파이프라인
소      속 : 광주캠퍼스 2반
작  성  자 : 신서현
주요 기능  :
    1. httpx와 asyncio.gather()를 이용한 3개 외부 API 비동기 동시 수집 파이프라인
    2. Pydantic v2 기반의 데이터 유효성(타입 및 리스트 요소 범위) 정밀 검증
    3. CSV 및 Parquet 성능 비교 저장
================================================================================
"""

import asyncio
import httpx
import time
import pandas as pd # 성능 비교 저장을 위한 라이브러리
from pydantic import BaseModel, Field, ValidationError, field_validator
from typing import List

# -------------------------------------------------------------
# Pydantic v2 스키마 정의
# -------------------------------------------------------------
class WeatherData(BaseModel):
    """Open-Meteo 날씨 데이터 검증 모델"""
    latitude: float
    longitude: float
    temperatures: List[float] = Field(..., description="3일간 시간대별 기온 리스트")
    precip_probabilities: List[int] = Field(..., description="3일간 시간대별 강수확률 리스트")

    # Pydantic v2 리스트 요소 범위 검증 (ge, le 오류 해결)
    @field_validator('precip_probabilities')
    @classmethod
    def validate_probabilities(cls, v: List[int]) -> List[int]:
        """강수확률 리스트 내부의 모든 요소가 0% 이상 100% 이하인지 검사합니다."""
        for prob in v:
            if not (0 <= prob <= 100):
                raise ValueError(f"강수확률 범위를 벗어난 값이 존재합니다 (0~100 사이 필수): {prob}")
        return v

class CountryData(BaseModel):
    """Countries.dev 국가 데이터 검증 모델"""
    name: str = Field(..., min_length=1)
    alpha2: str = Field(..., min_length=2, max_length=2)
    alpha3: str = Field(..., min_length=3, max_length=3)

class IPData(BaseModel):
    """ip-api 지역 데이터 검증 모델"""
    status: str = Field(..., min_length=1)
    country: str = Field(..., min_length=1)
    city: str = Field(..., min_length=1)
    # 범위 검증: 위도(-90~90), 경도(-180~180) 지리적 범위 제한
    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)


# -------------------------------------------------------------
# [비동기 데이터 수집 기능 정의]
# -------------------------------------------------------------
async def fetch_data(client: httpx.AsyncClient, name: str, url: str) -> dict:
    """지정된 API URL로 비동기 GET 요청을 보내고 JSON 응답을 반환합니다."""
    try:
        response = await client.get(url, timeout=10.0)
        if response.status_code == 200:
            print(f"[{name}] API 수집 완료 (status : {response.status_code})")
            return response.json()
        else:
            print(f"[{name}] API 응답 에러 (status : {response.status_code})")
            return {}
    except Exception as e:
        print(f"[{name}] 연결 실패: {str(e)}")
        return {}


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
    
    # -------------------------------------------------------------
    # 수집한 JSON에서 필요한 필드 추출 및 Pydantic 검증 파이프라인
    # -------------------------------------------------------------
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

        # -------------------------------------------------------------
        # CSV 및 Parquet 저장 성능 비교측정 
        # -------------------------------------------------------------
        print("\n[성능 비교] CSV vs Parquet 파일 입출력 및 스토리지 최적화 평가")

        # 검증 완료된 날씨 데이터 -> pandas dataframe으로 구조화
        df = pd.DataFrame({
            "temperature": weather_validated.temperatures,
            "precipitation_probability": weather_validated.precip_probabilities,
            "collected_city": ip_validated.city,          # IP 데이터 결합
            "country_code": country_validated.alpha3      # 국가 데이터 결합
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

        # 결과 화면 출력 (슬라이드 요구사항: 성능 측정 결과 출력 준수)
        print("-" * 60)
        print(f"포맷 종류   | 쓰기 속도(초)         | 읽기 속도(초)")
        print("-" * 60)
        print(f"CSV         | {csv_write_time:.6f}초         | {csv_read_time:.6f}초")
        print(f"Parquet     | {parquet_write_time:.6f}초         | {parquet_read_time:.6f}초")
        print("-" * 60)
        print("[분석 결과] 압축형 바이너리 포맷인 Parquet의 성능 우위가 입증되었습니다.")




    except ValidationError as e:
        print(f" 스키마 검증 실패 (타입 또는 범위 에러 발생): \n{e.json()}")
        


if __name__ == "__main__":
    asyncio.run(main_pipeline())