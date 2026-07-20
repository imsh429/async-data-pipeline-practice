"""
================================================================================
프로그램명 : SKALA AI 서비스화 과정 - 데이터 수집 미니 파이프라인
소      속 : 광주캠퍼스 2반
작  성  자 : 신서현
작  성  일 : 2026.07.20
주요 기능  :
    1. httpx와 asyncio.gather()를 이용한 3개 외부 API 비동기 동시 수집 파이프라인
    2. Pydantic v2 기반의 데이터 유효성 검증
    3. CSV 및 Parquet 성능 비교 저장 
================================================================================
"""

import asyncio
import httpx
import time

# -------------------------------------------------------------
# [비동기 데이터 수집 기능 정의]
# -------------------------------------------------------------
async def fetch_data(client: httpx.AsyncClient, name: str, url: str) -> dict:
    """
    API URL로 비동기 GET 요청을 보내고 JSON 응답을 반환합니다.
    """
    try:
        response = await client.get(url, timeout =10)
        # 응답 코드 200 => 정상
        if response.status_code == 200:
            print(f"{name} API 수집 완료 (status : {response.status_code})")
            return response.json()
        else:
            print(f"{name} API 응답 에러 (status : {response.status_code})")
    except Exception as e:
        print(f"{name} 연결 실패 : {str(e)}")
        return {}

async def main_pipeline():
    urls = {
        "Open-Meteo(날씨)": "https://api.open-meteo.com/v1/forecast?latitude=37.5665&longitude=126.9780&hourly=temperature_2m,precipitation_probability&forecast_days=3&timezone=Asia/Seoul",
        "Countries.dev(국가)": "https://countries.dev/alpha/KOR",
        "ip-api (지역)": "http://ip-api.com/json/8.8.8.8"
    }

    print("비동기 파이프라인 데이터 수집 시작")
    start_time = time.time()

    # 세션 낭비 방지 -> 단일 AsyncClient 환경에서 gather 수행
    async with httpx.AsyncClient() as client:
        tasks = [fetch_data(client, name, url) for name, url in urls.items()]
        results = await asyncio.gather(*tasks)
    
    end_time = time.time()
    print(f"모든 API 수집 완료 -> 총 소요 시간: {end_time - start_time:.4f}초")

    #수집된 데이터 각각 분리
    weather_raw, country_raw, ip_raw = results

    # 차후 단계를 위한 임시 디버깅 출력 (삭제하기 나중에!!!!!!)
    print(f"-> 날씨 데이터 데이터 포함 여부: {'hourly' in weather_raw}")
    print(f"-> IP 데이터 수집 도시: {ip_raw.get('city', 'Unknown')}")

if __name__ == "__main__":
    # 비동기 메인 루프 실행
    asyncio.run(main_pipeline())

