"""
================================================================================
프로그램명 : SKALA AI 서비스화 과정 - 데이터 수집 미니 파이프라인 (Collector)
소      속 : 광주캠퍼스 2반
작  성  자 : 신서현
주요 기능  : httpx와 asyncio를 이용한 외부 API 비동기 수집 모듈
================================================================================
"""

import httpx

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