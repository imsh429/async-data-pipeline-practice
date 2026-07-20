"""
================================================================================
프로그램명 : SKALA AI 서비스화 과정 - 데이터 수집 미니 파이프라인
소      속 : 광주캠퍼스 2반
작  성  자 : 신서현
주요 기능  : pytest를 이용한 Pydantic 데이터 검증 모델 단위 테스트(Unit Test) 수행
================================================================================
"""

import pytest
from pydantic import ValidationError
from main import WeatherData  # main.py에서 정의한 스키마 로드

def test_weather_data_validation():
    """
    Pydantic 스키마가 정상 범위와 비정상 범위를 올바르게 판별하는지 1건 이상 테스트합니다.
    """
    # 케이스 1: 정상 데이터 입력 시 오류 없이 객체가 생성되어야 함 (Pass 조건)
    valid_data = WeatherData(
        latitude=37.5665,
        longitude=126.9780,
        temperatures=[21.5, 22.0],
        precip_probabilities=[0, 45, 100]  # 0~100 사이의 정상 범위
    )
    assert len(valid_data.precip_probabilities) == 3
    
    # 케이스 2: 범위를 초과한 데이터(105%) 입력 시 ValidationError를 던져야 함 (Fail 예외처리 조건)
    with pytest.raises(ValidationError):
        WeatherData(
            latitude=37.5665,
            longitude=126.9780,
            temperatures=[21.5],
            precip_probabilities=[120]  # 100을 초과하여 에러가 터져야 정상
        )