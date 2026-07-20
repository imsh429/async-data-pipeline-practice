"""
================================================================================
프로그램명 : SKALA AI 서비스화 과정 - 데이터 수집 미니 파이프라인 (Models)
소      속 : 광주캠퍼스 2반
작  성  자 : 신서현
주요 기능  : Pydantic v2 기반의 데이터 유효성(타입 및 리스트 요소 범위) 정밀 검증 스키마
================================================================================
"""

from pydantic import BaseModel, Field, field_validator
from typing import List

class WeatherData(BaseModel):
    """Open-Meteo 날씨 데이터 검증 모델"""
    latitude: float
    longitude: float
    temperatures: List[float] = Field(..., description="3일간 시간대별 기온 리스트")
    precip_probabilities: List[int] = Field(..., description="3일간 시간대별 강수확률 리스트")

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
    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)