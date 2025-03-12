"""
news_extraction_schema.py
用于定义新闻稿信息抽取的数据结构和验证逻辑
"""

from typing import List, Optional
from pydantic import BaseModel, Field


class Person(BaseModel):
    """人物信息模型"""
    name: str = Field(..., description="人物姓名")
    title: Optional[str] = Field(None, description="人物职位或头衔，如不存在则为null")


class Location(BaseModel):
    """地点信息模型"""
    name: str = Field(..., description="地点名称")
    detail: Optional[str] = Field(None, description="地点详细信息，如不存在则为null")


class TargetEntity(BaseModel):
    """目标客体信息模型（可以是人、组织或其他实体）"""
    name: str = Field(..., description="目标客体名称")
    type: str = Field(..., description="目标客体类型，如'个人'、'公司'、'组织'、'国家'等")
    description: Optional[str] = Field(None, description="目标客体的附加描述，如不存在则为null")


class Event(BaseModel):
    """事件信息模型"""
    description: str = Field(..., description="事件描述")
    time: Optional[str] = Field(None, description="事件发生时间，如不存在则为null")


class NewsExtraction(BaseModel):
    """新闻稿结构化提取模型"""
    leader: Person = Field(..., description="主导人物/领导信息")
    location: Location = Field(..., description="事件发生地点")
    event: Event = Field(..., description="事件信息")
    targets: List[TargetEntity] = Field(default_factory=list, description="目标客体列表，可以有多个")
    accompanying_persons: List[Person] = Field(default_factory=list, description="陪同人物列表，可以有多个")

    class Config:
        """模型配置"""
        json_schema_extra = {
            "example": {
                "leader": {
                    "name": "张伟",
                    "title": "省长"
                },
                "location": {
                    "name": "北京",
                    "detail": "人民大会堂"
                },
                "event": {
                    "description": "签署经济合作协议",
                    "time": "2025年3月10日"
                },
                "targets": [
                    {
                        "name": "美国代表团",
                        "type": "组织",
                        "description": "由美国商务部长带队"
                    },
                    {
                        "name": "日本贸易协会",
                        "type": "组织",
                        "description": "null"
                    }
                ],
                "accompanying_persons": [
                    {
                        "name": "李红",
                        "title": "外交部副部长"
                    },
                    {
                        "name": "王明",
                        "title": "商务厅厅长"
                    }
                ]
            }
        }


# 导出所有相关类，便于导入
__all__ = ['Person', 'Location', 'TargetEntity', 'Event', 'NewsExtraction']