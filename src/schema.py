"""
biographical_models.py
用于定义人物履历数据结构和验证逻辑
"""

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field, model_validator, field_validator


class EventType(str, Enum):
    """事件类型枚举"""
    STUDY = "study"  # 学习经历
    WORK = "work"  # 工作经历


class BaseEvent(BaseModel):
    """基础事件模型，包含学习和工作经历的共同字段和验证逻辑"""

    eventType: EventType = Field(..., description="事件类型，'study'表示学习经历，'work'表示工作经历")
    startYear: Optional[int] = Field(None, description="开始年份，如果不存在则为null，范围必须在1900-2100之间")
    startMonth: Optional[int] = Field(None, description="开始月份，如果不存在则为null，范围必须在1-12之间")
    isEnd: bool = Field(..., description="该段经历是否已结束，true表示已结束，false表示未结束")
    hasEndDate: bool = Field(...,
                             description="是否有结束日期。true表示有结束年份（即使具体月份或日期可能未知），false表示没有结束日期信息")
    endYear: Optional[int] = Field(None, description="结束年份，未结束或不存在则为null，范围必须在1900-2100之间")
    endMonth: Optional[int] = Field(None, description="结束月份，未结束或不存在则为null，范围必须在1-12之间")


    # 学习经历特有字段
    school: Optional[str] = Field(None, description="学校名称，仅对学习经历有效")
    department: Optional[str] = Field(None, description="毕业院系，仅对学习经历有效")
    major: Optional[str] = Field(None, description="专业名称，仅对学习经历有效")
    degree: Optional[str] = Field(None, description="学位，如'学士'、'硕士'、'博士'等，仅对学习经历有效")

    # 工作经历特有字段
    place: Optional[str] = Field(None, description="工作单位名称，仅对工作经历有效")
    position: Optional[str] = Field(None, description="职位名称，仅对工作经历有效")

    # 验证年份范围
    @field_validator('startYear', 'endYear')
    @classmethod
    def validate_year(cls, v, info):
        """验证年份是否在有效范围内"""
        if v is not None and (v < 1900 or v > 2100):
            raise ValueError(f"{info.field_name}必须在1900-2100之间")
        return v

    # 验证月份范围
    @field_validator('startMonth', 'endMonth')
    @classmethod
    def validate_month(cls, v, info):
        """验证月份是否在有效范围内"""
        if v is not None and (v < 1 or v > 12):
            raise ValueError(f"{info.field_name}必须在1-12之间")
        return v

    # 验证结束日期规则
    @model_validator(mode='after')
    def validate_end_date(self) -> 'BaseEvent':
        """当isEnd和hasEndDate都为true时，endYear是必需的"""
        if self.isEnd and self.hasEndDate and self.endYear is None:
            raise ValueError("当isEnd和hasEndDate都为true时，endYear是必需的")
        return self

    # 验证学习经历特有规则
    @model_validator(mode='after')
    def validate_study_event(self) -> 'BaseEvent':
        """确保学习经历符合特定规则"""
        if self.eventType == EventType.STUDY:
            if self.school is None:
                raise ValueError("学习经历必须包含school字段")
            if self.place is not None:
                raise ValueError("学习经历的place字段必须为null")
            if self.position is not None:
                raise ValueError("学习经历的position字段必须为null")
        return self

    # 验证工作经历特有规则
    @model_validator(mode='after')
    def validate_work_event(self) -> 'BaseEvent':
        """确保工作经历符合特定规则"""
        if self.eventType == EventType.WORK:
            if self.place is None:
                raise ValueError("工作经历必须包含place字段")
            if self.position is None:
                raise ValueError("工作经历必须包含position字段")
            if self.school is not None:
                raise ValueError("工作经历的school字段必须为null")
            if self.major is not None:
                raise ValueError("工作经历的major字段必须为null")
            if self.degree is not None:
                raise ValueError("工作经历的degree字段必须为null")
            if self.department is not None:
                raise ValueError("工作经历的department字段必须为null")
        return self


class BiographicalEvents(BaseModel):
    """人物履历模型，包含多个事件"""
    events: List[BaseEvent] = Field(..., description="人物履历中的学习和工作事件列表")

    class Config:
        """模型配置"""
        json_schema_extra = {
            "example": {
                "events": [
                    {
                        "eventType": "study",
                        "startYear": 2000,
                        "startMonth": 9,
                        "isEnd": True,
                        "hasEndDate": True,
                        "endYear": 2004,
                        "endMonth": 7,
                        "school": "北京大学",
                        "department": "信息科学技术学院",
                        "major": "计算机科学",
                        "degree": "学士",
                        "place": None,
                        "position": None
                    },
                    {
                        "eventType": "work",
                        "startYear": 2004,
                        "startMonth": 8,
                        "isEnd": False,
                        "hasEndDate": False,
                        "endYear": None,
                        "endMonth": None,
                        "school": None,
                        "major": None,
                        "degree": None,
                        "department": None,
                        "place": "ABC公司",
                        "position": "软件工程师"
                    }
                ]
            }
        }


# 导出所有相关类，便于导入
__all__ = ['EventType', 'BaseEvent', 'BiographicalEvents']