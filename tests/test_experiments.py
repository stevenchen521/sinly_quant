# python
from pydantic import BaseModel, Field, field_validator
from typing import Any

class RectangleModel(BaseModel):
    width: float = Field(..., gt=0)
    height: float = Field(..., gt=0)

    @property
    def area(self) -> float:
        return self.width * self.height

    @field_validator("width", "height")
    def coerce_number(cls, v: Any):
        # pydantic will coerce strings like "2" to numbers (if possible)
        return float(v)

# parsing/validation + serialization for free
r = RectangleModel(width="2", height=3)
print(r.area)        # 6.0
print(r.model_dump())      # {'width': 2.0, 'height': 3.0}

def test_rectangle_area():

    rect = RectangleModel(width=2, height=3)
    print(rect.area)  # 6




