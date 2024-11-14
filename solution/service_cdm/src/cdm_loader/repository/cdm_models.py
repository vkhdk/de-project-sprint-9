from pydantic import BaseModel
import uuid
from datetime import datetime

class REPORT_user_category_counters(BaseModel):
    user_id: uuid.UUID
    category_id: uuid.UUID
    category_name: str

class REPORT_user_product_counters(BaseModel):
    user_id: uuid.UUID
    product_id: uuid.UUID
    product_name: str