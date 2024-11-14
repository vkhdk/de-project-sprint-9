from typing import Any, Dict, List
from datetime import datetime
import uuid
from cdm_loader.repository.cdm_models import (REPORT_user_category_counters,
                        REPORT_user_product_counters)


class CdmOrderBuilder:
    def __init__(self, dict: Dict) -> None:
        self._dict = dict
        self.order_ns_uuid = uuid.NAMESPACE_DNS

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))
    
    def user_category_counters(self) -> List[REPORT_user_category_counters]:
        user_id = self._dict['user_id']
        res = []
        for prod_dict in self._dict['products']:
            cat_name = prod_dict['category']
            res.append(
                REPORT_user_category_counters(
                    user_id=self._uuid(user_id),
                    category_id=self._uuid(cat_name),
                    category_name=cat_name
                )
            )
        return res
    
    def user_product_counters(self) -> List[REPORT_user_product_counters]:
        user_id = self._dict['user_id']
        res = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            prod_name = prod_dict['name']
            res.append(
                REPORT_user_product_counters(
                    user_id=self._uuid(user_id),
                    product_id=self._uuid(prod_id),
                    product_name=prod_name
                )
            )
        return res