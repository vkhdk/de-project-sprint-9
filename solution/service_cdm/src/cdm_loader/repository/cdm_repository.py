import uuid
from datetime import datetime
from typing import Any, Dict, List
from uuid import UUID
from cdm_loader.repository.cdm_models import (REPORT_user_category_counters,
                        REPORT_user_product_counters)



from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert_user_category_counters(self, model: REPORT_user_category_counters) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters(
                            user_id,
                            category_id,
                            category_name,
                            order_count
                        )
                        VALUES(
                            %(user_id)s,
                            %(category_id)s,
                            %(category_name)s,
                            1
                        )
                        ON CONFLICT (user_id, category_id) DO UPDATE SET
                            order_count = user_category_counters.order_count + 1;
                    """,
                    {
                        'user_id': model.user_id,
                        'category_id': model.category_id,
                        'category_name': model.category_name
                    }
                )

    def insert_user_product_counters(self, model: REPORT_user_product_counters) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters(
                            user_id,
                            product_id,
                            product_name,
                            order_count
                        )
                        VALUES(
                            %(user_id)s,
                            %(product_id)s,
                            %(product_name)s,
                            1
                        )
                        ON CONFLICT (user_id, product_id) DO UPDATE SET
                            order_count = user_product_counters.order_count + 1;
                    """,
                    {
                        'user_id': model.user_id,
                        'product_id': model.product_id,
                        'product_name': model.product_name
                    }
                )