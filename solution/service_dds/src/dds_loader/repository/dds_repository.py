import uuid
from datetime import datetime
from typing import Any, Dict, List
from dds_loader.repository.dds_models import (H_Order, 
                        H_User, 
                        H_Product, 
                        H_Restaurant, 
                        H_Category, 
                        L_Order_Product, 
                        L_Order_User, 
                        L_Product_Restaurant, 
                        L_Product_Category,
                        S_User_Names,
                        S_Product_Names,
                        S_Restaurant_Names,
                        S_Order_Cost,
                        S_Order_Status)

from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert_h_order(self, model: H_Order) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order(
                            h_order_pk,
                            order_id,
                            order_dt,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(order_id)s,
                            %(order_dt)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': model.h_order_pk,
                        'order_id': model.order_id,
                        'order_dt': model.order_dt,
                        'load_dt': model.load_dt,
                        'load_src': model.load_src
                    }
                )


    def insert_h_user(self, model: H_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user(
                            h_user_pk,
                            user_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(user_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    {
                        'h_user_pk': model.h_user_pk,
                        'user_id': model.user_id,
                        'load_dt': model.load_dt,
                        'load_src': model.load_src
                    }
                )

    def insert_h_product(self, model: H_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product(
                            h_product_pk,
                            product_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_product_pk)s,
                            %(product_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_product_pk) DO NOTHING;
                    """,
                    {
                        'h_product_pk': model.h_product_pk,
                        'product_id': model.product_id,
                        'load_dt': model.load_dt,
                        'load_src': model.load_src
                    }
                )


    def insert_h_restaurant(self, model: H_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant(
                            h_restaurant_pk,
                            restaurant_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(restaurant_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': model.h_restaurant_pk,
                        'restaurant_id': model.restaurant_id,
                        'load_dt': model.load_dt,
                        'load_src': model.load_src
                    }
                )
    

    def insert_h_category(self, model: H_Category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category(
                            h_category_pk,
                            category_name,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_category_pk)s,
                            %(category_name)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_category_pk) DO NOTHING;
                    """,
                    {
                        'h_category_pk': model.h_category_pk,
                        'category_name': model.category_name,
                        'load_dt': model.load_dt,
                        'load_src': model.load_src
                    }
                )
    

    def insert_l_order_product(self, model: L_Order_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_product(
                            hk_order_product_pk,
                            h_order_pk,
                            h_product_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_product_pk)s,
                            %(h_order_pk)s,
                            %(h_product_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_product_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_product_pk': model.hk_order_product_pk,
                        'h_order_pk': model.h_order_pk,
                        'h_product_pk': model.h_product_pk,
                        'load_dt': model.load_dt,
                        'load_src': model.load_src
                    }
                )

    def insert_l_order_user(self, model: L_Order_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user(
                            hk_order_user_pk,
                            h_order_pk,
                            h_user_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_user_pk)s,
                            %(h_order_pk)s,
                            %(h_user_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_user_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_user_pk': model.hk_order_user_pk,
                        'h_order_pk': model.h_order_pk,
                        'h_user_pk': model.h_user_pk,
                        'load_dt': model.load_dt,
                        'load_src': model.load_src
                    }
                )


    def insert_l_product_restaurant(self, model: L_Product_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant(
                            hk_product_restaurant_pk,
                            h_product_pk,
                            h_restaurant_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_restaurant_pk)s,
                            %(h_product_pk)s,
                            %(h_restaurant_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_restaurant_pk': model.hk_product_restaurant_pk,
                        'h_product_pk': model.h_product_pk,
                        'h_restaurant_pk': model.h_restaurant_pk,
                        'load_dt': model.load_dt,
                        'load_src': model.load_src
                    }
                )

    def insert_l_product_category(self, model: L_Product_Category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_category(
                            hk_product_category_pk,
                            h_product_pk,
                            h_category_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_category_pk)s,
                            %(h_product_pk)s,
                            %(h_category_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_category_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_category_pk': model.hk_product_category_pk,
                        'h_product_pk': model.h_product_pk,
                        'h_category_pk': model.h_category_pk,
                        'load_dt': model.load_dt,
                        'load_src': model.load_src
                    }
                )
    

    def insert_s_user_names(self, model: S_User_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names(
                            h_user_pk,
                            username,
                            userlogin,
                            load_dt,
                            load_src,
                            hk_user_names_hashdiff
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(username)s,
                            %(userlogin)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_user_names_hashdiff)s
                        )
                        ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    {
                        'h_user_pk': model.h_user_pk,
                        'username': model.username,
                        'userlogin': model.userlogin,
                        'load_dt': model.load_dt,
                        'load_src': model.load_src,
                        'hk_user_names_hashdiff': model.hk_user_names_hashdiff
                    }
                )

    def insert_s_product_names(self, model: S_Product_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names(
                            h_product_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_product_names_hashdiff
                        )
                        VALUES(
                            %(h_product_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_product_names_hashdiff)s
                        )
                        ON CONFLICT (h_product_pk) DO NOTHING;
                    """,
                    {
                        'h_product_pk': model.h_product_pk,
                        'name': model.name,
                        'load_dt': model.load_dt,
                        'load_src': model.load_src,
                        'hk_product_names_hashdiff': model.hk_product_names_hashdiff
                    }
                )




    def insert_s_restaurant_names(self, model: S_Restaurant_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names(
                            h_restaurant_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_restaurant_names_hashdiff
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_restaurant_names_hashdiff)s
                        )
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': model.h_restaurant_pk,
                        'name': model.name,
                        'load_dt': model.load_dt,
                        'load_src': model.load_src,
                        'hk_restaurant_names_hashdiff': model.hk_restaurant_names_hashdiff
                    }
                )


    def insert_s_order_cost(self, model: S_Order_Cost) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost(
                            h_order_pk,
                            cost,
                            payment,
                            load_dt,
                            load_src,
                            hk_order_cost_hashdiff
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(cost)s,
                            %(payment)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_cost_hashdiff)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': model.h_order_pk,
                        'cost': model.cost,
                        'payment': model.payment,
                        'load_dt': model.load_dt,
                        'load_src': model.load_src,
                        'hk_order_cost_hashdiff': model.hk_order_cost_hashdiff
                    }
                )


    def insert_s_order_status(self, model: S_Order_Status) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status(
                            h_order_pk,
                            status,
                            load_dt,
                            load_src,
                            hk_order_status_hashdiff
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(status)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_status_hashdiff)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': model.h_order_pk,
                        'status': model.status,
                        'load_dt': model.load_dt,
                        'load_src': model.load_src,
                        'hk_order_status_hashdiff': model.hk_order_status_hashdiff
                    }
                )



