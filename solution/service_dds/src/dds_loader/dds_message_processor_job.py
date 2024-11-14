from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository
from dds_loader.repository.dds_builder import DdsOrdersBuilder

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = batch_size
    
    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
                order = msg['payload']

            # Only 'CLOSED' msg should be processed
            if order['status'] != 'CLOSED':
                continue

            builder = DdsOrdersBuilder(order)
                
            # Create Hubs
            h_order = builder.h_order()
            self._dds_repository.insert_h_order(h_order)

            h_user = builder.h_user()
            self._dds_repository.insert_h_user(h_user)

            h_products = builder.h_product()
            self._dds_repository.insert_h_product(h_products)

            h_restaurant = builder.h_restaurant()
            self._dds_repository.insert_h_restaurant(h_restaurant)

            h_categories = builder.h_category()
            self._dds_repository.insert_h_category(h_categories)
                
            # Create Links
            l_order_product_links = builder.l_order_product(h_order.h_order_pk, h_products)
            self._dds_repository.insert_l_order_product(l_order_product_links)     

            l_order_user_link = builder.l_order_user(h_order.h_order_pk, h_user)
            self._dds_repository.insert_l_order_user(l_order_user_link)

            l_product_restaurant_links = builder.l_product_restaurant(h_products, h_restaurant.h_restaurant_pk)
            self._dds_repository.insert_l_product_restaurant(l_product_restaurant_links)

            l_product_category_links = builder.l_product_category(h_products, h_categories)
            self._dds_repository.insert_l_product_category(l_product_category_links)

            # Create Satellites
            s_user_names = builder.s_user_names(h_user)
            self._dds_repository.insert_s_user_names(s_user_names)

            s_product_names = builder.s_product_names(h_products)
            self._dds_repository.insert_s_product_names(s_product_names)

            s_restaurant_names = builder.s_restaurant_names(h_restaurant)
            self._dds_repository.insert_s_restaurant_names(s_restaurant_names)

            s_order_cost = builder.s_order_cost(h_order)
            self._dds_repository.insert_s_order_cost(s_order_cost)

            s_order_status = builder.s_order_status(h_order)
            self._dds_repository.insert_s_order_status(s_order_status)                

            # Send message to topic
            dst_msg = {
                    "user_id": h_user.h_user_pk,
                    "product_id": [p.h_product_pk for p in h_products],
                    "product_name": [p.name for p in h_products],
                    "category_id": [c.h_category_pk for c in h_categories],
                    "category_name": [c.category_name for c in h_categories],
                    "order_cnt": [p.quantity for p in h_products]
                }

            self._producer.produce(dst_msg)

        self._logger.info(f"{datetime.utcnow()}: FINISH")