from datetime import datetime
from logging import Logger
from typing import Dict, List
from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository
from cdm_loader.repository.cdm_builder import CdmOrderBuilder


class CdmMessageProcessor:
    def __init__(self,
                consumer: KafkaConsumer,
                cdm_repository: CdmRepository,
                batch_size: int,
                logger: Logger) -> None:
        self._logger = logger
        self._consumer = consumer
        self._repository = cdm_repository
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break
            order = msg['payload']
            builder = CdmOrderBuilder(order)
            self._load_reports(builder)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
    
    def _load_reports(self, builder: CdmOrderBuilder) -> None:
        for row in builder.user_category_counters():
            self._repository.insert_user_category_counters(row)
        for row in builder.user_product_counters():
            self._repository.insert_user_product_counters(row)