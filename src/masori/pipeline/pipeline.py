"""
Generic classes for pipeline functionality
"""

from typing import List, Dict, Callable, Any
from loguru import logger
from datetime import datetime

from masori.db.database import Database


class GenericPipeline:
    def __init__(
        self,
        pipeline_name: str,
        database_name: str,
        year: datetime.date,
        schema: str,
        table_name: str,
        partition_keys: List[str],
        id_fetcher: Callable[[], List[Any]],
        extract_fn: Callable[[Any], Any],
        data_slicer: Callable[[Any], List[Any]],
        transform_fn: Callable[[Any], Dict]
    ):
        self.logger = logger
        self.pipeline_name = pipeline_name
        self.year = year
        self.database_name = database_name
        self.schema = schema
        self.table_name = table_name
        self.partition_keys = partition_keys

        self.id_fetcher = id_fetcher
        self.extract_fn = extract_fn
        self.data_slicer = data_slicer
        self.transform_fn = transform_fn

        self.database = Database()

    def run(self):
        fq_table_name = f"{self.database_name}.{self.schema}.{self.table_name}"
        self.logger.info(f'Starting pipeline for {fq_table_name}')

        ids = self.id_fetcher(self.year)
        dataset = []

        for id in ids:
            self.logger.info(f'Fetching data for ID {id}')
            raw_data = self.extract_fn(id)

            self.logger.info(f'Slicing raw data for ID {id}')
            raw_items = self.data_slicer(raw_data)

            self.logger.info(f"Transforming {len(raw_items)} records for ID {id}")
            for item in raw_items:
                dataset.append(self.transform_fn(item))
            
        self.logger.info(f"Inserting {len(dataset)} records into {fq_table_name}")
        self.database.upsert_table(
            database=self.database_name,
            schema=self.schema,
            table_name=self.table_name,
            rows=dataset,
            partition_keys=self.partition_keys
        )

        self.logger.info(f'Pipeline for {self.pipeline_name} complete.')
