"""
Handles the ingestion pipelines for draftkings related datasets
"""

import datetime

from masori.ingest.common import Common
from masori.ingest.draftkings import Draftkings
from masori.pipeline.pipeline import GenericPipeline

class DraftkingsPipelineRunner:
    def __init__(self):
        self.draftkings = Draftkings()
        self.common = Common()

    def run(self):
        dk = GenericPipeline(
            pipeline_name='draftkings data [dk]',
            year = datetime.datetime.now().year,
            database_name='nfl',
            schema='draftkings',
            table_name='dk_salary',
            partition_keys=['s_full_name', 'id_year', 'id_week'],
            id_fetcher=self.draftkings.get_draftkings_group_id,
            extract_fn=self.draftkings.get_data_from_draftkings,
            data_slicer=lambda raw: raw,
            transform_fn=self.draftkings.transform_data_from_draftkings
        )

        pipelines = [
                dk
            ]

        for pipeline in pipelines:
            pipeline.run()

