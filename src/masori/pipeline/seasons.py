"""
Handles the ingestion pipelines for season related datasets
"""

import datetime

from masori.ingest.common import Common
from masori.ingest.seasons import Seasons
from masori.pipeline.pipeline import GenericPipeline
from masori.db.database import Database

class SeasonsPipelineRunner:
    def __init__(self):
        self.seasons = Seasons()
        self.common = Common()
        self.database = Database()

    def run(self):
        season_types = GenericPipeline(
            pipeline_name='reference data [season types]',
            year = datetime.datetime.now().year,
            database_name='nfl',
            schema='reference',
            table_name='season_types',
            partition_keys=['id'],
            id_fetcher=self.common.get_nfl_season_years,
            extract_fn=self.seasons.get_espn_season_types,
            data_slicer=lambda raw: raw.get('types', {}).get('items', []),
            transform_fn=self.seasons.transform_espn_season_types
        )

        pipelines = [
            season_types, 
        ]

        for pipeline in pipelines:
            pipeline.run()
