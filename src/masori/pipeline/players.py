"""
Handles the ingestion pipelines for players related datasets
"""

import datetime

from masori.ingest.common import Common
from masori.ingest.players import Players
from masori.pipeline.pipeline import GenericPipeline

class PlayerPipelineRunner:
    def __init__(self):
        self.players = Players()
        self.common = Common()

    def run(self):
        pipeline = GenericPipeline(
            pipeline_name='reference data [players]',
            year = datetime.datetime.now().year,
            database_name='nfl',
            schema='reference',
            table_name='players',
            partition_keys=['id'],
            id_fetcher=self.common.get_nfl_team_ids,
            extract_fn=self.players.get_espn_roster_by_team,
            data_slicer=lambda raw: [
                item for group in raw.get('athletes', []) for item in group.get('items', [])
            ],
            transform_fn=self.players.transform_espn_roster
        )

        pipeline.run()
