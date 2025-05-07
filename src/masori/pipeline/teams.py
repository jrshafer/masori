"""
Handles the ingestion pipelines for teams related datasets
"""

import datetime

from masori.ingest.common import Common
from masori.ingest.teams import Teams
from masori.pipeline.pipeline import GenericPipeline

class TeamPipelineRunner:
    def __init__(self):
        self.teams = Teams()
        self.common = Common()

    def run(self):
        pipeline = GenericPipeline(
            pipeline_name='reference data [teams]',
            year = datetime.datetime.now().year,
            database_name='nfl',
            schema='reference',
            table_name='teams',
            partition_keys=['id'],
            id_fetcher=self.common.get_nfl_team_ids,
            extract_fn=self.teams.get_espn_teams,
            data_slicer=lambda raw: [raw.get('team')],
            transform_fn=self.teams.transform_espn_teams
        )

        pipeline.run()
