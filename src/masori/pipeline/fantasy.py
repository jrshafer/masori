"""
Handles the ingestion pipelines for fantasy related datasets
"""

import datetime

from masori.ingest.common import Common
from masori.ingest.fantasy import Fantasy
from masori.pipeline.pipeline import GenericPipeline

class FantasyPipelineRunner:
    def __init__(self):
        self.fantasy = Fantasy()
        self.common = Common()

    def run(self):
        qbs = GenericPipeline(
            pipeline_name='fantasy data [qb]',
            year = datetime.datetime.now().year,
            database_name='nfl',
            schema='fantasy',
            table_name='qb_proj',
            partition_keys=['s_full_name', 'id_year', 'id_week'],
            id_fetcher=lambda year: ['qb'],
            extract_fn=self.fantasy.get_data_from_fantasypros,
            data_slicer=lambda raw: raw.get('results', []),
            transform_fn=self.fantasy.transform_qb_data_from_fantasypros
        )
        rbs = GenericPipeline(
            pipeline_name='fantasy data [rb]',
            year = datetime.datetime.now().year,
            database_name='nfl',
            schema='fantasy',
            table_name='rb_proj',
            partition_keys=['s_full_name', 'id_year', 'id_week'],
            id_fetcher=lambda year: ['rb'],
            extract_fn=self.fantasy.get_data_from_fantasypros,
            data_slicer=lambda raw: raw.get('results', []),
            transform_fn=self.fantasy.transform_rb_data_from_fantasypros
        )
        wrs = GenericPipeline(
            pipeline_name='fantasy data [wr]',
            year = datetime.datetime.now().year,
            database_name='nfl',
            schema='fantasy',
            table_name='wr_proj',
            partition_keys=['s_full_name', 'id_year', 'id_week'],
            id_fetcher=lambda year: ['wr'],
            extract_fn=self.fantasy.get_data_from_fantasypros,
            data_slicer=lambda raw: raw.get('results', []),
            transform_fn=self.fantasy.transform_wr_data_from_fantasypros
        )
        tes = GenericPipeline(
            pipeline_name='fantasy data [te]',
            year = datetime.datetime.now().year,
            database_name='nfl',
            schema='fantasy',
            table_name='te_proj',
            partition_keys=['s_full_name', 'id_year', 'id_week'],
            id_fetcher=lambda year: ['te'],
            extract_fn=self.fantasy.get_data_from_fantasypros,
            data_slicer=lambda raw: raw.get('results', []),
            transform_fn=self.fantasy.transform_te_data_from_fantasypros
        )
        defs = GenericPipeline(
            pipeline_name='fantasy data [dst]',
            year = datetime.datetime.now().year,
            database_name='nfl',
            schema='fantasy',
            table_name='dst_proj',
            partition_keys=['s_full_name', 'id_year', 'id_week'],
            id_fetcher=lambda year: ['dst'],
            extract_fn=self.fantasy.get_data_from_fantasypros,
            data_slicer=lambda raw: raw.get('results', []),
            transform_fn=self.fantasy.transform_dst_data_from_fantasypros
        )
        ks = GenericPipeline(
            pipeline_name='fantasy data [ks]',
            year = datetime.datetime.now().year,
            database_name='nfl',
            schema='fantasy',
            table_name='k_proj',
            partition_keys=['s_full_name', 'id_year', 'id_week'],
            id_fetcher=lambda year: ['k'],
            extract_fn=self.fantasy.get_data_from_fantasypros,
            data_slicer=lambda raw: raw.get('results', []),
            transform_fn=self.fantasy.transform_k_data_from_fantasypros
        )

        pipelines = [
            qbs, 
            rbs,
            wrs,
            tes,
            defs,
            ks
        ]

        for pipeline in pipelines:
            pipeline.run()

