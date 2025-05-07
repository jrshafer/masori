"""
Main entrypoint for the application
"""

import typer

from masori.pipeline.players import PlayerPipelineRunner
from masori.pipeline.teams import TeamPipelineRunner
from masori.pipeline.positions import PositionsPipelineRunner

app = typer.Typer()

@app.command()
def teams():
    tp = TeamPipelineRunner()
    tp.run()

@app.command()
def players():
    pl = PlayerPipelineRunner()
    pl.run()

@app.command()
def positions():
    pos = PositionsPipelineRunner()
    pos.run()

if __name__ == '__main__':
    app()