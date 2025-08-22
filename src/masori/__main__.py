"""
Main entrypoint for the application
"""

import typer

from masori.pipeline.players import PlayerPipelineRunner
from masori.pipeline.teams import TeamPipelineRunner
from masori.pipeline.positions import PositionsPipelineRunner
from masori.pipeline.seasons import SeasonsPipelineRunner
from masori.pipeline.fantasy import FantasyPipelineRunner
from masori.pipeline.draftkings import DraftkingsPipelineRunner

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

@app.command()
def seasons():
    sea = SeasonsPipelineRunner()
    sea.run()

@app.command()
def fantasy():
    fan = FantasyPipelineRunner()
    fan.run()

@app.command()
def draftkings():
    dk = DraftkingsPipelineRunner()
    dk.run()

if __name__ == '__main__':
    app()