"""
Main entrypoint for the application
"""

import typer

from masori.pipeline.teams import TeamPipelines
from masori.pipeline.players import PlayerPipelines

app = typer.Typer()

@app.command()
def teams():
    tp = TeamPipelines()
    tp.exec_team_reference_pipeline()

@app.command()
def players():
    pl = PlayerPipelines()
    pl.exec_player_reference_pipeline()

if __name__ == '__main__':
    app()