"""
Main entrypoint for the application
"""

import typer

from masori.pipeline.teams import TeamPipelines

app = typer.Typer()

@app.command()
def teams():
    tp = TeamPipelines()
    tp.exec_team_reference_pipeline()

@app.command()
def players():
    print('No support for players... yet')

if __name__ == '__main__':
    app()