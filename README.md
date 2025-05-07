# Masori | NFL Data ETL Pipeline Repository

Data ingestion logic relies highly on this [unofficial espn api documentation](https://gist.github.com/nntrn/ee26cb2a0716de0947a0a4e9a157bc1c).

Infrastructure is managed / deployed via the [aranea](https://github.com/jrshafer/aranea) repository.  

Database credentials (host, port, user, password, etc.) are stored in a `.env` file in the project root. Create the file with the following fields

```
DB_USER=<etl-db-user>
DB_PASSWORD=<etl-db-user-pw>
DB_NAME=<db>
DB_HOST=<hostname>
DB_PORT=<port>
POSTGRES_USER=<superuser>
POSTGRES_PASSWORD=<superuser-pw>
DEFAULT_PASSWORD=changeme
```

To run the ETL pipelines:
1. Ensure you have python 3.13.x installed. If not, download from the [python website](https://www.python.org/)  
2. Download PDM `pip install pdm`  
3. Clone the repo: `git clone git@github.com:jrshafer/masori.git`  
4. Change directory into the project folder: `cd masori`  
5. Install packages: `pdm install`  
6. Run pipelines: `pdm run masori <pipeline>`  


### Masori pipelines currently supported:  
 | Dataset | Masori Command |  
 | --------| -------------- |  
 | [NFL Teams](https://www.espn.com/nfl/teams) | `pdm run masori teams` |  
 | [NFL Players](https://www.espn.com/nfl/players) | `pdm run masori players` |  
 | [NFL Positions](https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions?limit=75) | `pdm run masori positions` |  

