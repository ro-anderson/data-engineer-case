# Data Engineering Pipeline Project

Author name: Rodrigo Anderson
[Linkedin](https://www.linkedin.com/in/ro-anderson/) | [github](https://github.com/ro-anderson)

## Introduction
This project implements a comprehensive data pipeline to process and analyze user interaction data, specifically focusing on user clicks and payments related to various value propositions within an application.

Also, there is a [brief explanation of the decisions made notebook](./notebooks/project_decisions_explanations.ipynb) for more details.

## Project Structure

```bash
.
├── data
│ ├── bronze
│ ├── gold
│ ├── landing
│ │ ├── pays.csv
│ │ ├── prints.json
│ │ └── taps.json
│ └── silver
├── docker-compose.yml
├── Dockerfile
├── etl
│ ├── factory.py
│ ├── init.py
│ └── test.py
├── Makefile
├── notebooks
│ └── etl.ipynb
├── pipeline.py
├── poetry.lock
├── pyproject.toml
├── README.md
└── requirements.txt
```

- **data/**: Contains data segregated into bronze, silver, and gold layers, mimicking a data lake.
- **docker-compose.yml & Dockerfile**: Setup for Docker-based deployment.
- **etl/**: Core ETL scripts including the factory design pattern implementation.
- **Makefile**: Commands for setup and operation.
- **notebooks/etl.ipynb**: Jupyter notebook for interactive ETL processes.
- **pipeline.py**: The main script to run the pipelines.
- **pyproject.toml & poetry.lock**: Poetry package management files.

## Setup and Run Instructions
### Prerequisites
Ensure data files (`taps.json`, `pays.csv`, `prints.json`) are placed in `./data/landing` before starting.

### Running the Pipeline
#### Via Docker Compose

```bash
docker-compose up notebook
```

Access the notebook at: ```http://127.0.0.1:8888/tree```

snapshot of the jupyter hub running locally via docker compose:
![Captura de tela de 2024-04-29 21-48-48](https://github.com/ro-anderson/data-engineer-case/assets/41929105/2c1ea3cf-09a7-4d0d-8e2d-3683e5ffffc6)

**obs:**
After up the server, inside the notebook running locally, you may want to run all cells and reset the kernel to see all the pipeline running on the notebook (that should works well):
![Captura de tela de 2024-04-29 21-51-51](https://github.com/ro-anderson/data-engineer-case/assets/41929105/34cb597c-f468-46a8-a6e0-b099ccd68221)


#### Via Poetry (without Docker)
Ensure Poetry is installed and them:

```bash
poetry install
poetry run python pipeline.py
```

Follow the prompts to select between bronze, silver, and gold pipelines.

#### Via Python Environment

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python pipeline.py
```
snapshoot of running via python virtualenv:
![Captura de tela de 2024-04-29 21-45-14](https://github.com/ro-anderson/data-engineer-case/assets/41929105/aec104a8-d7d1-45f9-947b-e9c29fa6eae2)


## Expected Results
The data pipeline is structured to provide detailed insights into user interactions with value propositions over specific time frames, particularly focusing on the activities leading up to the most recent prints. The expected outcomes are structured as follows:

- *Prints from the Last Week*:
- *For each print*:
    - A field indicating if the value props were clicked or not.
    - The number of views each value proposition received in the last 3 weeks prior to the print.
    - The number of times a user clicked on each of the value props in the last 3 weeks prior to the print.
    - The number of payments made by the user for each value proposition in the last 3 weeks prior to the print.
    - The total amount of payments made by the user for each value proposition in the last 3 weeks prior to the print.


## Architecture and Design Patterns
This project adopts a [**medallion architecture**](https://www.databricks.com/glossary/medallion-architecture), processing data through bronze, silver, and gold layers, which emulates a data lake architecture locally. This setup allows for scalable and modular data processing.

### Code Patterns
- [**Factory Pattern**](https://refactoring.guru/design-patterns/factory-method): Used to create instances of data processors dynamically.
- [**Strategy Pattern**](https://refactoring.guru/design-patterns/strategy): Encapsulates algorithms (data processing methods) allowing the runtime algorithm to be selected.

This approach ensures a robust, scalable, and maintainable codebase, suitable for processing complex and large datasets in a modular and efficient manner.
