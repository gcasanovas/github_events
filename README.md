# Intro

This program aggregates information about users and repositories from GitHub events using PySpark. For more information about the project and the data, visit https://www.gharchive.org/.

# Setup

Navigate to project's directory and create a venv:
`python -m venv myenv`

Activate the venv:
`.\myenv\Scripts\Activate.ps1`

Install requirements:
`pip install -r requirements.txt`

Run main.py passing the desired parameters:
`python python/main.py 2023-01-01 2023-02-01`

# Parameters explanation

The main idea is to extract data from the gharchive API between the desired dates. The first parameter consists of the start date of the range and the second parameter of the end date. For the program to work properly, both must have the following format: %Y-%m-%d.

Enjoy!
