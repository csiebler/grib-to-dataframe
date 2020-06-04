# Commands to run this repo

## Attaching to workspace

Attach the folder with the model code to the AML workspace (run this command from the repo's root folder):
```
az ml folder attach -g aml-demo -w aml-demo
```

This connects the repo to the workspace in AML.

## Convert files

First, put some GRIB files into `../sample-data/`.

Convert GRIB files locally with Python (without using AML):
```
python convert.py --data-path ../sample-data/
```

Convert on local Docker container, but log metrics to AML:
```
az ml run submit-script -c convert-local -e convert-grib-local
```
In this case `-c` refers to the `--run-configuration-name` (which points to `aml_config/<run-configuration-name>.runconfig`) and `-e` refers to the `--experiment-name`.

Convert using AML on an AML Compute Cluster:
```
az ml run submit-script -c convert-amlcompute -e convert-grib-amlcompute
```
In this case, update `aml_config/convert-amlcompute.runconfig` and point it to the dataset you want to convert.

## Convert files in parallel

Have a look at `pipelines-python\convert-parallel\pipeline.py`, which assembles a ML Pipeline using Parallel Run Step for processing multiple GRIB files in parallel.
