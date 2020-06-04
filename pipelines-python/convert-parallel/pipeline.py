import os
import azureml.core
from azureml.core import Workspace, Experiment, Datastore, Dataset, RunConfiguration, Model
from azureml.core.compute import AmlCompute
from azureml.pipeline.core import Pipeline, PipelineData

from azureml.data.dataset_consumption_config import DatasetConsumptionConfig
from azureml.pipeline.steps import ParallelRunConfig
from azureml.pipeline.steps import ParallelRunStep

print("SDK version:", azureml.core.VERSION)

dataset_name = 'grib-dataset'

ws = Workspace.from_config()
print(ws.name, ws.resource_group, ws.location, ws.subscription_id, sep = '\n')

datastore = ws.get_default_datastore()

input_ds = Dataset.get_by_name(ws, dataset_name)
batch_data = DatasetConsumptionConfig("batch_dataset", input_ds, mode='mount')

output_dir = PipelineData(name='batch_output', datastore=datastore)

parallel_run_config = ParallelRunConfig.load_yaml(workspace=ws, path='convert_parallel.yml')

batch_step = ParallelRunStep(
    name="batch-conversion-step",
    parallel_run_config=parallel_run_config,
    arguments=['--data_output_path', output_dir],
    inputs=[batch_data],
    output=output_dir,
    allow_reuse=False
)

steps = [batch_step]

pipeline = Pipeline(workspace=ws, steps=steps)
pipeline.validate()

pipeline_run = Experiment(ws, 'convert-batch-pipeline').submit(pipeline)
pipeline_run.wait_for_completion()

#published_pipeline = pipeline.publish(pipeline_name)
#print("Published pipeline id: ", published_pipeline.id)
