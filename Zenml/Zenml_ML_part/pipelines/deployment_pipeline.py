import numpy as np
import pandas as pd
from zenml import step, pipeline
from zenml.config import DockerSettings
from zenml.constants import DEFAULT_SERVICE_START_STOP_TIMEOUT
from zenml.integrations.constants import MLFLOW
from zenml.integrations.mlflow.model_deployers.mlflow_model_deployer import MLFlowModelDeployer
from zenml.integrations.mlflow.services import MLFlowDeploymentService
from zenml.integrations.mlflow.steps import mlflow_model_deployer_step
from zenml.steps import BaseParameters, Output
from steps.ingest_data import ingest_df
from steps.clean_data import clean_df
from steps.model_train import model_train
from steps.evaluation import evaluate
import logging
import json
from pipelines.utils import get_data_for_test

docker_settings = DockerSettings(required_integrations=[MLFLOW]) 

#Define class for deployment pipeline configuration
class DeploymentTriggerConfig(BaseParameters):
    min_accuracy:float = 0.95

@step (enable_cache=False)
def deployment_trigger(
    accuracy: float,
    config: DeploymentTriggerConfig,
):
    """
    It trigger the deployment only if accuracy is greater than min accuracy.
    Args:
        accuracy: accuracy of the model.
        config: Minimum accuracy thereshold.
    """
    try:
        return accuracy >= config.min_accuracy
    except Exception as e:
        logging.error("Error in deployment trigger",e)
        raise e

# Define a continuous pipeline
@pipeline(enable_cache=False,settings={"docker":docker_settings})
def continuous_deployment_pipeline(
    data_path:str,
    workers: int = 1,
    timeout: int = DEFAULT_SERVICE_START_STOP_TIMEOUT
):
  
    df = ingest_df(data_path)
    X_train, X_test, Y_train, Y_test, X_train_scaled, X_test_scaled = clean_df(df)
    model = model_train(X_train_scaled, Y_train)
    accuracy_Score, precision_Score, recall_Score, f1_Score = evaluate(X_test_scaled, Y_test, model) 
    deployment_decision = deployment_trigger(accuracy=accuracy_Score) # returning True or False depending on if accuracy >= config.min_accuracy
    mlflow_model_deployer_step(
        model=model,
        deploy_decision = deployment_decision,
        workers = workers,
        timeout = timeout
    )


########################################### Inference pipeline ##############################################

# class will serve for changing parameters of steps
class MLFlowDeploymentLoaderStepParameters(BaseParameters):
    pipeline_name: str
    step_name: str
    running: bool = True


# Fetching data to be served for the deployed model
@step(enable_cache=False)
def dynamic_importer() -> np.ndarray:
    data = get_data_for_test()
    return data    


# Fetching the MLFlow server with params
@step(enable_cache=False)
def prediction_service_loader(
    pipeline_name: str,
    pipeline_step_name: str,
    running: bool = True,
    model_name: str = "model",
) -> MLFlowDeploymentService:
    # get the MLflow model deployer stack component
    model_deployer = MLFlowModelDeployer.get_active_model_deployer()
    existing_services = model_deployer.find_model_server(
        pipeline_name=pipeline_name,
        pipeline_step_name=pipeline_step_name,
        model_name=model_name,
        running=running,
    )
    if not existing_services:
        raise RuntimeError(
            f"No MLflow prediction service deployed by the "
            f"{pipeline_step_name} step in the {pipeline_name} "
            f"pipeline for the '{model_name}' model is currently "
            f"running."
        )
    return existing_services[0]

# Starting server and running predictions
@step(enable_cache=False)
def predictor(service: MLFlowDeploymentService, data: np.ndarray) -> np.ndarray:
    service.start(timeout=10)
    prediction = service.predict(data)
    return prediction

@pipeline(enable_cache=False, settings={"docker": docker_settings})
def inference_pipeline(pipeline_name: str, pipeline_step_name: str):
    batch_data = dynamic_importer()
    model_deployment_service = prediction_service_loader(
        pipeline_name=pipeline_name,
        pipeline_step_name=pipeline_step_name,
        running=False,
    )
    prediction = predictor(service=model_deployment_service, data=batch_data)
    return prediction
