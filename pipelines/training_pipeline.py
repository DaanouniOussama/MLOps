from zenml import pipeline
from steps.ingest_data import ingest_df
from steps.clean_data import clean_df
from steps.model_train import model_train
from steps.evaluation import evaluation 

@pipeline
def train_pipeline(data_path : str):
    df = ingest_df(data_path)
    clean_df(df)
    model_train(df)
    evaluation(df) 