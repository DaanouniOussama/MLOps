from zenml import pipeline
from steps.ingest_data import ingest_df
from steps.clean_data import clean_df
from steps.model_train import model_train
from steps.evaluation import evaluate

@pipeline
def train_pipeline(data_path : str):
    df = ingest_df(data_path)
    X_train, X_test, Y_train, Y_test, X_train_scaled, X_test_scaled = clean_df(df)
    model = model_train(X_train_scaled, Y_train)
    accuracy_Score, precision_Score, recall_Score, f1_Score = evaluate(X_test_scaled, Y_test, model) 