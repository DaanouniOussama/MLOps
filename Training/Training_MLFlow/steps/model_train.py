import logging
import pandas as pd
from sklearn.base import RegressorMixin
from src.train_model import Training, XGBoostRegressionModel, RandomForestModel
import mlflow

# Configure logging
logging.basicConfig(level=logging.INFO,  # Set the logging level
                    format='%(asctime)s - %(levelname)s - %(message)s')


def model_train(X_train : pd.DataFrame , Y_train : pd.Series, algo : str) -> RegressorMixin:

    try :
        if(algo == 'RandomForest'):
            mlflow.sklearn.autolog()
            model_1 = Training(X_train, Y_train, RandomForestModel())
            model_1_trained = model_1.training()
            mlflow.sklearn.log_model(model_1_trained,'RF_model')
            logging.info('RF Training finished')
            return model_1_trained
        
        elif(algo == 'XGBoost'):
            mlflow.xgboost.autolog()
            model_1 = Training(X_train, Y_train, XGBoostRegressionModel())
            model_1_trained = model_1.training()
            mlflow.sklearn.log_model(model_1_trained,'XGBoost_model')
            logging.info('XGBoost Training finished')
            return model_1_trained
        else:
            logging.info('Please choose between RandomForest or XGBoost')

    
    except Exception as e:
        logging.error(f'Error while training the model {e}')
        raise e




