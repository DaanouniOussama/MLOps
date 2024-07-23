from pipelines.training_pipeline import train_pipeline

# database="Real_estate", user="airflow", password="airflow", host="172.18.0.2", port=5432

if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--algo", choices=["RandomForest", "XGBoost"], required=True,
                        help="Machine learning algorithm to use (RandomForest or XGBoost)")
    args = parser.parse_args()
    train_pipeline(args.algo)

