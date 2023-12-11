import yaml
from sqlalchemy import create_engine
import pandas as pd 

def load_credentials(file_path= 'credentials.yaml'):
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    return data

class RDSDatabaseConnector:
    def __init__(self, credentials) -> None:
        self.host = credentials.get('RDS_HOST')
        self.port = credentials.get('RDS_PORT')
        self.user = credentials.get('RDS_USER')
        self.password = credentials.get('RDS_PASSWORD')
        self.database = credentials.get('RDS_DATABASE')
        self.engine = None

    def initaliseSQLAlchemyEngine(self):
        self.engine = create_engine(f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}")
    
    def connect(self):
        self.connection = self.engine.connect()
    
    def extract_data(self):
        loan_df = pd.read_sql_table('loan_payments', self.engine)
        return loan_df
    
    def save_data_as_CSV(self, dataframe, file_path):
        dataframe.to_csv(file_path, index=False)
    
    def close_connection(self):
        self.connection.close()
    

credentials_data = load_credentials()
rds_connnector = RDSDatabaseConnector(credentials_data)
rds_connnector.initaliseSQLAlchemyEngine()
rds_connnector.connect()
loan_table_df = rds_connnector.extract_data()
print(loan_table_df.head())
rds_connnector.save_data_as_CSV(loan_table_df, r'C:\Users\Tom\EDS_Project_Loan_Finance\loan_payments.csv')
rds_connnector.close_connection()




