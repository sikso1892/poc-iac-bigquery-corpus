import os
from glob import glob
from google.cloud import bigquery
from google.oauth2 import service_account

PATH_CREDENIAL_FILE = "/workspaces/poc-iac-bigquery-corpus/config/poc-bigquery-408603-0c8d738a3f7c.json"
PATH_SRC_DIR = "/workspaces/poc-iac-bigquery-corpus/data"

def upload_csv_to_bigquery(client, dataset_name, table_name, csv_file_path, schema):

    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1  # CSV 파일에 헤더가 있는 경우
    job_config.schema = schema        # 스키마 정의

    files = glob(os.path.join(PATH_SRC_DIR, "*.csv"))
    for csv_file_path in files:    
        with open(csv_file_path, "rb") as file:
            load_job = client.load_table_from_file(file, table_ref, job_config=job_config)

    load_job.result()  # 대기, 작업 완료까지

    print(f"CSV 파일 '{csv_file_path}'이(가) '{dataset_name}.{table_name}' 테이블에 업로드되었습니다.")

if __name__ == "__main__":    
    # 사용 예시
    credentials = service_account.Credentials.from_service_account_file(PATH_CREDENIAL_FILE)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    upload_csv_to_bigquery(
        client,
        "your_dataset",
        "your_table",
        "path/to/your/file.csv",
        [
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("age", "INTEGER"),
            # 스키마 필드 계속...
        ]
    )
