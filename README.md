# Soda SQL Airflow Operator

Airflow Operator for running Soda SQL scans



# Example Usage

``` yaml
# src/soda/scans/my_scan.yml
# Note that the Airflow rendered templates are accessible (e.g. {{ params.client_id }})
table_name: tmp_{{ params.client_id }}_{{ ds_nodash }}
sql_metrics:
  - sql: |
      SELECT
        SUM(value1) AS staged_value1,
        SUM(value2) AS staged_value2
      FROM tmp_{{ params.client_id }}_{{ ds_nodash }}
  - sql: |
      SELECT
        SUM(value1) AS final_value1,
        SUM(value2) AS final_value2
      FROM final_table
      WHERE
        date = '{{ ds }}'
        AND client_id = {{ params.client_id }}
tests:
  - staged_value1 == final_value1
  - staged_value2 > final_value2
```


``` python
# my_airflow_dag.py
from pathlib import Path
from soda_util import build_soda_warehouse, convert_templated_yml_to_dict

SODA_PATH = Path(os.getenv("PYTHON_PATH", "/code/src")) / "/soda/scans/"  # Matches where my_scan.yml is saved

validate_staged_data = SodaSqlOperator(
    task_id="validate_staged_data",
    warehouse=build_soda_warehouse("warehouse_name", "database_name"),  # Could also pass a file path to a yml file
    scan=convert_templated_yml_to_dict(SODA_PATH, "my_scan.yml"),
    params={"client_id": 12345},  # Params are rendered by Airflow and accessible in the yaml file
)
```

# Notes

- Unlike Soda itself, a builder pattern is not used to define the warehouse and scan argument. Rather, the warehouse and scan parameters are instance checked and the relevant Soda methods are set. This provides a much simpler API, where we can just pass in the args to the Operator
- As we are passing over all rendering of Jinga templates to Airflow, the native Soda templates are not accessible. So always use Airflow templates
- Soft failures (i.e. the Airflow task doesn't fail, it just alerts) have been implemented, but alerting of soft failures has not. So soft failures will essentially just mean the Airflow task passes. Alerting to be implemented