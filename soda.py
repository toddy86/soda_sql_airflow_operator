from typing import Dict, Optional, Union

from airflow.models.baseoperator import BaseOperator
from sodasql.scan.scan import Scan, ScanResult
from sodasql.scan.scan_builder import ScanBuilder
from sodasql.scan.scan_yml import ScanYml
from sodasql.scan.warehouse_yml import WarehouseYml
from sodasql.soda_server_client.soda_server_client import SodaServerClient


class SodaDataValidation(Exception):
    pass


class SodaSqlOperator(BaseOperator):
    """
    Runs a Soda SQL data validation scan

    Args:
        warehouse: Definition of the data source as either the yml file path (str), dictionary or WarehouseYml instance
        scan: Scan file which defines the table, metric and tests. Either yml file path (str), dictionary or ScanYml
        soda_server_client: Authenticated Soda Server Client
        time: Soda Cloud scan time
        fail_task: Boolean as whether to fail the airflow task or not if any of the scan tests fail (soft vs hard fails)
        do_xcom_push: Boolean as to whether to push the scan measurements and test results to xcoms

    Raises:
        Exception: If fail_task is True and there are errors found in the scan tests

    Usage Notes:
        - CANNOT use Soda variables argument due to conflict in order of rendering between Soda and Airflow
        - Use native Airflow templates or the params argument to template any values in the scan
            - "filter": "client_id = {{ params.client_id }}"
            - "filter": "date = {{ ds }}",

    Returns:
        None
    """

    template_fields = (
        "scan",
        "time",
    )

    def __init__(  # pylint disable=too-many-arguments
        self,
        warehouse: Union[dict, str, WarehouseYml],
        scan: Union[dict, str, ScanYml],
        *args,
        soda_server_client: Optional[SodaServerClient] = None,
        time: Optional[str] = None,
        fail_task: bool = True,
        do_xcom_push: bool = False,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.warehouse = warehouse
        self.scan = scan
        self.soda_server_client = soda_server_client
        self.time = time
        self.fail_task = fail_task
        self.do_xcom_push = do_xcom_push

    def _set_scan_and_warehouse_attributes(self, scan_builder):
        """
        Temporary helper to dynamically sets the correct warehouse and scan type attributes on the ScanBuilder
        Soda team are considering changing the ScanBuilder API to essentially do the below
        """
        warehouse_mapper = {
            str: "warehouse_yml_file",
            dict: "warehouse_yml_dict",
            WarehouseYml: "warehouse_yml",
        }
        scan_mapper = {str: "scan_yml_file", dict: "scan_yml_dict", ScanYml: "scan_yml"}

        setattr(scan_builder, warehouse_mapper[type(self.warehouse)], self.warehouse)
        setattr(scan_builder, scan_mapper[type(self.scan)], self.scan)
        return scan_builder

    def _scan_builder(self, context: Dict) -> Scan:
        scan_builder = ScanBuilder()
        scan_builder = self._set_scan_and_warehouse_attributes(scan_builder)
        scan_builder.variables = context  # Use Airflow context as variables to avoid template rendering conflicts
        scan_builder.soda_server_client = self.soda_server_client
        scan_builder.time = self.time
        scan = scan_builder.build()

        return scan

    def execute(self, context: Dict) -> None:
        scan = self._scan_builder(context)

        # Patch the SQL filter which is already rendered by Airflow. Only applicable for Dict scan objects
        # Soda team looking to fix in source code in coming weeks
        if isinstance(self.scan, dict):
            scan.filter_sql = self.scan.get("filter")

        scan_result = scan.execute()

        if self.do_xcom_push:
            self.xcom_push(context, "soda_scan", scan_result.to_json())

        self._check_for_failures(scan_result)

    def _check_for_failures(self, scan_result: ScanResult) -> None:
        if not scan_result.has_test_failures():
            self.log.info("Soda Scan did not find any errors. Have a lovely day!")
            return

        self.log.error(
            "Soda Scan found %s errors in your data",
            scan_result.get_test_failures_count(),
        )
        for failure in scan_result.get_test_failures():
            self.log.error(failure)

        # @TODO: Implement Slack alerting if the fail_task == False (i.e. warning / soft failures)
        if self.fail_task:
            raise SodaDataValidation
