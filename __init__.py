import time
from threading import Lock

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils import apply_defaults


class AwsAthenaHook(AwsHook):

    def __init__(self, aws_conn_id='aws_default'):
        super(AwsAthenaHook, self).__init__(aws_conn_id=aws_conn_id)

        self._lock = Lock()
        self._kill = False
        self._client = None
        self._cur_query_id = None

    def run(self,
            sql,
            region_name=None,
            request_token=None,
            schema=None,
            result_config=None):
        if isinstance(sql, basestring):
            sql = [sql]

        params = {'ResultConfiguration': result_config or {}}

        if request_token:
            params['ClientRequestToken'] = request_token

        if schema:
            params['QueryExecutionContext'] = {'Database': schema}

        self._client = self.get_client_type('athena', region_name=region_name)

        for idx, s in enumerate(sql, 1):
            with self._lock:
                if self._kill:
                    self.log.info('get killed')
                    return

                resp = self._client.start_query_execution(QueryString=s,
                                                          **params)
                query_id = resp['QueryExecutionId']
                self._cur_query_id = query_id

            self.log.info('query id: #%d %s', idx, query_id)

            try:
                while True:
                    exec_resp = self._client.get_query_execution(
                        QueryExecutionId=query_id)
                    status = exec_resp['QueryExecution']['Status']['State']
                    if status != 'RUNNING':
                        break

                    self.log.info('query is running, wait 3 seconds')
                    time.sleep(3)
            finally:
                with self._lock:
                    self._cur_query_id = None

            self.log.info('query finished: #%d', idx)
            if status != 'SUCCEEDED':
                self.log.info('Query Detail: %s', exec_resp)
                err_msg = 'Query ({}) Failure: {}'.format(query_id, status)
                raise AirflowException(err_msg)

    def kill(self):
        if not self._client:
            return

        with self.lock:
            self._kill = True

            query_id = self._cur_query_id
            if not query_id:
                return

        self._client.stop_query_execution(QueryExecutionId=query_id)


class AwsAthenaOperator(BaseOperator):

    template_fields = ('request_token', 'schema', 'sql', 'result_config',)
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self,
                 sql,
                 aws_conn_id='aws_default',
                 region_name=None,
                 request_token=None,
                 schema=None,
                 result_config=None,
                 *args, **kwargs):
        super(AwsAthenaOperator, self).__init__(*args, **kwargs)

        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.request_token = request_token
        self.schema = schema
        self.sql = sql
        self.result_config = result_config

        self._hook = None

    def _get_hook(self):
        return AwsAthenaHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context):
        self._hook = self._get_hook()
        self._hook.run(sql=self.sql,
                       region_name=self.region_name,
                       request_token=self.request_token,
                       schema=self.schema,
                       result_config=self.result_config)

    def on_kill(self):
        if self._hook:
            self._hook.kill()


class AwsAthenaPlugin(AirflowPlugin):
    name = "awsathena_plugin"
    hooks = [AwsAthenaHook]
    operators = [AwsAthenaOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
