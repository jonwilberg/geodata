import logging

import pendulum
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2024, 10, 27, tz="UTC"),
    catchup=False,
)
def test_dag():
    """
    ### Test DAG
    A simple DAG to test the task functions.
    """

    @task()
    def test_task() -> None:
        """
        #### Test Task
        A simple task that logs a message.
        """
        logger.info("Hello, World!")

    test_task()


dag = test_dag()
