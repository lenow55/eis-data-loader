import json
import logging
import multiprocessing
import os
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta
from logging import config as log_config_m
from multiprocessing import Queue
from threading import Event

import pandas as pd
from boto3.session import Session
from botocore.config import Config
from mypy_boto3_s3.client import S3Client
from rich.progress import MofNCompleteColumn, Progress, TextColumn, TimeElapsedColumn

from src.config import ExternalMinioSettings
from src.minio_loader import LoadClusterTask
from src.processor import Worker
from src.schemas import AggregateComplete, LoadComplete
from src.utils.clusters import CLUSTERS
from src.utils.qmanager import QueueManager

settings_minio = ExternalMinioSettings()  # pyright: ignore[reportCallIssue]
# Пути к SSL сертификатам

logger = logging.getLogger(__name__)

with open(settings_minio.logging_conf_file) as l_f:
    logging_config_dict = json.loads(l_f.read())
    log_config_m.dictConfig(logging_config_dict)

# Создаем клиент S3 с сертификатами
session = Session()
client: S3Client = session.client(
    "s3",
    endpoint_url=settings_minio.endpoint_url,  # https для SSL
    aws_access_key_id=settings_minio.aws_access_key_id,
    aws_secret_access_key=settings_minio.aws_secret_access_key.get_secret_value(),
    verify=settings_minio.ca_cert,  # Проверка CA сертификата
    region_name=settings_minio.region_name,
    config=Config(
        signature_version="s3v4", retries={"max_attempts": 10, "mode": "standard"}
    ),
)

if __name__ == "__main__":
    # count_workers = 5
    count_workers = 1
    q_size = 8
    start_time = datetime.fromisoformat("2024-09-01T00:00:00")
    # start_time = datetime.fromisoformat("2024-12-28T00:00:00")
    end_time = datetime.fromisoformat("2025-01-01T00:00:00")
    # end_time = datetime.fromisoformat("2024-09-01T23:00:00")
    timedeltas = [timedelta(hours=1), timedelta(minutes=20)]
    aggregations = [["cluster"]]

    queues: list["Queue[LoadComplete]"] = [
        Queue(maxsize=q_size) for _ in range(count_workers)
    ]
    q_manager = QueueManager(queues)
    load_stop_event = Event()

    report_queue: "Queue[AggregateComplete]" = Queue()

    tasks: list[LoadClusterTask] = []
    progress = Progress(
        *Progress.get_default_columns(),
        TimeElapsedColumn(),
        MofNCompleteColumn(),
        TextColumn("{task.fields[check]}", justify="right"),
    )
    progress.start()

    times2load = pd.date_range(
        start=start_time,
        end=end_time,
        freq=timedelta(hours=1),
        inclusive="both",
    )

    for i, cluster in enumerate(CLUSTERS):
        task_id = progress.add_task(
            description=cluster,
            total=len(times2load),
            visible=False,
            start=False,
            check="",
        )
        task = LoadClusterTask(
            minio_client=client,
            bucket_name="metrics",
            cluster_name=cluster,
            q_manager=q_manager,
            report_queue=report_queue,
            stop_event=load_stop_event,
            start_time=start_time,
            end_time=times2load[-1],
            times2load=times2load,
            task_id=task_id,
            metrics_list=[
                "application_stats_error_total",
                "application_stats_seconds_count",
                "application_stats_seconds",
            ],
        )
        tasks.append(task)

    total_task = progress.add_task(
        description="Total", total=len(CLUSTERS), visible=True, start=True, check=""
    )
    stopEvent = multiprocessing.Event()

    workers: list[Worker] = []
    for i, queue in enumerate(queues):
        worker = Worker(
            worker_id=i,
            queue_in=queue,
            stop_event=stopEvent,
            report_queue=report_queue,
            timedeltas=timedeltas,
            aggregations=aggregations,
        )
        worker.start()

    executor = ThreadPoolExecutor(max_workers=count_workers)
    futures: list[Future[None]] = []
    for task in tasks:
        future = executor.submit(task)
        futures.append(future)

    pid = os.getpid()

    logger.info(f"start process: [{pid}]")

    try:
        while True:
            msg = report_queue.get()
            if not progress.tasks[msg.task_id].started:
                progress.start_task(msg.task_id)
                progress.tasks[msg.task_id].visible = True
            progress.advance(msg.task_id)

            if msg.is_end:
                progress.stop_task(msg.task_id)

                remaining = progress.tasks[msg.task_id].remaining
                if isinstance(remaining, float):
                    progress.advance(msg.task_id, remaining)

                if msg.is_empty:
                    progress.update(msg.task_id, check="empty")
                else:
                    progress.update(msg.task_id, check="ok")

                progress.advance(total_task, 1)
            if progress.finished:
                stopEvent.set()
                break
            progress.refresh()
        for worker in workers:
            worker.join()
        executor.shutdown()
    except KeyboardInterrupt:
        logger.error("program shutdown")
        load_stop_event.set()
        executor.shutdown(wait=True, cancel_futures=True)
    finally:
        logger.info("shutdown app")
        progress.stop()
        report_queue.close()
        q_manager.shutdown()
        client.close()
        stopEvent.set()
        for worker in workers:
            worker.join(timeout=30)
            if worker.is_alive():
                worker.terminate()
            worker.close()
        executor.shutdown(cancel_futures=True)

    logger.info("Close app")
