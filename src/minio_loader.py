import logging
import os
import tempfile
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from multiprocessing import Queue

import pandas as pd
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client
from rich.progress import TaskID

from src.schemas import LoadComplete
from src.utils.qmanager import QueueManager

logger = logging.getLogger("LoadClusterTask")


def list_months(start: datetime, end: datetime):
    result: list[str] = []
    year, month = start.year, start.month

    while (year < end.year) or (year == end.year and month <= end.month):
        result.append(f"{year}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1

    return result


class LoadClusterTask:
    def __init__(
        self,
        minio_client: S3Client,
        bucket_name: str,
        cluster_name: str,
        q_manager: QueueManager,
        start_time: datetime,
        end_time: datetime,
        metrics_list: list[str],
        task_id: TaskID,
        times2load: pd.DatetimeIndex,
    ):
        self._minio_client: S3Client = minio_client
        self.cluster_name: str = cluster_name
        self.bucket_name: str = bucket_name

        self._start_time: datetime = start_time
        self._end_time: datetime = end_time

        self._metrics_list: list[str] = metrics_list
        self._pool_executor: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=len(self._metrics_list)
        )
        self.logger: logging.LoggerAdapter[logging.Logger] = logging.LoggerAdapter(
            logger, {"cluster": self.cluster_name}
        )
        self._task_id: TaskID = task_id
        self._times2load: pd.DatetimeIndex = times2load

        self._q_manager: QueueManager = q_manager
        self._processor_queue: "Queue[LoadComplete]"

    def __call__(self):
        """ "
        1. проверяется что метрики существуют на minio
        2. вычитываются папки с месяцами и отбираются только подходящие по времени
        3. проверяется что папки с теми же месяцами есть в других метриках
        4. выполняется итерация с промежутком в час от начала и до конца периода.
           результат генерации отсылается во все метрики с нужным месяцем.
           сгружаются сразу все файлы по всем метрикам
        5. формируется сообщение в очередь.
           сообщение имеет вид словаря с ключами метриками, значением - инстансом файла.
           ещё пометку времени надо не забыть.
           если файл загрузить не удалось, кидаем warning и отправляем пустое значение.
        """

        self.logger.info("Task started")

        # сначала пытаемся получить свободную очередь
        self._processor_queue, lock = self._q_manager.get_free_queue()

        try:
            # получаем список метрик
            objects_from_minio = self._list_objects(prefix="/")
            metrics = [
                metric for metric in objects_from_minio if metric in self._metrics_list
            ]

            # проверяем что все метрики есть на минио
            if set(metrics) != set(self._metrics_list):
                self.logger.critical(
                    f"not all metrics presence in minio: \n\
                in minio: {objects_from_minio} \n\
                requested: {self._metrics_list}"
                )
                raise Exception("not all metrics presence in minio")
            self.logger.info("metrics check passed")

            # проверяем временные промежутки по месяцам
            objects_month4check = list_months(self._start_time, self._end_time)
            for metric in self._metrics_list:
                months_from_minio = self._list_objects(prefix=metric)
                if not all(month in months_from_minio for month in objects_month4check):
                    self.logger.critical(
                        f"not all months presence in minio: \n\
                    in minio: {months_from_minio} \n\
                    requested: {objects_month4check}"
                    )
                    raise Exception("not all months presence in minio")
            self.logger.info("months check passed")

            metric2files: dict[str, list[tuple[str, int]]] = {}
            # проверяем что все метрики для этого кластера не нулевые
            for metric in self._metrics_list:
                for month in objects_month4check:
                    clusters_from_minio = self._list_objects(
                        prefix=metric + "/" + month
                    )
                    if self.cluster_name not in clusters_from_minio:
                        self.logger.critical(
                            f"cluster not presence in minio, metric {metric}: \n\
                        in minio: {clusters_from_minio} \n\
                        requested: {self.cluster_name}"
                        )
                        raise Exception("cluster_name not presence in minio")
                    files_sizes = self._get_files_with_sizes(
                        prefix="/".join([metric, month, self.cluster_name])
                    )
                    empty_files = [item for item in files_sizes if item[1] <= 500]
                    if len(empty_files) == len(files_sizes):
                        self.logger.critical(
                            f"cluster is empty files in minio, metric {metric}"
                        )
                        raise Exception("cluster is empty in minio")

                    metric2files.update({metric: files_sizes})

            self.logger.info("cluster files sizes passed")

            self.logger.info("Start loading files from minio")
            for timestamp in self._times2load:
                filename = timestamp.isoformat(timespec="seconds") + ".json"
                futures: dict[str, Future[str]] = {}
                for metric in self._metrics_list:
                    minio_file_key = "/".join(
                        [
                            metric,
                            f"{timestamp.year}-{timestamp.month:02d}",
                            self.cluster_name,
                            filename,
                        ]
                    )
                    future = self._pool_executor.submit(
                        self._download_obj, minio_file_key
                    )
                    futures.update({metric: future})

                results: dict[str, str | None] = {}

                for metric, future in futures.items():
                    try:
                        file_name = future.result()
                        results.update({metric: file_name})
                    except Exception:
                        results.update({metric: None})

                self.logger.debug(f"Loaded time: {timestamp.isoformat()}")
                is_end = timestamp.to_pydatetime() == self._end_time
                msg = LoadComplete(
                    cluster_name=self.cluster_name,
                    loaded_datetime=timestamp.to_pydatetime(),
                    metrics=results,
                    is_end=is_end,
                    task_id=self._task_id,
                )

                # пытаемся положить и ждём пока очередь разблокируется
                start = datetime.now().timestamp()
                self._processor_queue.put(msg, block=True)
                end = datetime.now().timestamp()
                wait_time = end - start
                self.logger.debug(f"Wait queue: {wait_time:.2f} seconds")
        finally:
            # освобождаем очередь
            self.logger.info("Queue released")
            self._q_manager.release_queue(lock)

        self.logger.info("Task finished")

    def _get_files_with_sizes(self, prefix: str):
        if not prefix.endswith("/"):
            prefix += "/"
        paginator = self._minio_client.get_paginator("list_objects_v2")
        result = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=prefix,
            Delimiter="/",
        )

        key2size: list[tuple[str, int]] = []

        for page in result:
            contents = page.get("Contents")
            if not isinstance(contents, list):
                self.logger.warning(
                    "_get_sizes_of_objects: contents is None, skiped page"
                )
                continue
            for content in contents:
                key = content.get("Key")
                size = content.get("Size")
                if not isinstance(size, int):
                    self.logger.warning("_get_sizes_of_objects: size is None")
                    continue
                if not isinstance(key, str):
                    continue

                key2size.append((key, size))

        self.logger.info(f"_get_sizes_of_objects: get files with sizes: {prefix}")
        self.logger.debug(key2size)

        return key2size

    def _list_objects(self, prefix: str):
        if not prefix.endswith("/"):
            prefix += "/"
        if prefix.startswith("/"):
            prefix = prefix[1:]
        paginator = self._minio_client.get_paginator("list_objects_v2")
        result = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=prefix,
            Delimiter="/",
        )
        objects: list[str] = []
        for page in result:
            common_prefixes = page.get("CommonPrefixes")
            if not isinstance(common_prefixes, list):
                self.logger.warning(
                    "_list_objects: common_prefixes is None, skiped page"
                )
                continue

            for cp in common_prefixes:
                tmp_prefix = cp.get("Prefix")
                if not isinstance(tmp_prefix, str):
                    self.logger.warning(
                        f"_list_objects: tmp_prefix not int, type: {str(type(tmp_prefix))}"
                    )
                    continue
                objects.append(tmp_prefix.rstrip("/").split("/")[-1])

        self.logger.info(f"_list_objects: get list objects: {prefix}")
        self.logger.debug(objects)

        return objects

    def _download_obj(self, file_key: str) -> str:
        temp_file = tempfile.NamedTemporaryFile(
            mode="w+b", delete=False, suffix=self.cluster_name
        )
        try:
            # Скачиваем содержимое и пишем прямо в дескриптор
            self._minio_client.download_fileobj(self.bucket_name, file_key, temp_file)

            # Сбрасываем позицию в начало файла перед передачей

            temp_file.close()
            # Передаем числовой дескриптор в очередь
            return temp_file.name
        except ClientError as e:
            if e.operation_name == "HeadObject":
                self.logger.error(f"File not found: {file_key}")
            temp_file.close()
            os.remove(temp_file.name)
            raise e

        except Exception as e:
            # В случае ошибки закрываем дескриптор и удаляем файл
            self.logger.error("error with download, close file and delete")
            self.logger.error(traceback.format_exc())
            temp_file.close()
            os.remove(temp_file.name)
            raise e
