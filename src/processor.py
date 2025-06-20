import logging
import multiprocessing
from operator import is_
import traceback
from concurrent.futures import Future, ProcessPoolExecutor
from datetime import datetime, timedelta
from multiprocessing import Queue
from multiprocessing.synchronize import Event as EventMP
from queue import Empty

import numpy as np
import pandas as pd
from rich.progress import TaskID
from typing_extensions import override

from src.schemas import LoadComplete
from src.utils.aggregate import (
    COLS_KEYS_MAPPING,
    merge_current_with_prev,
    values_by_timeperiods_func,
)
from src.utils.reading import preprocess_metric_df


class Worker(multiprocessing.Process):
    def __init__(
        self,
        worker_id: int,
        queue_in: "Queue[LoadComplete]",
        stop_event: EventMP,
        report_queue: "Queue[tuple[TaskID, bool]]",
        timedeltas: list[timedelta],
        aggregations: list[list[str]],
    ):
        super().__init__(daemon=False)
        self.queue_in: "Queue[LoadComplete]" = queue_in
        self._report_queue: Queue[tuple[TaskID, bool]] = report_queue
        self.worker_id: int = worker_id

        self._base_logger: logging.Logger = logging.getLogger(__name__)
        self._cluster_in_progress: str | None = None
        self.logger: logging.LoggerAdapter[logging.Logger] = logging.LoggerAdapter(
            self._base_logger,
            {
                "worker_id": str(self.worker_id),
                "cluster": str(self._cluster_in_progress),
            },
        )
        self._stop_event: EventMP = stop_event
        self._previous_datasets: dict[str, pd.DataFrame] = {}
        self._previous_time: datetime | None = None

        self._timedeltas: list[timedelta] = timedeltas
        self._aggregations: list[list[str]] = aggregations
        self._resulted_datasets: dict[
            str, dict[timedelta, dict[str, pd.DataFrame]]
        ] = {}
        # INFO: метрика > временной промежуток > агрегация

        self.nested_executor: ProcessPoolExecutor

    def _update_df(
        self,
        metric: str,
        period: timedelta,
        aggregation: str,
        aggregate_df: pd.DataFrame,
    ):
        if metric in self._resulted_datasets:
            if period in self._resulted_datasets[metric]:
                if aggregation in self._resulted_datasets[metric][period]:
                    curr_df = self._resulted_datasets[metric][period][aggregation]
                    new_df = pd.concat([curr_df, aggregate_df])
                    self._resulted_datasets[metric][period][aggregation] = (
                        new_df.sort_index()
                    )
                else:
                    self._resulted_datasets[metric][period].update(
                        {aggregation: aggregate_df}
                    )
            else:
                self._resulted_datasets[metric].update(
                    {period: {aggregation: aggregate_df}}
                )
        else:
            self._resulted_datasets.update(
                {metric: {period: {aggregation: aggregate_df}}}
            )

        self.logger.debug(
            f"\n{self._resulted_datasets[metric][period][aggregation].tail(6)}"
        )
        self.logger.debug(
            f"Dimesion:\n{self._resulted_datasets[metric][period][aggregation].shape}"
        )

    def _process_seconds_count(
        self,
        current_dataframe: pd.DataFrame,
        merged_dataset: pd.DataFrame,
        task: LoadComplete,
    ):
        if not isinstance(self._previous_time, datetime):
            if not task.is_end:
                return
            self._previous_time = task.loaded_datetime

        for period in self._timedeltas:
            values_by_timeperiods = values_by_timeperiods_func(
                merged_dataset, self._previous_time, period
            )

            # INFO: дальше идёт суммирование и агрегации
            # тут тоже вычисляем количество изменений в промежутке
            dropnan = lambda a: a[~np.isnan(a)]
            change_by_timeperiod = values_by_timeperiods.map(dropnan)
            change_by_timeperiod = change_by_timeperiod.map(np.diff)
            change_by_timeperiod = change_by_timeperiod.map(
                np.clip, a_min=0, a_max=None
            )
            change_by_timeperiod = change_by_timeperiod.map(np.sum)

            for aggregation in self._aggregations:
                keys = [merged_dataset[col] for col in aggregation]

                groups = change_by_timeperiod.groupby(keys)
                sum_df = groups.sum().T
                sum_df = sum_df.add_suffix("_sum")
                max_df = groups.max().T
                max_df = max_df.add_suffix("_max")
                mean_df = groups.mean().T
                mean_df = mean_df.add_suffix("_mean")
                std_df = groups.std().T
                std_df = std_df.add_suffix("_std")

                aggregate_df = sum_df.join([max_df, mean_df, std_df])
                self._update_df(
                    "application_stats_seconds_count",
                    period,
                    "-".join(aggregation),
                    aggregate_df,
                )

        self._previous_datasets["application_stats_seconds_count"] = current_dataframe

    def _process_error_total(
        self,
        current_dataframe: pd.DataFrame,
        merged_dataset: pd.DataFrame,
        task: LoadComplete,
    ):
        if not isinstance(self._previous_time, datetime):
            if not task.is_end:
                return
            self._previous_time = task.loaded_datetime
        for period in self._timedeltas:
            values_by_timeperiods = values_by_timeperiods_func(
                merged_dataset, self._previous_time, period
            )

            # INFO: дальше идёт суммирование и агрегации
            # в ошибках делаем только разность из большего меньшее
            dropnan = lambda a: a[~np.isnan(a)]
            change_by_timeperiod = values_by_timeperiods.map(dropnan)
            change_by_timeperiod = change_by_timeperiod.map(np.diff)
            change_by_timeperiod = change_by_timeperiod.map(
                np.clip, a_min=0, a_max=None
            )
            change_by_timeperiod = change_by_timeperiod.map(np.sum)

            for aggregation in self._aggregations:
                keys = [merged_dataset[col] for col in aggregation]

                groups = change_by_timeperiod.groupby(keys)
                sum_df = groups.sum().T
                sum_df = sum_df.add_suffix("_sum")
                max_df = groups.max().T
                max_df = max_df.add_suffix("_max")
                mean_df = groups.mean().T
                mean_df = mean_df.add_suffix("_mean")
                std_df = groups.std().T
                std_df = std_df.add_suffix("_std")

                aggregate_df = sum_df.join([max_df, mean_df, std_df])
                self._update_df(
                    "application_stats_error_total",
                    period,
                    "-".join(aggregation),
                    aggregate_df,
                )

        self._previous_datasets["application_stats_error_total"] = current_dataframe

    def _perform_merge(self, preprocessed_data: dict[str, pd.DataFrame], is_end: bool):
        futures: dict[str, Future[pd.DataFrame]] = {}

        result: dict[str, pd.DataFrame] = {}
        for metric_name, metric_data in preprocessed_data.items():
            self.logger.info(f"Try merge {metric_name}")
            if metric_data.empty:
                # если данные пустые, то пропускаем обработку
                # пока не окажемся на конце кластера
                if is_end:
                    # в конце кластера вставляем предыдущий датасет просто, если есть
                    prev_dataset = self._previous_datasets.get(metric_name)
                    if isinstance(prev_dataset, pd.DataFrame):
                        result.update({metric_name: prev_dataset})

                self.logger.info(f"Metric data empty {metric_name}")
                continue
            if not isinstance(self._previous_time, datetime):
                # если нет предыдущего датасета, то текущий записываем в него
                # и идём дальше
                self._previous_datasets[metric_name] = metric_data
                self.logger.info(f"previous_dataset {metric_name} inited")
                continue

            # считаем что сейчас предыдущий точно есть, так как установлен _previous_time
            previous_dataset = self._previous_datasets[metric_name]
            future = self.nested_executor.submit(
                merge_current_with_prev,
                current_df=metric_data,
                prev_df=previous_dataset,
                key_cols=COLS_KEYS_MAPPING[metric_name],
            )
            futures.update({metric_name: future})

        for metric, future in futures.items():
            preproces_res_df = future.result()
            result.update({metric: preproces_res_df})
            self.logger.info(f"Merged {metric}")

        return result

    def _preprocess_data(self, task: LoadComplete):
        filename_err = task.metrics["application_stats_error_total"]
        filename_count = task.metrics["application_stats_seconds_count"]
        filename_seconds = task.metrics["application_stats_seconds"]

        preprocess_futures: dict[str, Future[pd.DataFrame]] = {}
        preprocess_futures.update(
            {
                "application_stats_error_total": self.nested_executor.submit(
                    preprocess_metric_df, file_path=filename_err
                )
            }
        )
        preprocess_futures.update(
            {
                "application_stats_seconds_count": self.nested_executor.submit(
                    preprocess_metric_df, file_path=filename_count
                )
            }
        )
        preprocess_futures.update(
            {
                "application_stats_seconds": self.nested_executor.submit(
                    preprocess_metric_df, file_path=filename_seconds
                )
            }
        )
        result_preprocessing: dict[str, pd.DataFrame] = {}

        for metric, future in preprocess_futures.items():
            preproces_res_df = future.result()
            result_preprocessing.update({metric: preproces_res_df})
            self.logger.info(f"Preprocessed {metric}")

        return result_preprocessing

    @override
    def run(self):
        self.nested_executor = ProcessPoolExecutor(max_workers=3)
        while True:
            try:
                task = self.queue_in.get(block=True, timeout=10)
                self.logger.info(f"Accept task datetime: {task.loaded_datetime}")

                if not isinstance(self._cluster_in_progress, str):
                    self._cluster_in_progress = task.cluster_name
                    self.logger.info(
                        f"start processing cluster: {self._cluster_in_progress}"
                    )
                if isinstance(self.logger.extra, dict):
                    self.logger.extra.update(
                        {"cluster": str(self._cluster_in_progress)}
                    )

                result_preprocessing = self._preprocess_data(task)

                result_merge = self._perform_merge(result_preprocessing, task.is_end)

                if self._previous_time:
                    self.logger.debug(
                        f"start processing time: {self._previous_time.isoformat()}"
                    )
                    # INFO: обрабатываем метрики и набираем датасеты
                    self._process_error_total(
                        result_preprocessing["application_stats_error_total"],
                        result_merge["application_stats_error_total"],
                        task,
                    )
                    self._process_seconds_count(
                        result_preprocessing["application_stats_seconds_count"],
                        result_merge["application_stats_seconds_count"],
                        task,
                    )
                else:
                    self.logger.info("Sckiped first record")

                if self._previous_time:
                    self._report_queue.put((task.task_id, False))

                # устанавливаем предыдущее время на текущую запись
                self._previous_time = task.loaded_datetime

                if task.is_end:
                    # INFO: был передан последний файл. Его надо отдельно обработать
                    self.logger.info(
                        f"end processing cluster: {self._cluster_in_progress}"
                    )
                    self.logger.debug(
                        f"processing time: {self._previous_time.isoformat()}"
                    )
                    # почистили прошлый датасет
                    self._previous_datasets = {}

                    self._process_error_total(
                        result_preprocessing["application_stats_error_total"],
                        result_merge["application_stats_error_total"],
                        task,
                    )
                    self._process_seconds_count(
                        result_preprocessing["application_stats_seconds_count"],
                        result_merge["application_stats_seconds_count"],
                        task,
                    )

                    self._previous_datasets = {}
                    self._cluster_in_progress = None
                    self._report_queue.put((task.task_id, True))

                if isinstance(self.logger.extra, dict):
                    self.logger.extra.update(
                        {"cluster": str(self._cluster_in_progress)}
                    )

            except Empty:
                self.logger.warning("No tasks consumed")
            except Exception:
                self.logger.critical(traceback.format_exc())
            finally:
                if self._stop_event.is_set():
                    self.logger.warning("processor was interrupted")
                    break

        self.nested_executor.shutdown()
        self.logger.warning("processor end")

    def _log_datasets2mlflow(self):
        pass
