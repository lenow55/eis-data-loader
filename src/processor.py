import logging
import multiprocessing
import traceback
from collections import defaultdict
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
    mode_fft_via_hist,
    safe_median,
    values_by_timeperiods_func,
)
from src.utils.log_handlers import RotatingFileHandler
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

        self._base_logger: logging.Logger = logging.getLogger(
            f"{__name__}.{self.worker_id}"
        )
        handler = RotatingFileHandler(
            f"./logs/worker_id_{self.worker_id}.log", maxBytes=10485760, backupCount=5
        )
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s - %(worker_id)s:%(cluster)s:%(funcName)s:%(lineno)d - %(levelname)s - %(message)s"
            )
        )
        self._base_logger.addHandler(handler)
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
        merged_dataset: pd.DataFrame,
        values_by_timeperiods: pd.DataFrame,
        period: timedelta,
    ):
        dropnan = lambda a: a[~np.isnan(a)]
        change_by_timeperiod = values_by_timeperiods.map(dropnan)
        change_by_timeperiod = change_by_timeperiod.map(np.diff)
        change_by_timeperiod = change_by_timeperiod.map(np.clip, a_min=0, a_max=None)
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

    def _process_devide_error_by_requests(
        self,
        merged_dataset_err: pd.DataFrame,
        values_by_timeperiods_err: pd.DataFrame,
        merged_dataset_req: pd.DataFrame,
        values_by_timeperiods_req: pd.DataFrame,
        period: timedelta,
    ):
        self.logger.info("process errors by requests")
        dropnan = lambda a: a[~np.isnan(a)]
        # подготавливаем ошибки
        change_by_timeperiod_err = values_by_timeperiods_err.map(dropnan)
        change_by_timeperiod_err = change_by_timeperiod_err.map(np.diff)
        change_by_timeperiod_err = change_by_timeperiod_err.map(
            np.clip, a_min=0, a_max=None
        )
        change_by_timeperiod_err = change_by_timeperiod_err.map(np.sum)

        # подготавливаем запросы
        change_by_timeperiod_req = values_by_timeperiods_req.map(dropnan)
        change_by_timeperiod_req = change_by_timeperiod_req.map(np.diff)
        change_by_timeperiod_req = change_by_timeperiod_req.map(
            np.clip, a_min=0, a_max=None
        )
        change_by_timeperiod_req = change_by_timeperiod_req.map(np.sum)

        change_by_timeperiod = change_by_timeperiod_err.div(
            change_by_timeperiod_req, fill_value=0
        )
        change_by_timeperiod = change_by_timeperiod.fillna(value=-1)
        change_by_timeperiod = change_by_timeperiod.replace([np.inf, -np.inf], 0)

        for aggregation in self._aggregations:
            keys = [merged_dataset_err[col] for col in aggregation]

            groups = change_by_timeperiod.groupby(keys)
            sum_df = groups.sum(numeric_only=True).T
            sum_df = sum_df.add_suffix("_sum")
            max_df = groups.max(numeric_only=True).T
            max_df = max_df.add_suffix("_max")
            mean_df = groups.mean(numeric_only=True).T
            mean_df = mean_df.add_suffix("_mean")
            std_df = groups.std(numeric_only=True).T
            std_df = std_df.add_suffix("_std")

            aggregate_df = sum_df.join([max_df, mean_df, std_df])
            self._update_df(
                "errors_by_requests",
                period,
                "-".join(aggregation),
                aggregate_df,
            )

    def _process_seconds_count(
        self,
        merged_dataset: pd.DataFrame,
        values_by_timeperiods: pd.DataFrame,
        period: timedelta,
    ):
        self.logger.info("process application_stats_seconds_count")
        dropnan = lambda a: a[~np.isnan(a)]
        change_by_timeperiod = values_by_timeperiods.map(dropnan)
        change_by_timeperiod = change_by_timeperiod.map(np.diff)
        change_by_timeperiod = change_by_timeperiod.map(np.clip, a_min=0, a_max=None)
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

    def _process_error_total(
        self,
        merged_dataset: pd.DataFrame,
        values_by_timeperiods: pd.DataFrame,
        period: timedelta,
    ):
        self.logger.info("process application_stats_error_total")
        dropnan = lambda a: a[~np.isnan(a)]
        change_by_timeperiod = values_by_timeperiods.map(dropnan)
        change_by_timeperiod = change_by_timeperiod.map(np.diff)
        change_by_timeperiod = change_by_timeperiod.map(np.clip, a_min=0, a_max=None)
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
            self.logger.debug(f"\n{aggregate_df.head(10)}")

    def _compute_quantile(
        self,
        merged_dataset: pd.DataFrame,
        change_by_timeperiod: pd.DataFrame,
        period: timedelta,
    ):
        for aggregation in self._aggregations:
            keys = [merged_dataset[col] for col in aggregation]
            keys.append(merged_dataset["quantile"])

            groups = change_by_timeperiod.groupby(keys)
            q50_df = groups.quantile(q=0.5).T.add_suffix("_q50")
            q75_df = groups.quantile(q=0.75).T.add_suffix("_q75")
            q90_df = groups.quantile(q=0.90).T.add_suffix("_q90")
            q95_df = groups.quantile(q=0.95).T.add_suffix("_q95")
            q99_df = groups.quantile(q=0.99).T.add_suffix("_q99")

            aggregate_df = q50_df.join([q75_df, q90_df, q95_df, q99_df])
            aggregate_df.columns = aggregate_df.columns.droplevel(0)
            self._update_df(
                f"application_stats_seconds",
                period,
                "-".join(aggregation),
                aggregate_df,
            )
            self.logger.debug(f"application_stats_seconds\n{aggregate_df.head(10)}")

    def _process_execution_time(
        self,
        merged_dataset: pd.DataFrame,
        values_by_timeperiods: pd.DataFrame,
        period: timedelta,
    ):
        self.logger.info("process application_stats_seconds")

        change_by_timeperiod_median = values_by_timeperiods.map(safe_median)

        self._compute_quantile(merged_dataset, change_by_timeperiod_median, period)

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

    def _perform_merge(self, preprocessed_data: dict[str, pd.DataFrame], is_end: bool):
        result: dict[str, pd.DataFrame] = {}
        for metric_name, metric_data in preprocessed_data.items():
            self.logger.info(f"Try merge {metric_name}")
            if metric_data.empty:
                # если данные пустые, то пропускаем обработку
                # пока не окажемся на конце кластера
                # в конце кластера вставляем предыдущий датасет просто, если есть
                prev_dataset = self._previous_datasets.get(metric_name)
                if isinstance(prev_dataset, pd.DataFrame):
                    result.update({metric_name: prev_dataset})

                self.logger.info(f"Metric data empty {metric_name}")
                continue
            if metric_name not in self._previous_datasets:
                # если нет предыдущего датасета, то текущий записываем в него
                # и идём дальше
                self._previous_datasets[metric_name] = metric_data
                self.logger.info(f"previous_dataset {metric_name} inited")
                continue

            # считаем что сейчас предыдущий точно есть, так как установлен _previous_time
            previous_dataset = self._previous_datasets[metric_name]

            result.update(
                {
                    metric_name: merge_current_with_prev(
                        current_df=metric_data,
                        prev_df=previous_dataset,
                        key_cols=COLS_KEYS_MAPPING[metric_name],
                    )
                }
            )
            self.logger.info(f"Merged {metric_name}")
            self._previous_datasets[metric_name] = metric_data

        return result

    def _perform_values_by_timedelta(self, merged_data: dict[str, pd.DataFrame]):
        result: dict[tuple[str, timedelta], pd.DataFrame] = {}

        for metric_name, metric_data in merged_data.items():
            if not isinstance(self._previous_time, datetime):
                continue
            for period in self._timedeltas:
                self.logger.info(
                    f"Try compute values_by_timeperiods: {metric_name}; timedelta: {period}"
                )

                # считаем что сейчас предыдущий точно есть, так как установлен _previous_time
                res_df = values_by_timeperiods_func(
                    merged_dataset=metric_data,
                    start_time=self._previous_time,
                    period=period,
                )
                self.logger.info(
                    f"Computed values_by_timeperiods {metric_name}; timedelta: {period}"
                )

                result.update({(metric_name, period): res_df})

        return result

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

                result_val_by_period = self._perform_values_by_timedelta(
                    merged_data=result_merge
                )

                if self._previous_time:
                    self.logger.debug(
                        f"start processing time: {self._previous_time.isoformat()}"
                    )
                else:
                    self.logger.info("Sckiped first record")

                grouped: defaultdict[timedelta, dict[str, pd.DataFrame]] = defaultdict(
                    dict
                )
                for key, item in result_val_by_period.items():
                    metric, period = key
                    if metric == "application_stats_error_total":
                        self._process_error_total(result_merge[metric], item, period)
                    if metric == "application_stats_seconds_count":
                        self._process_seconds_count(result_merge[metric], item, period)
                    if metric == "application_stats_seconds":
                        self._process_execution_time(result_merge[metric], item, period)
                    grouped[period][metric] = item

                for period, metric_group in grouped.items():
                    if (
                        "application_stats_error_total" in metric_group
                        and "application_stats_seconds_count" in metric_group
                    ):
                        self._process_devide_error_by_requests(
                            merged_dataset_err=result_merge[
                                "application_stats_error_total"
                            ],
                            merged_dataset_req=result_merge[
                                "application_stats_seconds_count"
                            ],
                            values_by_timeperiods_err=metric_group[
                                "application_stats_error_total"
                            ],
                            values_by_timeperiods_req=metric_group[
                                "application_stats_seconds_count"
                            ],
                            period=period,
                        )

                if self._previous_time:
                    self._report_queue.put((task.task_id, False))

                # устанавливаем предыдущее время на текущую запись
                self._previous_time = task.loaded_datetime

                if task.is_end:
                    # INFO: был передан последний файл. Его надо отдельно обработать
                    self.logger.info(
                        f"end processing cluster: {self._cluster_in_progress}"
                    )
                    self._previous_time = task.loaded_datetime

                    self.logger.debug(
                        f"processing time: {self._previous_time.isoformat()}"
                    )

                    result_val_by_period = self._perform_values_by_timedelta(
                        merged_data=result_merge
                    )
                    grouped: defaultdict[timedelta, dict[str, pd.DataFrame]] = (
                        defaultdict(dict)
                    )
                    for key, item in result_val_by_period.items():
                        metric, period = key
                        if metric == "application_stats_error_total":
                            self._process_error_total(
                                result_merge[metric], item, period
                            )
                        if metric == "application_stats_seconds_count":
                            self._process_seconds_count(
                                result_merge[metric], item, period
                            )
                        if metric == "application_stats_seconds":
                            self._process_execution_time(
                                result_merge[metric], item, period
                            )
                        grouped[period][metric] = item

                    for period, metric_group in grouped.items():
                        if (
                            "application_stats_error_total" in metric_group
                            and "application_stats_seconds_count" in metric_group
                        ):
                            self._process_devide_error_by_requests(
                                merged_dataset_err=result_merge[
                                    "application_stats_error_total"
                                ],
                                merged_dataset_req=result_merge[
                                    "application_stats_seconds_count"
                                ],
                                values_by_timeperiods_err=metric_group[
                                    "application_stats_error_total"
                                ],
                                values_by_timeperiods_req=metric_group[
                                    "application_stats_seconds_count"
                                ],
                                period=period,
                            )

                    self._previous_datasets = {}
                    self._previous_time = None
                    self._report_queue.put((task.task_id, True))

                    for metric, item in self._resulted_datasets.items():
                        for period, item2 in item.items():
                            for aggregate, item3 in item2.items():
                                self.logger.info(
                                    f"END: {metric} -> {period} -> {aggregate}: shape: {item3.shape}"
                                )
                                file_path = f"datasets/{self._cluster_in_progress}_{metric}_{period.total_seconds()}_{aggregate}.csv"
                                item3.to_csv(file_path)
                                self.logger.info(f"Saved: {file_path}")

                    # сбрасываем информацию о кластере
                    self._cluster_in_progress = None
                    self._resulted_datasets = {}

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
