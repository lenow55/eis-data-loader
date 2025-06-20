from datetime import UTC, datetime, timedelta
from functools import partial

import numpy as np
import pandas as pd

COLS_KEYS_MAPPING = {
    "application_stats_error_total": [
        "pod_uid",
        "context",
        "group",
        "operation",
        "app",
        "pod_node_name",
    ],
    "application_stats_seconds_count": [
        "pod_uid",
        "context",
        "group",
        "operation",
        "app",
        "pod_node_name",
    ],
    "application_stats_seconds": [
        "pod_uid",
        "context",
        "group",
        "operation",
        "app",
        "pod_node_name",
        "quantile",
    ],
}


def unix_ts2datetime(timestamps: list[int]):
    # нужно вычитать три часа, чтобы преобразовать во время utc
    return [datetime.fromtimestamp(timestamp / 1000.0, UTC) for timestamp in timestamps]


def values_by_timeperiods_func(
    merged_dataset: pd.DataFrame,
    start_time: datetime,
    period: timedelta,
):
    end_time = start_time + timedelta(hours=1)
    intervals = pd.date_range(start_time, end_time, freq=period, tz=UTC)
    bins = pd.IntervalIndex.from_breaks(intervals, closed="left")
    # Добавим уникальный идентификатор строки, если их несколько
    merged_dataset = merged_dataset.reset_index()
    df = merged_dataset.explode(["timestamps", "values"])
    df["timestamps"] = pd.to_datetime(df["timestamps"], utc=True)
    df["interval"] = pd.cut(df["timestamps"], bins)
    # Группировка: по исходной группе (index) и интервалу
    grouped = (
        df.groupby(["index", "interval"])["values"]
        .apply(lambda x: np.array(list(x), dtype=float))
        .reset_index()
    )

    # Добавляем первое значение после конца интервала для каждой группы
    def append_after(row):
        idx, interval = row["index"], row["interval"]
        mask = (df["index"] == idx) & (df["timestamps"] >= interval.right)
        after = df.loc[mask, "values"]
        arr = row["values"]
        if not after.empty:  # pyright: ignore[reportAttributeAccessIssue]
            arr = np.append(arr, after.iloc[0])  # pyright: ignore[reportAttributeAccessIssue]
        return arr

    grouped["values"] = grouped.apply(append_after, axis=1)
    # Привести к формату: index -> интервалы -> значения
    result = grouped.pivot(index="index", columns="interval", values="values")
    result.columns = [i.right for i in result.columns]  # pyright: ignore[reportAttributeAccessIssue]
    result = result.sort_index(axis=1)
    return result.reset_index(drop=True)


# def values_by_timeperiods_func(
#     merged_dataset: pd.DataFrame,
#     start_time: datetime,
#     period: timedelta,
# ):
#     end_time: datetime = start_time + timedelta(hours=1)
#     time_intervals: np.ndarray = pd.date_range(
#         start_time, end_time, freq=period, tz=UTC
#     ).to_pydatetime()
#     # Формируем интервалы (start, end) как пары соседних времён
#     time_intervals_list: list[tuple[datetime, datetime]] = [
#         (time_intervals[i], time_intervals[i + 1])
#         for i in range(len(time_intervals) - 1)
#     ]
#     clamp_in_timedelta_p = partial(
#         clamp_in_timedelta,
#         time_intervals=time_intervals_list,
#         none_to_zero=False,
#     )
#     values_by_timeperiods = merged_dataset.loc[:, ["timestamps", "values"]].apply(
#         clamp_in_timedelta_p, axis=1
#     )
#     return values_by_timeperiods


def merge_current_with_prev(
    current_df: pd.DataFrame,
    prev_df: pd.DataFrame,
    key_cols: list[str],
) -> pd.DataFrame:
    # 1) Определяем ключевые колонки — все, кроме 'values' и 'timestamps'
    cols = key_cols + ["values", "timestamps"]

    if current_df.empty:
        return prev_df

    # 2) Выполняем inner-слияние по этим ключам
    df = pd.merge(prev_df, current_df, on=key_cols, suffixes=("_1", "_2"), how="left")

    # 3) Заменяем NaN в столбцах values_2 и timestamps_2 на пустые списки
    val_2_isna = df["values_2"].isna()
    time_2_isna = df["timestamps_2"].isna()
    df.loc[val_2_isna, "values_2"] = pd.Series([[]] * val_2_isna.sum()).values
    df.loc[time_2_isna, "timestamps_2"] = pd.Series([[]] * time_2_isna.sum()).values

    # 4) Конкатенируем списки из двух датафреймов
    df["values"] = df["values_1"] + df["values_2"]
    df["timestamps"] = df["timestamps_1"] + df["timestamps_2"]

    # 5) Убираем временные столбцы и приводим итог к нужному порядку
    df = df.drop(columns=["values_1", "values_2", "timestamps_1", "timestamps_2"])
    df = df[cols]

    return df


def clamp_in_timedelta(
    timestamps_values: tuple[list[datetime], list[float]],
    time_intervals: list[tuple[datetime, datetime]],
    none_to_zero: bool = True,
):
    """
    Извлекает значения из заданных временных интервалов с добавлением первого значения за границей интервала.

    Аргументы:
        timestamps_values (tuple[list[datetime], list[float]]): Кортеж из списков меток времени и соответствующих значений.
        time_intervals (list[tuple[datetime, datetime]]): Список временных интервалов (start, end).
        none_to_zero (bool, optional): Заменять ли значения None на 0. По умолчанию True.

    Возвращает:
        list[tuple[datetime, datetime, np.ndarray]]: Список кортежей (start, end, значения), где значения — numpy-массив значений за интервал с добавленным первым значением после конца.
    """
    timestamps, values = timestamps_values
    if len(timestamps) != len(values):
        raise ValueError("Length of timestamps and values must be equal")

    result = pd.Series()

    for start, end in time_intervals:
        # Выбираем индексы меток времени, попадающих в интервал [start, end)
        selected_indices = [i for i, t in enumerate(timestamps) if start <= t < end]
        # Собираем соответствующие значения
        selected_values = [values[i] for i in selected_indices]

        # Находим первое значение после конца интервала (если есть) и добавляем его
        after_end_indices = [i for i, t in enumerate(timestamps) if t >= end]
        if after_end_indices:
            val_after = values[after_end_indices[0]]
            selected_values.append(val_after)

        # При необходимости заменяем None на 0
        if none_to_zero:
            selected_values = [0 if v is None else v for v in selected_values]

        # Конвертируем в numpy массив с типом float
        filtered_values = np.array(selected_values, dtype=float)
        result[end] = filtered_values

    return result
