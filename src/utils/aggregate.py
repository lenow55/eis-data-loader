from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd
from numpy.typing import NDArray
from scipy.signal import fftconvolve

COLS_KEYS_MAPPING = {
    "application_stats_error_total": [
        "pod_uid",
        "context",
        "group",
        "operation",
        # "app",
        "pod_node_name",
        "cluster",
        "reason",
    ],
    "application_stats_seconds_count": [
        "pod_uid",
        "context",
        "group",
        "operation",
        # "app",
        "pod_node_name",
        "cluster",
    ],
    "application_stats_seconds": [
        "pod_uid",
        "context",
        "group",
        "operation",
        # "app",
        "pod_node_name",
        "quantile",
        "cluster",
    ],
}


def safe_quantile(arr: np.ndarray, q: float):
    arr = np.asarray(arr)
    if arr.size == 0:
        return np.nan  # или любое дефолтное значение
    return np.quantile(arr, q)


def unix_ts2datetime(timestamps: list[int]):
    # нужно вычитать три часа, чтобы преобразовать во время utc
    return [datetime.fromtimestamp(timestamp / 1000.0, UTC) for timestamp in timestamps]


def values_by_timeperiods_func(
    merged_dataset: pd.DataFrame,
    start_time: datetime,
    period: timedelta,
    non2zero: bool = False,
) -> pd.DataFrame:
    end_time = start_time + timedelta(hours=1)
    time_edges = pd.date_range(start=start_time, end=end_time, freq=period, tz=UTC)
    time_edges_np = time_edges.to_numpy()

    results = []
    interval_count = len(time_edges_np) - 1

    for index, row in merged_dataset.iterrows():
        ts_list = row["timestamps"]
        val_list = row["values"]

        values = np.array(val_list, dtype=np.float64)
        timestamps = pd.DatetimeIndex(ts_list, tz=UTC).to_numpy()
        valid_mask = ~pd.isnull(timestamps)
        timestamps = timestamps[valid_mask]
        values = values[valid_mask]

        if timestamps.size > 1:
            sort_idx = np.argsort(timestamps)
            timestamps = timestamps[sort_idx]
            values = values[sort_idx]

        idxs = np.searchsorted(timestamps, time_edges_np)
        row_data: list[NDArray[np.float64]] = [
            np.empty(0, dtype=float)
        ] * interval_count
        for i in range(interval_count):
            left, right = idxs[i], idxs[i + 1]
            selected = values[left:right]

            if right < len(values):
                if selected.size == 0:
                    row_data[i] = values[right : right + 1]
                else:
                    out = np.empty(selected.size + 1, dtype=np.float64)
                    out[:-1] = selected
                    out[-1] = values[right]
                    row_data[i] = out
            else:
                row_data[i] = selected

        row_series = pd.Series(row_data, index=time_edges[1:], dtype=object)
        results.append(row_series)

    return pd.DataFrame(results)


def merge_current_with_prev(
    current_df: pd.DataFrame,
    prev_df: pd.DataFrame,
    key_cols: list[str],
) -> pd.DataFrame:
    if current_df.empty:
        return prev_df

    valid_keys = [
        k for k in key_cols if k in current_df.columns and k in prev_df.columns
    ]
    if len(valid_keys) < 1:
        # Если нет общих ключей, возвращаем prev_df без изменений
        return prev_df

    cols = valid_keys + ["values", "timestamps"]

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


def safe_median(arr: np.ndarray):
    """
    Медиана с проверкой на пустоту массива
    """
    # Пустой массив
    if arr.size == 0:
        return np.nan
    res = np.nanmedian(arr)
    return res


def mode_fft_via_hist(
    arr: np.ndarray, grid_size: int = 50, bandwidth_arg: float | None = None
) -> float:
    """
    Приближённая мода через FFT-свертку гистограммы.

    Параметры:
    - arr: одномерный numpy-массив.
    - grid_size: число бинов в гистограмме.
    - bandwidth: ширина гауссовского ядра. Если None, используется правило Сильвермана.

    Возвращает:
    - x_mode: центр бина, в котором оценённая плотность максимальна.
    """
    # Пустой массив
    if arr.size == 0:
        return np.nan

    # Все значения одинаковые
    unique_vals = np.unique(arr)
    if unique_vals.size < 2:
        return unique_vals[0]

    # 1) Строим гистограмму (плотность)
    counts, bin_edges = np.histogram(arr, bins=grid_size, density=True)
    dx = bin_edges[1] - bin_edges[0]
    n = arr.size
    sigma = arr.std(ddof=1)

    # 2) Вычисляем bandwidth по правилу Сильвермана, если не задано
    if bandwidth_arg is None:
        bandwidth = 1.06 * sigma * n ** (-1 / 5)
    else:
        bandwidth = bandwidth_arg

    # 3) Формируем гауссовское ядро, охватывающее ±4σ
    L = max(int(np.ceil(8 * bandwidth / dx)), 1)
    kernel_x = np.linspace(-4 * bandwidth, 4 * bandwidth, L)
    kernel = np.exp(-0.5 * (kernel_x / bandwidth) ** 2)
    kernel /= kernel.sum()

    # 4) Быстрая свёртка
    density = fftconvolve(counts, kernel, mode="same")

    # 5) Индекс максимума и возвращаем центр бина
    idx = np.argmax(density)
    x_mode = 0.5 * (bin_edges[idx] + bin_edges[idx + 1])
    return x_mode
