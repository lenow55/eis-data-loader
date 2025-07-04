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
    take_next: bool = True,
) -> pd.DataFrame:
    """
    Развёртка + группировка в списки, с опциональным захватом первой
    точки за концом каждого интервала (take_next=True).
    """

    # 1) Границы интервалов (closed='left'): [edges[i], edges[i+1])
    start_time = start_time.replace(tzinfo=UTC)
    end_time = start_time + timedelta(hours=1)
    edges = pd.date_range(start=start_time, end=end_time + period, freq=period, tz=UTC)

    # 2) Explode — из wide в long
    df = (
        merged_dataset.reset_index()
        .rename(columns={"index": "row_id"})
        .explode(["timestamps", "values"])
    )

    # 3) Преобразуем раз и навсегда
    df["timestamps"] = pd.to_datetime(df["timestamps"], utc=True)

    # 4) Нумерация бинов 0..len(edges)-2, всё вне — NaN
    df["bin"] = pd.cut(df["timestamps"], bins=edges, right=False, labels=False)  # pyright: ignore[reportArgumentType, reportCallIssue]

    # Оставим только валидные
    orig = df.dropna(subset=["bin"]).copy()
    orig["bin"] = orig["bin"].astype(int)

    if take_next:
        # 5a) Для каждой (row_id, bin) найдём первую точку
        first = (
            orig.sort_values(["row_id", "timestamps"])
            .groupby(["row_id", "bin"], sort=False)["values"]
            .first()
            .reset_index()
        )

        # 5b) Берём только группы bin>0 и «сдвигаем» их на один влево
        extra = first[first["bin"] > 0].copy()
        extra["bin"] = extra["bin"] - 1

        # 5c) Объединяем оригинальные и «экстра»-строки
        combined = pd.concat(
            [orig[["row_id", "bin", "values"]], extra[["row_id", "bin", "values"]]],
            ignore_index=True,
        )
    else:
        combined = orig[["row_id", "bin", "values"]]

    # 6) Снова группируем в списки и разворачиваем в wide
    grouped = (
        combined.groupby(["row_id", "bin"], sort=False)["values"]
        .agg(lambda x: np.array(list(x)))
        .unstack(fill_value=np.array([]))  # pyright: ignore[reportArgumentType]
    )

    # 7) Восстанавливаем оригинальный индекс строк и метки столбцов
    new_cols = edges[1 : len(grouped.columns) + 1]
    grouped.index = merged_dataset.index
    grouped.columns = new_cols

    # 8) Проверка на пустоту колонок
    if len(new_cols) == 0:
        # Вернуть пустой DataFrame с тем же индексом, что и у merged_dataset
        return pd.DataFrame(index=merged_dataset.index)

    # 9) берём все колонки кроме последней
    if new_cols[-1] > end_time:
        grouped = grouped.iloc[:, 0:-1]

    return grouped


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
    df = pd.merge(prev_df, current_df, on=valid_keys, suffixes=("_1", "_2"), how="left")

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
