import json
import logging
import os
import traceback
import pandas as pd

from src.utils.aggregate import unix_ts2datetime

logger = logging.getLogger(__name__)


def read_jsonl(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        result = []
        for line in f:
            line_dict = json.loads(line)
            tmp_metric = line_dict.pop("metric")
            for key, value in tmp_metric.items():
                line_dict.update({key: value})
            result.append(line_dict)
        return result


def preprocess_metric_df(file_path: str | None) -> pd.DataFrame:
    if not isinstance(file_path, str):
        return pd.DataFrame([])

    try:
        metric_content = read_jsonl(file_path)
        dataframe = pd.json_normalize(metric_content, sep="_")
        dataframe.loc[:, "timestamps"] = dataframe.loc[:, "timestamps"].apply(
            unix_ts2datetime
        )
    except Exception as e:
        logger.error(traceback.format_exc())
        return pd.DataFrame([])
    finally:
        # INFO: удаляем файлы
        os.remove(file_path)

    return dataframe
