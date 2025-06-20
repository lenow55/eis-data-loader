import time
import random
from threading import Thread
from rich.progress import Progress, TaskID


def worker(progress: Progress, task_id: TaskID, error_bar: TaskID) -> None:
    error_count = 0
    for i in range(1, 101):
        time.sleep(random.uniform(0, 1))
        if random.random() < 0.1:  # Симуляция ошибки с вероятностью 10%
            error_count += 1
        progress.update(task_id, advance=1)
    progress.update(
        error_bar, advance=error_count
    )  # Обновление бара ошибок по количеству ошибок


if __name__ == "__main__":
    progress = Progress()
    progress.start()

    error_bar = progress.add_task(
        "Ошибки", total=5
    )  # Установим максимальное число ошибок для отображения
    tasks = []

    for i in range(5):
        task_id = progress.add_task(f"Process {i + 1}", total=100)
        t = Thread(target=worker, args=(progress, task_id, error_bar))
        t.start()
        tasks.append(t)

    for t in tasks:
        t.join()

    progress.stop()
