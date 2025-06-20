from multiprocessing import Queue
from threading import Lock
from time import sleep

from src.schemas import LoadComplete


class QueueManager:
    def __init__(self, queues: list["Queue[LoadComplete]"]):
        self.queues: list["Queue[LoadComplete]"] = queues
        self.locks: list[Lock] = [Lock() for _ in queues]

    def get_free_queue(self) -> tuple["Queue[LoadComplete]", Lock]:
        while True:
            for q, lock in zip(self.queues, self.locks):
                if lock.acquire(blocking=False):
                    return q, lock
            sleep(10)  # Ждем 10 секунд перед следующей проверкой

    def release_queue(self, lock: Lock) -> None:
        lock.release()
