import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Type


HANDLER_REGISTRY = dict()


def register_handler(format_name: str):
    def decorator(handler_cls):
        HANDLER_REGISTRY[format_name] = handler_cls
        return handler_cls
    return decorator


@dataclass
class AbstractHandler(ABC):

    path: Path


class AbstractReader(AbstractHandler, ABC):

    @abstractmethod
    def read_file(self) -> dict | list:
        ...


class AbstractWriter(AbstractHandler, ABC):

    @abstractmethod
    def write_file(self, data: dict | list):
        ...


@register_handler('json')
class JsonHandler(AbstractReader, AbstractWriter):

    def read_file(self) -> dict | list:
        with open(self.path, 'r') as file:
            return json.load(file)

    def write_file(self, data: dict | list):
        with open(self.path, 'w') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)


def get_handler_class(format: str) -> Type[AbstractWriter]:
    if format not in HANDLER_REGISTRY:
        raise ValueError(f'`{format}` isn`t supported')
    return HANDLER_REGISTRY[format]


def get_handler(path: Path, *args, **kwargs) -> AbstractWriter:
    format = path.name.split('.')[-1]
    return get_handler_class(format)(path, *args, **kwargs)
