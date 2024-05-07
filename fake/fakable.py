"""
Interface for faking data in dataclasses
"""
import json
from dataclasses import asdict, dataclass, fields
from importlib import import_module
from typing import Any, Union

import yaml

from ippon.utils.encoders import JsonDatetimeEncoder


def get_model_class(module_path: str, model_name: str) -> Union[object, Any]:
    """
    Returns the model class for a given model name.
    """

    module = import_module(f"ippon.data.fake.models.{module_path}")
    cls = getattr(module, model_name)
    # from ippon.utils.fakable import FakableModel
    # if not isinstance(cls, FakableModel):
    #     raise TypeError(f"{cls} exists in {module_path} but is not a FakableModel")
    return cls


@dataclass
class FakableModel:
    """
    Abstract dataclass to move ``dataclasses.asdict`` as a function on the class as well
        as some other helper funcs
    """

    def as_dict(self) -> dict:
        """Returns a dictionary representation of this object"""
        return asdict(self)

    def as_json(self, **kwargs) -> str:
        """
        Returns a JSON string representation of this object.
        Keyword args are forwarded to the ``json.dumps`` method- so
            ``self.as_json(indent=2)`` sets indent, etc.
        """
        my_dict = self.as_dict()
        return json.dumps(my_dict, cls=JsonDatetimeEncoder, **kwargs)

    def as_yaml(self, **kwargs) -> str:
        """
        Returns a YAML string representation of this object.
        Keyword args are forwarded to the ``yaml.dump`` method- so
            ``self.as_yaml(indent=2)`` sets indent, etc.
        """
        my_dict = self.as_dict()
        return yaml.dump(my_dict, **kwargs)

    @classmethod
    def spoof(cls) -> "FakableModel":
        """Spoof an instance of this class"""
        return cls(**{field: cls.spoof_field(field) for field in cls.field_list()})

    @classmethod
    def spoof_field(cls, field_name: str) -> Any:
        raise NotImplementedError("`spoof_field` must be implemented by subclasses")

    @classmethod
    def spoof_json(cls, **kwargs) -> str:
        """Spoofs as JSON"""
        my_dict = cls.spoof().as_dict()
        return json.dumps(my_dict, cls=JsonDatetimeEncoder, **kwargs)

    @classmethod
    def spoof_yaml(cls, **kwargs) -> str:
        """Spoofs as YAML"""
        my_dict = cls.spoof().as_dict()
        return yaml.dump(my_dict, **kwargs)

    @classmethod
    def field_list(cls):
        """Returns a list of data fields in the class"""
        return list(field.name for field in fields(cls))

    def __str__(self) -> str:
        return self.as_json()

    def __repr__(self) -> str:
        return self.as_json()

    def __eq__(self, other) -> bool:
        return self.as_dict() == other.as_dict()

    def __ne__(self, other) -> bool:
        return self.as_dict() != other.as_dict()

    def __hash__(self) -> int:
        return hash(self.as_dict())

    def __lt__(self, other) -> bool:
        return self.as_dict() < other.as_dict()

    def __le__(self, other) -> bool:
        return self.as_dict() <= other.as_dict()

    def __gt__(self, other) -> bool:
        return self.as_dict() > other.as_dict()

    def __ge__(self, other) -> bool:
        return self.as_dict() >= other.as_dict()

    def __add__(self, other) -> dict:
        return self.as_dict() | other.as_dict()

    def __sub__(self, other) -> dict:
        return self.as_dict() ^ other.as_dict()


class ForeignKey:
    """Fakable type that references another field on a class"""

    def __init__(self, target_table: Any, target_field: str) -> None:
        self.target_table = target_table
        self.target_field = target_field

    def spoof(self) -> Any:
        """Select a random value from the target table"""
        Model = get_model_class(*self.target_table.split("."))

        return Model.spoof_field(self.target_field)

    def __repr__(self) -> str:
        return f"ForeignKey({self.target_table}, {self.target_field})"

    def __str__(self) -> str:
        return self.__repr__()
