import dataclasses
import json
from dataclasses import dataclass
from enum import Enum
from json import dumps


_asdict_inner_actual = dataclasses._asdict_inner
def _asdict_inner(obj, dict_factory):

    # if override exists, intercept and return that instead
    if dataclasses._is_dataclass_instance(obj) and getattr(obj, '__dict_factory_override__', None):
        user_dict = obj.__dict_factory_override__()
    
        for k, v in user_dict.items(): # in case of further nesting
            if isinstance(v, list) and len(v) > 0 and dataclasses._is_dataclass_instance(v[0]):
                user_dict[k] = [_asdict_inner(vv, dict_factory) for vv in v]
            if dataclasses._is_dataclass_instance(v):
                user_dict[k] = _asdict_inner(v, dict_factory)
        return user_dict

    # otherwise do original behavior
    return _asdict_inner_actual(obj, dict_factory)
dataclasses._asdict_inner = _asdict_inner
asdict = dataclasses.asdict


class OperatorEnum(Enum):
    EQ = 'EQ'
    CONTAINS = 'CONTAINS'
    GT = 'GT'
    GTE = 'GTE'
    LT = 'LT'
    LTE = 'LTE'
    IN = 'IN'
    STARTS_WITH = 'STARTS_WITH'
    INTERSECTS = 'INTERSECTS'



class LogicalOpEnum(Enum):
    AND = 'AND'
    OR = 'OR'


RESERVED_WORD_LIST = ('from_', '_next')


@dataclass
class BaseDataclass:
    def __dict_factory_override__(self):
        normal_dict = {k: getattr(self, k) for k in self.__dataclass_fields__}
        d = {}
        for k, v in normal_dict.items():
            if k in RESERVED_WORD_LIST:
                k = k[:-1]

            d[k] = v.value if isinstance(v, Enum) else v
        return d

    def asdict(self):
        return asdict(self)

    def json(self):
        """
        get the json formatted string
        """
        d = self.asdict()
        return dumps(self.asdict())

    @classmethod
    def from_dict(cls, dict_: dict):
        for k in list(dict_.keys()):
            if k in RESERVED_WORD_LIST:
                dict_[f'{k}_'] = dict_[k]
                del dict_[k]
        return cls(**dict_)

    def _fix_enums(self, list_of_fields: set[tuple[str, type]]):
        for field_tuple in list_of_fields:
            attr = getattr(self, field_tuple[0])
            if attr is not None:
                setattr(self, field_tuple[0], field_tuple[1](attr))

    def _fix_nested_classes(self, list_of_fields: set[tuple[str, type]]):
        for field_tuple in list_of_fields:
            attr = getattr(self, field_tuple[0])
            if attr is not None and isinstance(attr, dict):
                setattr(self, field_tuple[0], field_tuple[1].from_dict(attr))

    def _fix_nested_list_classes(self, list_of_fields: set[tuple[str, type]]):
        for field_tuple in list_of_fields:
            attr = getattr(self, field_tuple[0])
            if attr is not None and isinstance(attr, list) and len(attr) > 0 and isinstance(attr[0], dict):
                setattr(self, field_tuple[0], [field_tuple[1].from_dict(a) for a in attr])

    def __str__(self):
        return json.dumps(self.asdict(), indent=4, sort_keys=True)


@dataclass()
class Query(BaseDataclass):
    size: int
    filters: dict
    orderByProperty: str | None = None
    fromCursor: str | None = None
    expansions: dict[str, list[str]] | None = None

    def add_expansions(self, expansions: list[str]):
        self.expansions = {"fields" : expansions}