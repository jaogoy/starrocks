from typing import Any, Dict, Optional, Union, Mapping, Iterator


class CaseInsensitiveDict(dict):
    """A dictionary that enables case insensitive searching while preserving case sensitivity when keys are set."""

    def __init__(self, data: Optional[Union[Dict[str, Any], Mapping[str, Any]]] = None) -> None:
        super().__init__()
        self._lower_keys: Dict[str, str] = {}  # Maps lowercase keys to actual keys
        if data:
            self.update(data)

    def __setitem__(self, key: str, value: Any) -> None:
        lower_key: str = key.lower()
        if lower_key in self._lower_keys:
            # Remove old entry if exists
            old_key: str = self._lower_keys[lower_key]
            super().__delitem__(old_key)
        self._lower_keys[lower_key] = key
        super().__setitem__(key, value)

    def __getitem__(self, key: str) -> Any:
        lower_key: str = key.lower()
        if lower_key in self._lower_keys:
            actual_key: str = self._lower_keys[lower_key]
            return super().__getitem__(actual_key)
        raise KeyError(key)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key: str) -> bool:
        return key.lower() in self._lower_keys

    def update(self, other: Union['CaseInsensitiveDict', Mapping[str, Any], Iterator[tuple[str, Any]]]) -> None:
        if hasattr(other, 'items'):
            for key, value in other.items():
                self[key] = value
        else:
            for key, value in other:
                self[key] = value
