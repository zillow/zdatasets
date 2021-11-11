import keyword


def _is_upper_pascal_case(name: str) -> bool:
    is_valid: bool = (
        name.isidentifier()
        and name[0].isupper()
        and name[0].isalpha()
        and not keyword.iskeyword(name)
        and name.isalnum()
        and name.isascii()
        and name != name.lower()
        and name != name.upper()
    )
    return is_valid


def _pascal_to_snake_case(name: str) -> str:
    assert _is_upper_pascal_case(name)
    snake = [f"_{c.lower()}" if c.isupper() else c for c in name]
    return "".join(snake).lstrip("_")
