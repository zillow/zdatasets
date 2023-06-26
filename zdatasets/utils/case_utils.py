import keyword


def is_upper_pascal_case(name: str) -> bool:
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


def is_snake_case(name: str) -> bool:
    is_valid: bool = (
        name.isidentifier()
        and name[0].islower()
        and name[0].isalpha()
        and not keyword.iskeyword(name)
        and name.replace("_", "").isalnum()
        and name.isascii()
        and name == name.lower()
        and name[0] != "_"
        and name[-1] != "_"
    )
    return is_valid


def pascal_to_snake_case(name: str) -> str:
    assert is_upper_pascal_case(name)
    snake = [f"_{c.lower()}" if c.isupper() or not c.isalpha() else c for c in name]
    return "".join(snake).lstrip("_")


def snake_case_to_pascal(name: str) -> str:
    upper_pascal = name.replace("_", " ").title().replace(" ", "")
    return upper_pascal
