import re

import polars as pl

def to_snake_case(text: str) -> str:
    """
    Convert a string to valid snake_case following Python naming conventions.

    The output will pass validation by is_snake_case:
    - Starts with lowercase letter or underscore
    - Contains only lowercase letters, numbers, or single underscores
    - No consecutive underscores
    - No trailing underscore

    Args:
        text (str): Input string that could be in any format (camelCase, PascalCase, etc.)

    Returns:
        str: The string converted to valid snake_case

    Examples:
        >>> to_snake_case("helloWorld")
        'hello_world'
        >>> to_snake_case("HelloWorld")
        'hello_world'
        >>> to_snake_case("hello-world")
        'hello_world'
        >>> to_snake_case("hello__world")
        'hello_world'
        >>> to_snake_case("_hello_world_")
        '_hello_world'
    """
    if not text:
        return text

    # Replace hyphens and spaces with underscores
    text = re.sub(r"[-\s]+", "_", text)

    # Insert underscore between camelCase
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)

    # Convert to lowercase and remove any non-alphanumeric characters (except underscores)
    text = re.sub(r"[^a-zA-Z0-9_]", "", text)

    # Convert to lowercase
    text = text.lower()

    # Replace multiple consecutive underscores with single underscore
    text = re.sub(r"_+", "_", text)

    # Remove trailing underscore if present
    text = re.sub(r"_$", "", text)

    # If string doesn't start with a letter or underscore, prepend underscore
    if text and not re.match(r"^[a-z_]", text):
        text = "_" + text

    if not is_snake_case(text):
        raise NameError(f"{text} is not in snake_case format")

    return text


def is_snake_case(text: str) -> bool:
    """
    Check if a string follows Python snake_case naming convention using regex.

    Pattern explanation:
    ^           # Start of string
    [a-z_]      # Start with lowercase letter or underscore
    [a-z0-9_]*  # Followed by any number of lowercase letters, numbers, or underscores
    (?<!_)      # Negative lookbehind to prevent trailing underscore
    $           # End of string

    Additional negative pattern:
    .*__.*      # Matches any string containing consecutive underscores

    Args:
        text (str): The string to check

    Returns:
        bool: True if the string follows snake_case, False otherwise

    Examples:
        >>> is_snake_case("hello_world")
        True
        >>> is_snake_case("HelloWorld")
        False
        >>> is_snake_case("hello__world")
        False
    """
    # Pattern for valid snake_case
    snake_pattern = r"^[a-z_][a-z0-9_]*(?<!_)$"

    # Pattern for consecutive underscores
    double_underscore_pattern = r".*__.*"

    return bool(re.match(snake_pattern, text)) and not bool(
        re.match(double_underscore_pattern, text)
    )


def convert_columns_to_snake_case(df: pl.DataFrame) -> pl.DataFrame:
    """Return a new DataFrame with all column names converted to snake_case.

    Args:
        df (pl.DataFrame): The DataFrame to convert

    Returns:
        pl.DataFrame: A new DataFrame with all column names converted to snake_case
    """
    return df.rename({col: to_snake_case(col) for col in df.columns})
