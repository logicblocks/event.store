"""The docstrings package.

It has a second paragraph.
"""


def transfer(amount: int, currency: str = "GBP") -> bool:
    """Transfers an amount.

    The transfer is immediate.

    Args:
        amount: The amount to transfer.
        currency (str): The currency, which may be
            spread over two lines.

    Returns:
        Whether the transfer succeeded.

    Raises:
        ValueError: If the amount is negative.

    Note:
        Transfers are final.

    Examples:
        >>> transfer(10)
        True
    """
    return amount >= 0


class Account:
    """A bank account.

    Attributes:
        balance (int): The current balance.
        owner: The owner.
    """

    balance: int
    owner: str


def broken(value: int) -> None:
    """Has a broken section.

    Args:
        value without a colon
    """


__all__ = ["Account", "broken", "transfer"]
