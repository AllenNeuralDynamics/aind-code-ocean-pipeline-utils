"""Parsers for Code Ocean app-panel parameter values.

CO's app panel passes every parameter as a string when
``named_parameters: true``, so bool-flag CLIs (argparse ``store_true``,
tyro's ``--flag``/``--no-flag``) don't round-trip: the panel always
emits ``--flag <string>``. Capsules end up reimplementing a
permissive truthy parser. The one here centralizes the accepted set
so consumers don't independently pick slightly different semantics.
"""

from __future__ import annotations

__all__ = ["parse_truthy"]

_TRUTHY_WORDS: frozenset[str] = frozenset({"true", "yes", "y", "t"})


def parse_truthy(value: str) -> bool:
    """Parse a permissive truthy string from an app-panel named parameter.

    Returns True for:

    - any case variant of ``"true"`` / ``"yes"`` / ``"y"`` / ``"t"``
    - any numeric string (int or float) whose value is non-zero:
      ``"1"``, ``"42"``, ``"-1"``, ``"3.14"``, ``"1.5e2"`` all parse as True

    Returns False for everything else, including the empty string,
    ``"0"``, ``"0.0"``, ``"false"``, ``"no"``, and unrecognized words.

    The numeric handling exists because CO panels sometimes pass
    integers through as strings ("0"/"1" being a common idiom), and
    ``parse_truthy("0")`` returning True would be a subtle footgun.

    Parameters
    ----------
    value : str
        The raw parameter value from the app panel.

    Returns
    -------
    bool
    """
    stripped = value.strip()
    if not stripped:
        return False
    if stripped.lower() in _TRUTHY_WORDS:
        return True
    try:
        return bool(float(stripped))
    except ValueError:
        return False
