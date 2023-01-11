"""
Helper functions for working with tags.
"""
import re
from typing import List


def tag_pims_to_adls(tag_list: List[str]) -> List[str]:
    """Convert tag names from PIMs to the format used on the DataLake.

    Parameters
    ----------
    tag_list : List[str]
        List of tags to convert.

    Returns
    -------
    List[str]
        List of converted tags.
    """
    return [
        re.sub(
            "[^a-zA-Z0-9_]",
            "_",
            tag.replace("_-", "_ME").replace("_+", "_MA").replace("-ANLG", ""),
        ).upper()
        + "__3600"
        for tag in tag_list
    ]
