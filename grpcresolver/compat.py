
"""Compat methods module."""

import six


__all__ = ['b2str']


def b2str(i_b):
    if issubclass(six.binary_type, six.string_types):
        return i_b

    return i_b.decode()
