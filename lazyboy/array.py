# -*- coding: utf-8 -*-
#
# © 2010 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

"""Lazyboy arrays."""

from functools import partial
from itertools import islice

from lazyboy.base import CassandraBase
from iterators import slice_iterator


class Array(CassandraBase):

    """An Array abstraction on a Row."""

    def __init__(self, key, **kwargs):
        """Initialize the Array."""
        CassandraBase.__init__(self)
        self.key = key
        self.kwargs = kwargs
        self.columns = []
        self._materialize = partial(slice_iterator, self.key, self.consistency,
                                    **self.kwargs)

    def __getitem__(self, key):
        if isinstance(key, slice):
            return list(islice(self._materialize(count=key.stop),
                          key.start, key.stop, key.step))
        return list(self._materialize(count=key+1))[key]

    def __iter__(self):
        """Iterate over the row."""
        return self._materialize()

    def __reversed__(self):
        """Iterate in reverse."""
        return self._materialize(reverse=True)

    def __len__(self):
        """Return the length of this row."""
        return self._get_cas().get_count(self.key.keyspace, self.key.key,
                                         self.key, self.consistency)

    def __repr__(self):
        """Return representation."""
        return "Array %r" % self.key
