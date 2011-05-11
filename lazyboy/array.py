# -*- coding: utf-8 -*-
#
# © 2010 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

"""Lazyboy arrays."""

from functools import partial
from itertools import islice
from copy import copy

from lazyboy.base import CassandraBase
import column_crud as crud
from iterators import slice_iterator
from util import timestamp


class Array(CassandraBase):

    """An Array abstraction on a Row."""

    def __init__(self, key, **kwargs):
        """Initialize the Array."""
        CassandraBase.__init__(self)
        self.key = key
        self.kwargs = kwargs
        self.columns = []
        self._slice_iterator = slice_iterator

    def _materialize(self, **kwargs):
        """Return an iterator over the array."""
        args = copy(self.kwargs)
        args.update(kwargs)
        return self._slice_iterator(self.key, self.consistency, **args)

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
        self.kwargs.update(reverse=not self.kwargs.get('reverse', False))
        return self.__iter__()

    def __len__(self):
        """Return the length of this row."""
        return self._get_cas().get_count(self.key.key,
                                         self.key,
                                         SlicePredicate(slice_range=SliceRange("", "", False, 2147483647)),
                                         self.consistency)

    def __repr__(self):
        """Return representation."""
        return "Array %r" % self.key

    def destroy(self):
        """Destroy this array."""
        self._get_cas().remove(
            self.key.keyspace, self.key.key,
            ColumnPath(self.key.column_family), timestamp(), self.consistency)

    def append(self, value):
        """Append a record to this array."""
        crud.set(self.key, value, "", timestamp())

    def extend(self, iterable):
        """Append multiple records to this array."""
        now = timestamp()
        columns = [Column(value, "", now) for value in iterable]
        mutations = [Mutation(column_or_supercolumn=ColumnOrSuperColumn(column=col))
                     for col in columns]

        mutation_map = {key.key:
                        {key.column_family: mutations}}
                                                            
        self._get_cas().batch_mutate(mutation_map, consistency)
