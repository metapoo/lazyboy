# -*- coding: utf-8 -*-
#
# © 2010 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

"""Unit tests for lazyboy.array."""


import unittest
import uuid
import random

import lazyboy as lzb
import lazyboy.util as util
from lazyboy.array import Array
from lazyboy.iterators import columns


class ArrayTest(unittest.TestCase):

    """Test suite for lazyboy.array."""

    def setUp(self):
        self.row_data = list(lzb.pack(columns(
                    ((str(uuid.uuid4()), random.randint(0, 10000))
                     for x in range(100)), util.timestamp())))

        self.array = Array(lzb.Key("Keyspace", "Colfam", "Rowkey"))
        self.array._slice_iterator = lambda *args, **kwargs: \
            (kwargs.get('reverse') and reversed(self.row_data)
             or iter(self.row_data))

        fake_cas = type('FakeCas', (),
                        {'get_count': lambda *args: len(self.row_data)})
        self.array._get_cas = fake_cas

    def test_length(self):
        self.assert_(len(self.row_data) == len(self.array))

    def test_indexes(self):
        for key in range(len(self.array)):
            self.assert_(self.row_data[key] == self.array[key])

    def test_slices(self):
        for key in range(len(self.array)):
            self.assert_(self.row_data[key:2] == self.array[key:2])
            self.assert_(self.row_data[:key] == self.array[:key])
            self.assert_(self.row_data[2:key] == self.array[2:key])
        self.assert_(self.row_data[::2] == self.array[::2])

    def test_reversed(self):
        self.assert_(list(reversed(self.array)) ==
                     list(reversed(self.row_data)))


if __name__ == '__main__':
    unittest.main()
