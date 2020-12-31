# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for preprocess."""

# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

import io
import os
import pathlib
import unittest

import preprocess

module_dir_ = os.path.dirname(__file__)


class PreprocessTest(unittest.TestCase):

    def test_main(self):
        exp_csv = pathlib.Path(os.path.join(module_dir_,
            'test_data/npi_cleaned.csv')).read_text()
        exp_tmcf = pathlib.Path(os.path.join(module_dir_,
            'test_data/npi.tmcf')).read_text()
        act_csv = io.StringIO(newline='')
        act_tmcf = io.StringIO(newline='')

        with open(os.path.join(module_dir_,
                               'test_data/npi_original.csv')) as in_csv:
            preprocess.preprocess(in_csv, act_csv, act_tmcf)

        self.maxDiff = None
        self.assertEqual(exp_tmcf, act_tmcf.getvalue())
        self.assertEqual(exp_csv, act_csv.getvalue().replace('\r\n', '\n'))


if __name__ == '__main__':
    unittest.main()
