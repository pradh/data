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

"""Script to download + clean NPI registry dataset."""

from sys import path
# For util library.
path.insert(1, '../../../')

from absl import app
from absl import flags
import csv
import io
import re
import ssl
import string
import urllib.request
import util.alpha2_to_dcid as alpha2_to_dcid
import util.name_to_alpha2 as name_to_alpha2
import zipfile


FLAGS = flags.FLAGS
flags.DEFINE_string('csv_path', '',
                    'Filesystem path to the raw CSV file. ' +
                    'Mutually exclusive with --zip_path')
flags.DEFINE_string('zip_path', '',
                    'http or filesystem path to the ZIP file. ' +
                    'Mutually exclusive with --csv_path.')
OUTPUT_CSV = 'npi_cleaned.csv'
OUTPUT_TMCF = 'npi.tmcf'

OUTPUT_COLUMNS = ['dcid', 'providerType', 'name', 'gender',
                  'providerCredential',
                  'primaryPracticeAddress', 'primaryPracticeLocation',
                  'primaryTaxonomy', 'enumerationDate']


def _strfy(s):
    return '"' + s + '"'


def _emit_tmcf(out_tmcf):
    lines = ['Node: E:npi_csv->E1', 'typeOf: dcs:HealthCareProvider']
    for c in OUTPUT_COLUMNS:
        lines.append(c + ': C:npi_csv->' + c)
    out_tmcf.write('\n'.join(lines) + '\n')


def _process_person_details(in_rec, out_rec):
    # Individual type.
    out_rec['providerType'] = 'schema:Person'

    # Name
    name_parts = []
    for f in ['Provider First Name', 'Provider Last Name (Legal Name)']:
        if in_rec[f]:
            name_parts.append(string.capwords(in_rec[f]))
    if name_parts:
        out_rec['name'] = _strfy(' '.join(name_parts))

    # Credential
    cred = in_rec['Provider Credential Text']
    if cred:
        out_rec['providerCredential'] = _strfy(cred)

    # Gender
    gender = in_rec['Provider Gender Code']
    if gender == 'M':
        out_rec['gender'] = 'schema:Male'
    else:
        out_rec['gender'] = 'schema:Female'


def _process_org_details(in_rec, out_rec):
    # Organization type.
    out_rec['providerType'] = 'schema:Organization'

    # Name
    name = in_rec['Provider Organization Name (Legal Business Name)']
    if name:
        out_rec['name'] = _strfy(string.capwords(name))


def _process_address(in_rec, out_rec):
    pc = in_rec['Provider Business Practice Location Address Postal Code']
    st = in_rec['Provider Business Practice Location Address State Name']
    cc = in_rec['Provider Business Practice Location Address Country Code (If outside U.S.)']
    cc = cc.upper()

    # Compute 'primaryPracticeLocation'
    locations = []
    within_us = True
    if cc and cc != 'US' and cc in alpha2_to_dcid.COUNTRY_MAP:
        # Outside US.
        within_us = False
        d = alpha2_to_dcid.COUNTRY_MAP[cc]
        locations.append('dcid:' + d)
    else:
        # Within US.
        st2 = st.upper()
        if st2 in alpha2_to_dcid.USSTATE_MAP:
            d = alpha2_to_dcid.USSTATE_MAP[st2]
            locations.append('dcid:' + d)
        else:
            st2 = string.capwords(st2).replace(' ', '')
            if st2 in name_to_alpha2.USSTATE_MAP:
                st_code = name_to_alpha2.USSTATE_MAP[st2]
                if st_code in alpha2_to_dcid.USSTATE_MAP:
                    d = alpha2_to_dcid.USSTATE_MAP[st_code]
                    locations.append('dcid:' + d)
        if len(pc) >= 5:
            locations.append('dcid:zip/' + pc[:5])
    if locations:
        out_rec['primaryPracticeLocation'] = ', '.join(locations)

    # Compute 'primaryPracticeAddress'
    address_parts = []
    for f in ['Provider First Line Business Practice Location Address',
              'Provider Second Line Business Practice Location Address',
              'Provider Business Practice Location Address City Name']:
        if in_rec[f]:
            address_parts.append(string.capwords(in_rec[f]))
    if within_us:
        if st:
            st_str = st.upper() if len(st) == 2 else string.capwords(st)
            if pc:
                address_parts.append(st_str + ' ' + pc)
            else:
                address_parts.append(st_str)
        address_parts.append('USA')
    else:
        address_parts.append(cc)
    if address_parts:
        out_rec['primaryPracticeAddress'] = _strfy(', '.join(address_parts))


def _process_taxonomy(in_rec, out_rec):
    for i in range(1, 16):
        pri_key = 'Healthcare Provider Primary Taxonomy Switch_' + str(i)
        code_key = 'Healthcare Provider Taxonomy Code_' + str(i)

        assert pri_key in in_rec, 'Key ' + pri_key + ' not found!'
        assert code_key in in_rec, 'Key ' + pri_key + ' not found!'
        if in_rec[pri_key] == 'Y':
            out_rec['primaryTaxonomy'] = 'dcid:nuccCode/' + in_rec[code_key]
            break


def preprocess(in_csv, out_csv, out_tmcf):
    _emit_tmcf(out_tmcf)

    csv_writer = csv.DictWriter(out_csv, fieldnames=OUTPUT_COLUMNS)
    csv_writer.writeheader()
    num_rows = 0
    for in_rec in csv.DictReader(in_csv):
        out_rec = {}

        if in_rec['NPI Deactivation Reason Code']:
            # Deactivated NPI
            continue

        npi = in_rec['NPI']
        if not npi:
            continue
        out_rec['dcid'] = _strfy('npi/' + npi)

        pt = in_rec['Entity Type Code']
        if pt == '1':
            _process_person_details(in_rec, out_rec)
        elif pt == '2':
            _process_org_details(in_rec, out_rec)
        else:
            continue

        ed = in_rec['Provider Enumeration Date']
        if ed:
            m = re.match('^(\d\d)/(\d\d)/(\d\d\d\d)', ed)
            if len(m.groups()) == 3:
                out_rec['enumerationDate'] = _strfy(m.groups()[2] + '-' +
                        m.groups()[0] + '-' + m.groups()[1])

        _process_address(in_rec, out_rec)
        _process_taxonomy(in_rec, out_rec)
        
        csv_writer.writerow(out_rec)
        num_rows += 1
        if num_rows % 100000 == 0:
            print('Processed ' + str(num_rows) + ' rows')


def main(_):
    assert FLAGS.csv_path or FLAGS.zip_path, 'Neither of the flags is set.'
    assert not FLAGS.csv_path or not FLAGS.zip_path, 'Only one should be set.'

    out_csv = open(OUTPUT_CSV, 'w', newline='')
    out_tmcf = open(OUTPUT_TMCF, 'w', newline='')

    if FLAGS.zip_path:
        if FLAGS.zip_path.startswith('http'):
            zip_file = 'npi_registry.zip'
            # Workaround for CERTIFICATE_VERIFY_FAILED failure in urlretrieve().
            ssl._create_default_https_context = ssl._create_unverified_context
            urllib.request.urlretrieve(FLAGS.zip_path, zip_file)
        else:
            zip_file = FLAGS.zip_path
        with zipfile.ZipFile(zip_file) as npi_zip:
            csv_name = None
            for n in npi_zip.namelist():
                if (n.startswith('npidata_') and not
                        n.endswith('_FileHeader.csv')):
                    csv_name = n
                    break
            assert csv_name, 'File not found inside ZIP file.'
            print('Opening ' + csv_name)
            with npi_zip.open(csv_name, 'r') as in_csv:
                preprocess(io.TextIOWrapper(in_csv), out_csv, out_tmcf)
    else:
        with open(FLAGS.csv_path) as in_csv:
            preprocess(in_csv, out_csv, out_tmcf)


if __name__ == '__main__':
    app.run(main)
