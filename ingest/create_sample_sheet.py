#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os.path

parser = argparse.ArgumentParser(description='Generate a sample sheet')
parser.add_argument('--dir', help='Root directory to look for fastq files', required=True)
# parser.add_argument('--recursive', help='Whether to search recursively', action='store_true')
parser.add_argument('--index', help='Whether to include index files', action='store_true')
parser.add_argument('--output', help='Output file name')
parser.add_argument('--replace', help='Optionally replace output paths (--replace=/foo/bar/:gs://adsfjkl231123/')
parser.add_argument('--verbose', action='store_true',
                    help='Print detailed information')
args = parser.parse_args()
output = args.output
if output is None:
    output = os.path.basename(args.dir) + '.txt'
writer = open(output, 'w')
all_names = set()
suffixes = ['_R1', '_R2']
keys = ['R1', 'R2']
if args.index:
    suffixes.append('_I1')
    keys.append('I1')
replace = None
find = None
if args.replace is not None:
    index = args.replace.find(':')
    find = args.replace[0:index]
    replace = args.replace[index + 1:]
counter = 0
for root, dirs, files in os.walk(args.dir):
    name_to_files = {}
    for name in files:
        if name.lower().startswith('undetermined'):
            print('Skipped ' + os.path.join(root, name))
            continue
        ext_index = name.rfind('.fastq.gz')
        if ext_index != -1:
            path = os.path.join(root, name)
            if find is not None:
                path = path.replace(find, replace)
            basename = name[0:ext_index]
            key = None
            for suffix_index in range(len(suffixes)):
                index = basename.rfind(suffixes[suffix_index])
                if index != -1:
                    basename = basename[0:index]
                    key = keys[suffix_index]
                    break

            if key is None:
                # if args.verbose:
                #     print('Unknown type for ' + path)
                continue

            val = name_to_files.get(basename)

            if val is None:
                val = {'R1': None, 'R2': None}
                if args.index:
                    val['I1'] = None
                name_to_files[basename] = val
            val[key] = path
    for key, val in name_to_files.items():
        if key in all_names:
            raise ValueError('Duplicate name ' + key)
        counter += 1
        all_names.add(key)
        r1 = val.get('R1')
        if r1 is None:
            raise ValueError('R1 not found for ' + key)
        r2 = val.get('R2')
        if r2 is None:
            raise ValueError('R2 not found for ' + key)
        i1 = ''
        if args.index:
            i1 = val.get('I1')
            if i1 is None:
                raise ValueError('I1 not found for ' + key)
            i1 = '\t' + i1
        writer.write(key + '\t' + r1 + '\t' + r2 + i1 + '\n')
if args.verbose:
    print(str(counter) + ' samples found in ' + args.dir)
writer.close()
