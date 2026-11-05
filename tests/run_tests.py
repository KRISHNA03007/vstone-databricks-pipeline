import os
import argparse
import unittest

parser = argparse.ArgumentParser()

parser.add_argument("--source-path", required=True)
parser.add_argument("--table-name", required=True)
parser.add_argument("--format", default="csv")
parser.add_argument("--row-tag", default=None)

args = parser.parse_args()

os.environ["SOURCE_PATH"] = args.source_path
os.environ["TABLE_NAME"] = args.table_name
os.environ["FILE_FORMAT"] = args.format
if args.row_tag:
    os.environ["ROW_TAG"] = args.row_tag

# Discover all test files
loader = unittest.TestLoader()

suite = loader.discover(
    start_dir="tests",
    pattern="test_*.py"
)

# Execute all discovered tests
runner = unittest.TextTestRunner(verbosity=2)

result = runner.run(suite)

if not result.wasSuccessful():
    raise Exception("Unit testing failed!")

print("All unit tests passed!")