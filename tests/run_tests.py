import os
import sys
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

# Databricks runs this file with exec(), so __file__ does not exist here.
# Check a few likely locations for the actual tests folder instead.
candidates = [os.getcwd(), os.path.join(os.getcwd(), "tests")]
if sys.argv and sys.argv[0]:
    candidates.append(os.path.dirname(os.path.abspath(sys.argv[0])))

start_dir = next(
    (c for c in candidates if os.path.isfile(os.path.join(c, "test_bronze.py"))),
    None,
)
if start_dir is None:
    raise RuntimeError(f"Could not find test_bronze.py. Tried: {candidates}")

loader = unittest.TestLoader()
suite = loader.discover(
    start_dir=start_dir,
    pattern="test_*.py"
)

# Execute all discovered tests
runner = unittest.TextTestRunner(verbosity=2)

result = runner.run(suite)

if not result.wasSuccessful():
    raise Exception("Unit testing failed!")

print("All unit tests passed!")