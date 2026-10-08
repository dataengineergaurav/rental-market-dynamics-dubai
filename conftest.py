"""Make the repository root importable for the test suite.

`lib/` is a plain package at the repo root, not an installed distribution, so tests import it via
`sys.path`. Relying on one test module to insert the root makes collection order significant
(alphabetically-earlier test files fail to import `lib`). Doing it once here removes that ordering
dependency for every test file.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
