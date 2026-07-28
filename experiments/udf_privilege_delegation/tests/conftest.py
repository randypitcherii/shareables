import sys
from pathlib import Path

# Scripts import each other by bare module name (they run as __main__ from make),
# so the scripts/ dir has to be importable the same way here.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
