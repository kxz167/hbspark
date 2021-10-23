import hbspark as hs
import sys

hs.connect(sys.argv[1])
print(hs.tables())