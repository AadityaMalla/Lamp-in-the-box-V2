# =============================================================
# Author : Aaditya Malla (CSCI 312 - Group 4)
# File   : spark/warmup.py
# Purpose: Run at image build time to pre-download the Delta Lake
#          and hadoop-aws (S3) jars into the image, so containers
#          start quickly and do not need to download at runtime.
# =============================================================
from server import build_spark

build_spark().stop()
print("Delta Lake + S3 libraries cached in image")
