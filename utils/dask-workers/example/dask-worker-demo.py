# NOTE: Due to an interaction between CDSW's and Dask's umosse of
# Tornado, this script does not work in the workbench. It does,
# however, work when run from the terminal:
# 
#   python3 dask-worker-demo.py
#
# It will likely work from most third-party editors as well.

import os
import cdsw_dask_utils

# Run a Dask cluster with three workers and return a description.
cluster = cdsw_dask_utils.run_dask_cluster(
  n=3, \
  cpu=0.2, \
  memory=0.5
)

# Connect a Dask client to the scheduler address in the cluster
# description.
from dask.distributed import Client
client = Client(cluster["scheduler_address"])

# Use the client to run a distributed job.
def square(x):
  return x ** 2

def neg(x):
  return -x

A = client.map(square, range(10))
B = client.map(neg, A)

total = client.submit(sum, B)
print("Result: ", total.result())
