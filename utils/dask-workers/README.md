## Demo of Dask on CDSW Workers

To use, first `pip3 install -r requirements.txt` then run 
`python3 dask-worker-demo.py` **in a terminal**.
It unfortunately won't work in a session due to an apparent
incompatibility between the Tornado version Dask wants and
the one Jupyter uses, in the Jupyter version upon which CDSW's
Python 3 kernel is based.

The main library function is `run_dask_cluster` in 
`cdsw_dask_utils.py` It will run a Dask scheduler in the local
session, then will run the requested number of Dask workers 
inside CDSW workers and will wait for everything to come up.