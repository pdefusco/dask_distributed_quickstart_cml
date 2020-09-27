import os, subprocess, cdsw, socket, time, cdsw_await_workers

# Since CDSW engines have independent IP addresses and 
# their own port ranges, it's simpler to hardcode the 
# scheduler port.
default_scheduler_port = 2323

def scheduler_address(scheduler_port=default_scheduler_port):
  return "%s:%d" % (os.environ["CDSW_IP_ADDRESS"], scheduler_port)

def run_scheduler(scheduler_port=default_scheduler_port):
  """
  Run a Dask scheduler process in the background.
  
  The dashboard will be available in the CDSW nine-dot menu
  at the upper right corner of the app.
  """
  scheduler_proc = subprocess.Popen([
    "dask-scheduler",
    "--host", 
    os.environ["CDSW_IP_ADDRESS"], 
    "--port", 
    str(scheduler_port),
    "--dashboard-address",
    ("127.0.0.1:%s" % os.environ["CDSW_READONLY_PORT"])

  ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  
  # Wait for the scheduler to become ready
  print("Waiting for Dask scheduler to become ready...")
  while True:
    try:
      with socket.create_connection((os.environ["CDSW_IP_ADDRESS"], scheduler_port), timeout=1.0):
        break
    except OSError as ex:
      time.sleep(0.01)
  print("Dask scheduler is ready")
  return scheduler_proc

def _run_dask_worker_in_worker(scheduler_port):
  """
  Run a Dask worker process in the background.
  
  This function assumes it will run in a CDSW worker and that
  the scheduler process runs in the CDSW master, so it will
  look for the scheduler's IP address in environmental variable 
  $CDSW_MASTER_IP.
  """
  print("Starting Dask worker")
  return subprocess.Popen([
    "dask-worker",
    ("%s:%d" % (os.environ["CDSW_MASTER_IP"], scheduler_port))                        
  ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def run_dask_workers(n, cpu, memory, nvidia_gpu=0, scheduler_port=default_scheduler_port):
  """
  Run a CDSW worker, and run a Dask worker inside it.
  Assumes that the scheduler is running on the CDSW master.
  """
  worker_code = """
import cdsw_dask_utils
worker_proc = cdsw_dask_utils._run_dask_worker_in_worker(scheduler_port=%d)
# Keep the CDSW worker alive until the Dask worker exits.
print(worker_proc.wait())
""" % scheduler_port
  workers = cdsw.launch_workers(
    n=n, \
    cpu=cpu, \
    memory=memory, \
    nvidia_gpu=nvidia_gpu, \
    kernel="python3", \
    code=worker_code
  )
  
  try:
    ids = [worker['id'] for worker in  workers]
    
  except KeyError as key : 
    errors = [[worker['k8sMessage'],worker['engineId']] for worker in workers ]
    for error in errors : 
        print('''worker {} failed to launch with err message : 
              {}'''.format(error[1],error[0]))
    raise RuntimeError("failed to launch workers with err : "+error[0])
  
  print("IDs", ids)
  # Wait for the workers to start running, but don't wait for them to exit - 
  # we want them to stay up for use as daemons.
  cdsw_await_workers.await_workers(ids, wait_for_completion=False)
  return workers

def run_dask_cluster(n, cpu, memory, nvidia_gpu=0, scheduler_port=default_scheduler_port):
  """
  Runs a Dask scheduler in the local session, and runs Dask
  workers in CDSW workers.
  """
  scheduler_proc = run_scheduler(scheduler_port)
  workers = run_dask_workers(n=n, cpu=cpu, memory=memory, nvidia_gpu=nvidia_gpu, scheduler_port=scheduler_port)
  return {
    "scheduler_proc": scheduler_proc,
    "workers": workers,
    "scheduler_address": scheduler_address(scheduler_port)
  }
