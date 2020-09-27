import cdsw, time

def await_workers(ids, wait_for_completion=True, timeout_seconds=60):
    """await_workers

    Description
    -----------

    Waits for workers to either reach the 'running' status, or to
    complete and exit.

    Parameters
    ----------
    ids: list of worker id's
        The id's of the worker engines to await. The id's are
        available in the "id" key of worker descriptions returned
        by launch_workers and list_workers.
    wait_for_completion: boolean, optional
        If True, will wait for all workers to exit successfully.
        If False, will wait for all workers to reach the 'running'
        status.
        Defaults to True.
    timeout_seconds: int, optional
        Maximum number of seconds to wait for workers to reach the
        desired status. Defaults to 60. If equal to 0, there is no
        timeout. Workers that have not reached the desired status
        by the timeout will be returned in the 'failures' key. See
        the return value documentation.

    Returns
    -------
    dict
        A dict with keys 'workers' and 'failures'. The 'workers'
        key contains a list of dicts describing the workers that
        reached the desired status. The 'failures' key contains a
        list of descriptions of the workers that did not.

        Note: If wait_for_completion is False, the workers in the
        'workers' key will contain a key called 'ip_address'
        which contains each worker's external IP address. This can be
        useful for running distributed frameworks on workers.
    """
    t = 0
    poll_interval = 5
    out = {"workers": [], "failures": []}
    running = set()
    ids_to_await = set(ids)
    while timeout_seconds != 0 and t <= timeout_seconds:
        workers_now = cdsw.list_workers()
        status_dict = dict([(worker['id'], worker['status']) for worker in workers_now])
        done = True
        for worker in workers_now:
            id_ = worker["id"]
            if id_ not in ids_to_await:
                continue
            if status_dict[id_] == 'failed':
                out['failures'].append(worker)
                ids_to_await.remove(id_)
            elif status_dict[id_] == 'timedout':
                out['failures'].append(worker)
                ids_to_await.remove(id_)
            elif status_dict[id_] == 'stopped':
                out['failures'].append(worker)
                ids_to_await.remove(id_)
            else:
                if wait_for_completion:
                    if status_dict[id_] == 'succeeded':
                        # If the worker has reached the 'succeeded' status,
                        # and we are waiting for completion, it is a success.
                        out['workers'].append(worker)
                        ids_to_await.remove(id_)
                    if status_dict[id_] != 'succeeded':
                        # If the worker is in a non-terminal state, and we
                        # are waiting for completion, exit the loop iteration
                        # and wait.
                        done = False
                        break
                else:
                    if status_dict[id_] == 'succeeded':
                        # We want the workers to reach 'running', so this is a
                        # failure.
                        out['failures'].append(worker)
                        ids_to_await.remove(id_)
                    elif status_dict[id_] == 'running' and worker.get("ip_address") != "unknown":
                        # If the worker has reached the 'running' status, and we
                        # are not waiting for completion, it is a success.
                        out['workers'].append(worker)
                        ids_to_await.remove(id_)
                    else:
                        # If the worker is in a non-terminal state but is not
                        # running, we need to exit the loop iteration and wait.
                        done = False
                        break
        if done:
            return out
        else:
            time.sleep(poll_interval)
            t = t + poll_interval

    # Here we have timed out. All workers that are not successes
    # are considered failures.
    if len(ids_to_await) > 0:
      workers_now = cdsw.list_workers()
      for worker in workers_now:
          if worker["id"] in ids_to_await:
              out['failures'].append(worker)

    return out
