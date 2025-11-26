import os
import tarfile
import tempfile
import time

import yaml
from fabric import Connection
from invoke import task
import invoke


CLIENT_WORKLOAD_SCRIPT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "client_workload.py")
)
if not os.path.isfile(CLIENT_WORKLOAD_SCRIPT):
    raise FileNotFoundError(f"client_workload.py not found at {CLIENT_WORKLOAD_SCRIPT}")


# =================================================
#               Helper functions
# =================================================


def resolve(c, ip, platform):
    if platform == "gcloud":
        return f"ubuntu@{ip}"
    elif platform == "cloudlab":
        return f"root@{ip}"
    else:
        raise ValueError(f"Unknown platform {platform}")


def arun_on(ip, logfile, timeout, profile=False):
    def perf_prefix(prof_file):
        return f"env LD_PRELOAD='/home/dqian/libprofiler.so' CPUPROFILE={prof_file} CPUPROFILE_FREQUENCY={10} "

    # Previous versions of this function logged directly to local files using a command like
    #   log = open(logfile, "w")
    #     ...
    #   conn.run(command + " 2>&1", **kwargs, asynchronous=True, warn=True, out_stream=log)
    # This was changed to use the remote machine's filesystem to avoid issues with this outstream flushing

    def arun(command, **kwargs):
        conn = Connection(ip)

        if profile:
            command = perf_prefix(os.path.splitext(logfile)[0] + ".prof") + command

        print(f"Running {command} on {ip}, logging on remote machine {logfile}")
        return conn.run(
            command + f" &>{logfile}",
            **kwargs,
            asynchronous=True,
            timeout=timeout,
            warn=True,
        )

    return arun


def get_logs(c, ips, log_descriptor):
    for id, ip in enumerate(ips):
        conn = Connection(ip)
        remote_home = conn.run("echo $HOME", hide=True).stdout.strip() or "/tmp"

        def expand_remote(path):
            if path.startswith("~"):
                return path.replace("~", remote_home, 1)
            return path

        if callable(log_descriptor):
            log_path = log_descriptor(id, ip)
        else:
            log_path = f"{log_descriptor}{id}.log"
        if not log_path:
            continue
        log_path = expand_remote(log_path)
        log_gz = f"{log_path}.gz"
        conn.run(f"rm -f {log_gz}", hide=True)
        conn.run(
            f"gzip {log_path}", hide=True, warn=True
        )  # original files are too large
        conn.get(log_gz, "../logs/")


def replica_log_path(id, _ip=None):
    return f"~/replica{id}/replica{id}.log"


def client_log_path(id, _ip=None):
    if id == 0:
        return f"~/client{id}/client{id}.log"
    return ""


def get_process_ips(config_file, resolve):
    config_file = os.path.abspath(config_file)

    with open(config_file) as cfg_file:
        config = yaml.load(cfg_file, Loader=yaml.Loader)

    replicas = [resolve(ip) for ip in config["replica"]["ips"]]
    return replicas



def get_all_ips(config_file, resolve):
    ips = []
    for entry in get_process_ips(config_file, resolve):
        if isinstance(entry, (list, tuple, set)):
            ips.extend(entry)
        else:
            ips.append(entry)
    return list(dict.fromkeys(ips))  # preserve order, remove duplicates


def run_on_all(ips, command, **kwargs):
    for ip in ips:
        conn = Connection(ip)
        conn.run(command, **kwargs)


def put_on_all(ips, local_path, remote=None, **kwargs):
    for ip in ips:
        conn = Connection(ip)
        conn.put(local_path, remote=remote, **kwargs)


def upload_directory(conn, local_dir, remote_tar_dir="/tmp", remote_extract_dir="~"):
    local_dir = os.path.abspath(local_dir)
    base_name = os.path.basename(os.path.normpath(local_dir))
    fd, tmp_path = tempfile.mkstemp(suffix=".tar.gz")
    os.close(fd)
    remote_tar = f"{remote_tar_dir.rstrip('/')}/{base_name}.tar.gz"

    try:
        with tarfile.open(tmp_path, "w:gz") as tar:
            tar.add(local_dir, arcname=base_name)

        conn.put(tmp_path, remote=remote_tar)
        conn.run(
            f"tar -xzf {remote_tar} -C {remote_extract_dir}",
            warn=True,
        )
        conn.run(f"rm -f {remote_tar}", warn=True)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


# =================================================
#             Main experiment tasks
# =================================================


@task
def logs(c, config_file="./remote_config.yaml", resolve=lambda x: x):
    # ips of each process
    c.run("rm -f ../logs/*.log")

    replicas = get_process_ips(config_file, resolve)

    get_logs(c, replicas, replica_log_path)
    get_logs(c, replicas, client_log_path)

@task
def run(
    # Invoke context
    c,
    # Necessary args to run dombft
    config_file="./remote_config.yaml",  # Path to the config file on the local machine
    # function to resolve addresses in the config file to accesible addresses
    resolve=lambda x: x,
    # Options for logging/output to fetch
    v=5,
):
    config_file = os.path.abspath(config_file)

    with open(config_file) as f:
        cfg = yaml.load(f, Loader=yaml.Loader)

    replicas = get_process_ips(config_file, resolve)
    all_ips = get_all_ips(config_file, resolve)

    # Kill previous runs
    run_on_all(
        all_ips,
        "killall pnyxdb",
        warn=True,
        hide="both",
    )

    # Run a dummy command with pty to log a session so that the machine doesn't shutdown from being inactive
    run_on_all(all_ips, "echo ''", pty=True)
    replica_handles = []
    print("Starting replicas")
    for id, ip in enumerate(replicas):
        # push the replica folder to the remote machine
        conn = Connection(ip)
        replica_dir = f"../machines/replica{id}"
        conn.run(f"rm -rf ~/replica{id}", warn=True)
        upload_directory(conn, replica_dir)
        cmd = f"export PASSWORD='1' && cd ~/replica{id} && ../pnyxdb -c ./config.yaml server start"
        log_file = f"~/replica{id}/replica{id}.log"
        hdl = arun_on(ip, log_file, timeout=cfg.get("runtimeSeconds", 600) + 100)(cmd)
        replica_handles.append(hdl)

    # on first replica, run the client
    client_ip = replicas[0]
    client_id = 0
    client_conn = Connection(client_ip)
    cli_folder = f"~/client{client_id}"
    client_conn.run(f"rm -rf {cli_folder}", warn=True)
    client_conn.run(f"mkdir -p {cli_folder}", warn=True)
    print("Starting client")
    client_conn.put(CLIENT_WORKLOAD_SCRIPT)
    client_conn.run(f"mv client_workload.py {cli_folder}/", warn=True)
    client_conn.run(f"chmod +x {cli_folder}/client_workload.py", warn=True)
    send_rate = float(cfg.get("sendRate", 0) or 0)
    runtime_seconds = float(cfg.get("runtimeSeconds", 600) or 0)
    client_start_delay = float(cfg.get("clientStartDelay", 2))
    server_addr = f"127.0.0.1:{4200}" # problematic when use remote IP. 
    client_script = (
        f"cd {cli_folder} && "
        "python3 client_workload.py "
        f"--binary ../pnyxdb --server {server_addr} "
        f"--rate {send_rate} --duration {runtime_seconds} "
        f"--start-delay {client_start_delay}"
    )
    cli_log = f"{cli_folder}/client{client_id}.log"
    time.sleep(3)  # give replicas time to start up
    client_hdl = arun_on(
        client_ip,
        cli_log,
        timeout=cfg.get("runtimeSeconds", 600) + 0,
    )(client_script)
    try:
        # join on the client processes, which should end
        client_hdl.join()

    finally:
        print("Waiting for other processes to finish...")

        # kill these processes and then join
        run_on_all(
            all_ips,
            "killall pnyxdb",
            warn=True,
            hide="both",
        )

        for hdl in replica_handles:
            try:
                hdl.join()
            except invoke.exceptions.CommandTimedOut as e:
                print(f"{e}")

        c.run("rm -f ../logs/*", warn=True)

        get_logs(c, replicas, replica_log_path)
        get_logs(c, [replicas[0]], client_log_path)


#==========================================
#             Other tasks
# =================================================

@task
def copy_bin(
    c, config_file="./remote_config.yaml", upload_once=False, resolve=lambda x: x
):
    
    #c.run("ssh-add -D")
    replicas = get_process_ips(config_file, resolve)
    all_ips = get_all_ips(config_file, resolve)
    print("Killing existing binaries...")
    print(replicas)
    
    if upload_once:
        # TODO try and check to see if binaries are stale
        print(f"Copying binaries over to one machine {replicas[0]}")
        start_time = time.time()
        conn = Connection(replicas[0])

        conn.put("../pnyxdb")
        conn.run("chmod +w pnyxdb", warn=True)

        print(f"Copying took {time.time() - start_time:.0f}s")

        print(f"Copying to other machines")
        start_time = time.time()

        replicas = get_process_ips(config_file, lambda x: x)

        for ip in replicas:
            print(f"Copying pnyxdb to {ip}")
            conn.run(f"scp -o StrictHostKeyChecking=no pnyxdb {ip}:", warn=True)

        print(f"Copying to other machines took {time.time() - start_time:.0f}s")

    else:
        # Otherwise, just copy to all machines

        run_on_all(all_ips, "rm -f pnyxdb", warn=True)
        run_on_all(all_ips, "chmod +w pnyxdb", warn=True)

        print("Copying binaries over...")

        put_on_all(all_ips, "../pnyxdb")
        print("Copied to replica")

        run_on_all(all_ips, "chmod +w pnyxdb", warn=True)


@task
def cmd(c, cmd, config_file="./remote_config.yaml", resolve=lambda x: x):
    ips = get_all_ips(config_file, resolve)
    print(ips)
    run_on_all(ips, cmd)
