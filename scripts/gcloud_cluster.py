#!/usr/bin/env python3
"""
Provision a small PnyxDB cluster on Google Cloud Compute Engine.

The flow mimics scripts/local_cluster.py but additionally spins up GCE instances,
copies the generated artifacts, and (optionally) boots each node.

Example:
    PASSWORD=secret python3 scripts/gcloud_cluster.py \
        --project my-gcp-project \
        --zone us-central1-a \
        --nodes alice bob carol dave
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path

# Remove redundant functions by reusing logging config logic only

CURRENT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(CURRENT_DIR))
from setup_cluster import write_config  # reuse config writer from local script

def run(cmd, *, capture=False):
    if capture:
        return subprocess.run(cmd, check=True, text=True, capture_output=True).stdout.strip()
    subprocess.run(cmd, check=True)


def ensure_instance(name, project, zone, machine_type, image_family, image_project, disk_size, tags):
    describe = ["gcloud", "compute", "instances", "describe", name, "--project", project, "--zone", zone]
    exists = subprocess.run(describe, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if exists.returncode == 0:
        print(f"[gcloud] VM {name} already exists")
        return

    cmd = [
        "gcloud",
        "compute",
        "instances",
        "create",
        name,
        "--project",
        project,
        "--zone",
        zone,
        "--machine-type",
        machine_type,
        "--image-family",
        image_family,
        "--image-project",
        image_project,
        "--boot-disk-size",
        str(disk_size),
    ]
    if tags:
        cmd.extend(["--tags", tags])

    print(f"[gcloud] creating VM: {' '.join(cmd)}")
    run(cmd)


def get_ip(name, project, zone, field):
    fmt = f"value({field})"
    cmd = [
        "gcloud",
        "compute",
        "instances",
        "describe",
        name,
        "--project",
        project,
        "--zone",
        zone,
        f"--format={fmt}",
    ]
    return run(cmd, capture=True)


def scp_to_instance(instance, project, zone, local_path, remote_path, recursive=False):
    cmd = [
        "gcloud",
        "compute",
        "scp",
        "--project",
        project,
        "--zone",
        zone,
    ]
    if recursive:
        cmd.append("--recurse")
    cmd.extend([str(local_path), f"{instance}:{remote_path}"])
    run(cmd)


def ssh_command(instance, project, zone, command):
    cmd = [
        "gcloud",
        "compute",
        "ssh",
        instance,
        "--project",
        project,
        "--zone",
        zone,
        "--command",
        command,
    ]
    run(cmd)


def bootstrap(nodes, workspace, password, project, zone, machine_type, image_family, image_project, disk_size, tags, start):
    repo_root = Path(__file__).resolve().parents[1]
    bin_path = repo_root / "bin" / "pnyxdb"
    build_binary(bin_path)

    workspace = workspace.resolve()
    workspace.mkdir(parents=True, exist_ok=True)

    configs = {}
    for idx, node in enumerate(nodes):
        node_dir = workspace / node
        node_dir.mkdir(parents=True, exist_ok=True)
        cfg_path = node_dir / "config.yaml"
        write_config(
            cfg_path,
            node,
            len(nodes),
            4100 + idx,
            4200 + idx,
            listen_host="0.0.0.0",
            api_host="0.0.0.0",
        )
        init_keys(bin_path, cfg_path, password)
        configs[node] = cfg_path

    # Mutual trust
    key_material = {node: export_key(bin_path, cfg) for node, cfg in configs.items()}
    for node, cfg in configs.items():
        for peer, exported in key_material.items():
            if node == peer:
                continue
            import_key(bin_path, cfg, peer, exported, "high")
            sign_key(bin_path, cfg, peer, password)

    # Provision VMs and sync artifacts
    for node in nodes:
        instance = f"pnyxdb-{node}"
        ensure_instance(instance, project, zone, machine_type, image_family, image_project, disk_size, tags)

        # Upload binary and node directory
        remote_base = f"~/pnyxdb/{node}"
        ssh_command(instance, project, zone, f"mkdir -p ~/pnyxdb/bin {remote_base}")
        scp_to_instance(instance, project, zone, bin_path, "~/pnyxdb/bin/pnyxdb")
        scp_to_instance(instance, project, zone, workspace / node, remote_base, recursive=True)
        ssh_command(instance, project, zone, "chmod +x ~/pnyxdb/bin/pnyxdb")

        if start:
            remote_cmd = (
                f"cd {remote_base} && "
                f"PASSWORD={shlex.quote(password)} "
                f"nohup ~/pnyxdb/bin/pnyxdb -c config.yaml server >/tmp/pnyxdb.log 2>&1 &"
            )
            ssh_command(instance, project, zone, remote_cmd)

        internal_ip = get_ip(instance, project, zone, "networkInterfaces[0].networkIP")
        external_ip = get_ip(instance, project, zone, "networkInterfaces[0].accessConfigs[0].natIP")
        print(f"[info] {node} -> {instance} internal={internal_ip} external={external_ip}")

    print(
        "\nAll nodes staged on GCE. If you did not pass --start, launch a node via:\n"
        "  gcloud compute ssh <instance> --command 'cd ~/pnyxdb/<node> && PASSWORD=**** ~/pnyxdb/bin/pnyxdb -c config.yaml server'\n"
        "Once each node prints its multiaddress, add the peers to the other configs (p2p.peers)."
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Provision a PnyxDB cluster on Google Cloud.")
    parser.add_argument("--nodes", nargs="+", default=["alice", "bob", "carol", "dave"], help="Node identities.")
    parser.add_argument("--workspace", type=Path, default=Path("clusters/gcloud"), help="Where to stage configs.")
    parser.add_argument("--password", default=os.environ.get("PASSWORD"), help="Password for every keyring.")
    parser.add_argument("--project", required=True, help="GCP project ID.")
    parser.add_argument("--zone", default="us-central1-a", help="Compute Engine zone.")
    parser.add_argument("--machine-type", default="e2-standard-2", help="Machine type.")
    parser.add_argument("--image-family", default="debian-12", help="Image family.")
    parser.add_argument("--image-project", default="debian-cloud", help="Image project.")
    parser.add_argument("--disk-size", type=int, default=20, help="Boot disk size (GB).")
    parser.add_argument("--network-tags", default="", help="Optional comma-separated network tags.")
    parser.add_argument("--start", action="store_true", help="Start the server on each VM after syncing.")
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.password:
        print("ERROR: provide --password or set PASSWORD env var.", file=sys.stderr)
        sys.exit(1)

    nodes = [n.strip() for n in args.nodes if n.strip()]
    if not nodes:
        print("ERROR: need at least one node.", file=sys.stderr)
        sys.exit(1)

    bootstrap(
        nodes,
        args.workspace,
        args.password,
        args.project,
        args.zone,
        args.machine_type,
        args.image_family,
        args.image_project,
        args.disk_size,
        args.network_tags,
        args.start,
    )


if __name__ == "__main__":
    main()
