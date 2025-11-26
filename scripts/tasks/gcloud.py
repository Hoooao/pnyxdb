import os
import time

import yaml
from invoke import task

from copy import deepcopy

from . import remote


def get_gcloud_ext_ips(c):
    # parse gcloud CLI to get internalIP -> externalIP mapping
    gcloud_output = c.run("gcloud compute instances list").stdout.splitlines()[1:]
    gcloud_output = map(lambda s: s.split(), gcloud_output)
    ext_ips = {
        # internal ip and external ip are last 2 tokens in each line
        line[-3]: line[-2]
        for line in gcloud_output
    }
    return ext_ips


def get_all_int_ips(config):
    int_ips = set()
    for process in config:
        if process == "transport" or process == "app" or process == "resiliency":
            continue
        int_ips |= set([ip for ip in config[process]["ips"]])

    return int_ips


def get_all_ext_ips(config, ext_ip_map):
    ips = []
    for ip in get_all_int_ips(config):  # TODO non local receivers?
        if ip not in ext_ip_map:
            continue
        ips.append(ext_ip_map[ip])

    return ips


def get_address_resolver(context):
    ext_ip_map = get_gcloud_ext_ips(context)
    return lambda ip: ext_ip_map[ip]


@task
def vm(c, config_file="./remote_config.yaml", stop=False):
    config_file = os.path.abspath(config_file)

    with open(config_file) as cfg_file:
        config = yaml.load(cfg_file, Loader=yaml.Loader)

    int_ips = get_all_int_ips(config)

    gcloud_output = c.run("gcloud compute instances list").stdout.splitlines()[1:]
    gcloud_output = map(lambda s: s.split(), gcloud_output)

    vm_info = {
        # name, zone, type, internal ip
        line[3]: (line[0], line[1])
        for line in gcloud_output
    }

    hdls = []
    for ip in int_ips:
        name, zone = vm_info[ip]
        h = c.run(
            f"gcloud compute instances {'stop' if stop else 'start'} {name} --zone {zone}",
            asynchronous=True,
        )
        hdls.append(h)

    for h in hdls:
        h.join()

    print(f"{'Stopped' if stop else 'Started'} all instances!")


@task
def cmd(c, cmd, config_file="./remote_config.yaml"):
    remote.cmd(c, cmd, config_file=config_file, resolve=get_address_resolver(c))


@task
def copy_keys(c, config_file="./remote_config.yaml"):
    resolve = get_address_resolver(c)
    remote.copy_keys(c, config_file, resolve=resolve)


@task
def copy_bin(c, config_file="./remote_config.yaml", upload_once=False):
    resolve = get_address_resolver(c)
    remote.copy_bin(c, config_file, upload_once=upload_once, resolve=resolve)


def get_gcloud_process_ips(c, filter):
    gcloud_output = c.run(
        f"gcloud compute instances list | grep {filter}"
    ).stdout.splitlines()
    gcloud_output = map(lambda s: s.split(), gcloud_output)

    return [
        # internal ip is 3rd last token in line
        line[-3]
        for line in gcloud_output
    ]




# local_log_file is good for debugging, but will slow the system down at high throughputs
@task
def run(
    c,
    config_file="./remote_config.yaml",
    v=5
):
    # Wrapper around remote run to convert ips
    resolve = get_address_resolver(c)
    remote.run(
        c,
        config_file,
        resolve=resolve,
        v=v
    )


@task
def logs(
    c,
    config_file="./remote_config.yaml",
    resolve=lambda x: x,
):
    # ips of each process
    resolve = get_address_resolver(c)
    remote.logs(c, config_file=config_file, resolve=resolve)

