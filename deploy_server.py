#!/usr/bin/env python3
import getpass
from pathlib import Path

import paramiko


HOST = "50.114.113.121"
USER = "root"
LOCAL_ROOT = Path(__file__).resolve().parent
REMOTE_ROOT = "/opt/hyperliquid-monitor"
DEPLOY_FILES = ("hyperliquid_correlation_monitor.py", "crypto_strategy_lab.py", "crypto_strategy_pine.py")
SERVICE = "hyperliquid-alt-monitor.service"


def run(ssh, command):
    stdin, stdout, stderr = ssh.exec_command(command)
    code = stdout.channel.recv_exit_status()
    out = stdout.read().decode("utf-8", "replace").strip()
    err = stderr.read().decode("utf-8", "replace").strip()
    if code != 0:
        raise RuntimeError(f"{command}\n{err or out}")
    return out


def main():
    password = getpass.getpass(f"{USER}@{HOST} password: ")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USER, password=password, timeout=15)
    try:
        sftp = ssh.open_sftp()
        try:
            for name in DEPLOY_FILES:
                sftp.put(str(LOCAL_ROOT / name), f"{REMOTE_ROOT}/{name}.new")
        finally:
            sftp.close()
        remote_new = " ".join(f"{REMOTE_ROOT}/{name}.new" for name in DEPLOY_FILES)
        print(run(ssh, f"python3 -m py_compile {remote_new}") or "py_compile ok")
        for name in DEPLOY_FILES:
            run(ssh, f"mv {REMOTE_ROOT}/{name}.new {REMOTE_ROOT}/{name}")
        run(ssh, f"systemctl restart {SERVICE}")
        print(run(ssh, f"systemctl is-active {SERVICE}"))
    finally:
        ssh.close()


if __name__ == "__main__":
    main()
