#!/usr/bin/env python3
import getpass
from pathlib import Path

import paramiko


HOST = "50.114.113.121"
USER = "root"
LOCAL_FILE = Path(__file__).resolve().parent / "hyperliquid_correlation_monitor.py"
REMOTE_FILE = "/opt/hyperliquid-monitor/hyperliquid_correlation_monitor.py"
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
            sftp.put(str(LOCAL_FILE), REMOTE_FILE)
        finally:
            sftp.close()
        print(run(ssh, f"python3 -m py_compile {REMOTE_FILE}") or "py_compile ok")
        run(ssh, f"systemctl restart {SERVICE}")
        print(run(ssh, f"systemctl is-active {SERVICE}"))
    finally:
        ssh.close()


if __name__ == "__main__":
    main()
