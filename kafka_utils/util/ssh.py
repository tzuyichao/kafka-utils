# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import os
import sys
import time
from contextlib import closing
from contextlib import contextmanager
from typing import Iterator

from paramiko import ProxyCommand
from paramiko import SSHConfig
from paramiko.agent import AgentRequestHandler
from paramiko.channel import ChannelFile
from paramiko.client import AutoAddPolicy
from paramiko.client import SSHClient

from kafka_utils.util.error import MaxConnectionAttemptsError


class Connection:
    """Represents a SSH connection with an SSH server.
    """

    def __init__(self, client: SSHClient, forward_agent: bool, sudoable: bool) -> None:
        self.client = client
        self.transport = client.get_transport()
        self.forward_agent = forward_agent
        self.sudoable = sudoable

    def sudo_command(self, command: str, passwd: str, bufsize: int = -1) -> tuple[ChannelFile, ChannelFile, ChannelFile]:
        new_command = f"sudo {command}"
        return self.exec_command(new_command, passwd, bufsize)

    def exec_command(self, command: str, passwd: str, bufsize: int = -1, check_status: bool = True) -> tuple[ChannelFile, ChannelFile, ChannelFile]:
        assert self.transport is not None
        chan = self.client.invoke_shell()
        chan.exec_command(command)
        output = chan.recv(1024).decode('utf-8')
        if 'Password for' in output:
            chan.send(f"{passwd}\n")
        stdin = chan.makefile('wb', bufsize)
        stdout = chan.makefile('rb', bufsize)
        stderr = chan.makefile_stderr('rb', bufsize)
        return (stdin, stdout, stderr)
    
    def put_file(self, local_path: str, remote_path: str) -> None:
        """Put a file on the SSH server.

        :param local_path: the local path of the file to put
        :type local_path: str
        :param remote_path: the remote path to put the file
        :type remote_path: str
        """
        assert self.transport is not None
        sftp = self.transport.open_sftp_client()
        sftp.put(local_path, remote_path)
        sftp.close()

    def sudo_command(self, command: str, bufsize: int = -1) -> tuple[ChannelFile, ChannelFile, ChannelFile]:
        """Sudo a command on the SSH server.
        Delegates to :func`~ssh.Connection.exec_command`

        :param command: the command to execute
        :type command: str
        :param bufsize: interpreted the same way as by the built-in C{file()} function in python
        :type bufsize: int
        :returns the stdin, stdout, and stderr of the executing command
        :rtype: tuple(L{ChannelFile}, L{ChannelFile}, L{ChannelFile})

        :raises SSHException: if the server fails to execute the command
        """
        new_command = f"sudo {command}"
        return self.exec_command(new_command, bufsize)

    def exec_command(self, command: str, bufsize: int = -1, check_status: bool = True) -> tuple[ChannelFile, ChannelFile, ChannelFile]:
        """Execute a command on the SSH server while preserving underling
        agent forwarding and sudo privileges.
        https://github.com/paramiko/paramiko/blob/1.8/paramiko/client.py#L348

        :param command: the command to execute
        :type command: str
        :param bufsize: interpreted the same way as by the built-in C{file()} function in python
        :type bufsize: int
        :param check_staus: if enabled, waits for the command to complete and return an exception
        if the status is non-zero.
        :type check_staus: bool
        :returns the stdin, stdout, and stderr of the executing command
        :rtype: tuple(L{ChannelFile}, L{ChannelFile}, L{ChannelFile})

        :raises SSHException: if the server fails to execute the command
        """
        assert self.transport is not None
        channel = self.transport.open_session()

        if self.forward_agent:
            AgentRequestHandler(channel)
        if self.sudoable:
            channel.get_pty()

        channel.exec_command(command)
        if check_status and channel.recv_exit_status() != 0:               # 
            raise RuntimeError(f"Command execution error: {command}")      #
        # time.sleep(2)

        stdin = channel.makefile('wb', bufsize)
        # stdin.write("<PASSWPRD>")
        # stdin.flush()
        stdout = channel.makefile('rb', bufsize)
        stderr = channel.makefile_stderr('rb', bufsize)
        return (stdin, stdout, stderr)


@contextmanager
def ssh(host: str, forward_agent: bool = False, sudoable: bool = False, max_attempts: int = 1, max_timeout: int = 5, ssh_password: str | None = None) -> Iterator[Connection]:
    """Manages a SSH connection to the desired host.
       Will leverage your ssh config at ~/.ssh/config if available

    :param host: the server to connect to
    :type host: str
    :param forward_agent: forward the local agents
    :type forward_agent: bool
    :param sudoable: allow sudo commands
    :type sudoable: bool
    :param max_attempts: the maximum attempts to connect to the desired host
    :type max_attempts: int
    :param max_timeout: the maximum timeout in seconds to sleep between attempts
    :type max_timeout: int
    :param ssh_password: SSH password to use if needed
    :type ssh_password: str
    :returns a SSH connection to the desired host
    :rtype: Connection

    :raises MaxConnectionAttemptsError: Exceeded the maximum attempts
    to establish the SSH connection.
    """
    with closing(SSHClient()) as client:
        client.set_missing_host_key_policy(AutoAddPolicy())

        cfg = {
            "hostname": host,
            "timeout": max_timeout,
        }
        if ssh_password:
            cfg['password'] = ssh_password

        ssh_config = SSHConfig()
        user_config_file = os.path.expanduser("~/.ssh/config")
        if os.path.exists(user_config_file):
            with open(user_config_file) as f:
                ssh_config.parse(f)
                host_config = ssh_config.lookup(host)
                if "user" in host_config:
                    cfg["username"] = host_config["user"]

                if "proxycommand" in host_config:
                    cfg["sock"] = ProxyCommand(host_config["proxycommand"])

                if "identityfile" in host_config:
                    cfg['key_filename'] = host_config['identityfile']

                if "port" in host_config:
                    cfg["port"] = int(host_config["port"])

        attempts = 0
        while attempts < max_attempts:
            try:
                attempts += 1
                client.connect(**cfg)  # type: ignore[arg-type]
                break
            except OSError as e:
                if attempts < max_attempts:
                    print(f"SSH to host {host} failed, retrying...")
                    time.sleep(max_timeout)
                else:
                    print(f"SSH Exception: {e}")

        else:
            raise MaxConnectionAttemptsError(
                f"Exceeded max attempts to connect to host {host} after {max_attempts} retries"
            )

        yield Connection(client, forward_agent, sudoable)


def report_stdout(host: str, stdout: ChannelFile) -> None:
    """Take a stdout and print it's lines to output if lines are present.

    :param host: the host where the process is running
    :type host: str
    :param stdout: the std out of that process
    :type stdout: paramiko.channel.Channel
    """
    lines = stdout.readlines()
    if lines:
        print(f"STDOUT from {host}:")
        for line in lines:
            print(line.rstrip(), file=sys.stdout)


def report_stderr(host: str, stderr: ChannelFile) -> None:
    """Take a stderr and print it's lines to output if lines are present.

    :param host: the host where the process is running
    :type host: str
    :param stderr: the std error of that process
    :type stderr: paramiko.channel.Channel
    """
    lines = stderr.readlines()
    if lines:
        print(f"STDERR from {host}:")
        for line in lines:
            print(line.rstrip(), file=sys.stderr)
