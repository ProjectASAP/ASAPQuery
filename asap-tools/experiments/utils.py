from typing import Union
import subprocess
import time

import constants


def run_cmd_with_retry(
    cmd, popen, ignore_errors=False, max_retries=3, retry_delay=5
) -> Union[subprocess.Popen, subprocess.CompletedProcess]:
    """
    Run a command with retry logic for SSH/rsync failures.

    Args:
        cmd: Command to execute
        popen: If True, use Popen (non-blocking), else use run (blocking)
        ignore_errors: If True, don't raise exception on failure
        max_retries: Maximum number of retry attempts (default: 3)
        retry_delay: Delay in seconds between retries (default: 5)

    Returns:
        Popen or CompletedProcess object

    Raises:
        CalledProcessError: If command fails after all retries (and not ignoring errors)
    """
    # Detect if this is an ssh/rsync command
    is_ssh_rsync = "ssh" in cmd.lower() or "rsync" in cmd.lower()

    # Only apply retry logic for ssh/rsync commands
    if not is_ssh_rsync:
        return run_cmd(cmd, popen, ignore_errors)

    attempt = 0
    last_exception = None

    while attempt <= max_retries:
        try:
            if attempt > 0:
                print(
                    f"Retry attempt {attempt}/{max_retries} after {retry_delay}s delay..."
                )
                time.sleep(retry_delay)

            return run_cmd(cmd, popen, ignore_errors)

        except subprocess.CalledProcessError as e:
            # Only retry on SSH connection failures (exit code 255)
            if e.returncode == 255:
                last_exception = e
                print(
                    f"SSH/rsync connection failed (exit 255) on attempt {attempt + 1}"
                )
                attempt += 1
                if attempt > max_retries:
                    print(f"Failed after {max_retries + 1} attempts")
                    raise
            else:
                # For other error codes, raise immediately
                print(f"Command failed with exit code {e.returncode} (not retrying)")
                raise

    # This should never be reached, but just in case
    if last_exception:
        raise last_exception


def get_ssh_cmd(username, ip, cmd, cmd_dir, nohup, redirect=False):
    user = username
    if nohup:
        cmd = f"nohup {cmd}"

    if cmd_dir:
        cmd = f'ssh {constants.SSH_OPTIONS} {user}@{ip} "cd {cmd_dir}; {cmd}"'
    else:
        cmd = f'ssh {constants.SSH_OPTIONS} {user}@{ip} "{cmd}"'

    if redirect:
        cmd = f"{cmd} < /dev/null > /dev/null 2>&1"
    return cmd


def run_cmd(
    cmd, popen, ignore_errors=False
) -> Union[subprocess.Popen, subprocess.CompletedProcess]:
    print(cmd)
    if popen:
        return subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
    else:
        try:
            return subprocess.run(
                cmd,
                shell=True,
                check=not ignore_errors,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            # Print captured output for debugging
            print("=" * 80)
            print("ERROR: Command failed with exit code:", e.returncode)
            print("=" * 80)
            if e.stdout:
                print("STDOUT:")
                print(e.stdout)
                print("=" * 80)
            if e.stderr:
                print("STDERR:")
                print(e.stderr)
                print("=" * 80)
            # Re-raise the exception
            raise


def run_on_cloudlab_node(
    node_idx,
    username,
    hostname_suffix,
    cmd,
    cmd_dir,
    nohup,
    popen,
    ignore_errors=False,
    manual=False,
):
    hostname = f"node{node_idx}.{hostname_suffix}"
    ssh_cmd = get_ssh_cmd(username, hostname, cmd, cmd_dir, nohup)
    if manual:
        print(f"Run the following command on {hostname}:")
        print(ssh_cmd)
        input("Press Enter to continue...")
        return
    return run_cmd_with_retry(ssh_cmd, popen, ignore_errors=ignore_errors)


def run_on_cloudlab_nodes_in_parallel(
    node_idxs, username, hostname_suffix, cmd, cmd_dir, nohup, popen, redirect=False
):
    if not popen:
        raise ValueError("popen must be True to run commands in parallel")

    popens = []
    for node_idx in node_idxs:
        hostname = f"node{node_idx}.{hostname_suffix}"
        ssh_cmd = get_ssh_cmd(username, hostname, cmd, cmd_dir, nohup, redirect)
        popens.append(run_cmd_with_retry(ssh_cmd, popen))

    for popen in popens:
        popen.wait()


def run_on_cloudlab_nodes_in_parallel_without_wait(
    node_idxs, username, hostname_suffix, cmd, cmd_dir, nohup, popen, redirect=False
):
    if not popen:
        raise ValueError("popen must be True to run commands in parallel")

    popens = []
    for node_idx in node_idxs:
        hostname = f"node{node_idx}.{hostname_suffix}"
        ssh_cmd = get_ssh_cmd(username, hostname, cmd, cmd_dir, nohup, redirect)
        popens.append(run_cmd_with_retry(ssh_cmd, popen))
