use std::io::ErrorKind;
use std::io::Read;
use std::io::Write;
use std::process::Command;
use std::process::Stdio;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use pretty_assertions::assert_eq;

#[cfg(unix)]
use std::os::unix::net::UnixListener;

#[cfg(windows)]
use uds_windows::UnixListener;

#[test]
fn pipes_stdin_and_stdout_through_socket() -> anyhow::Result<()> {
    let dir = tempfile::TempDir::new().context("failed to create temp dir")?;
    let socket_path = dir.path().join("socket");
    let listener = match UnixListener::bind(&socket_path) {
        Ok(listener) => listener,
        Err(err) if err.kind() == ErrorKind::PermissionDenied => {
            eprintln!("skipping test: failed to bind unix socket: {err}");
            return Ok(());
        }
        Err(err) => {
            return Err(err).context("failed to bind test unix socket");
        }
    };

    let (tx, rx) = mpsc::channel();
    let server_thread = thread::spawn(move || -> anyhow::Result<()> {
        let (mut connection, _) = listener
            .accept()
            .context("failed to accept test connection")?;
        let mut received = Vec::new();
        connection
            .read_to_end(&mut received)
            .context("failed to read data from client")?;
        tx.send(received)
            .map_err(|_| anyhow::anyhow!("failed to send received bytes to test thread"))?;
        connection
            .write_all(b"response")
            .context("failed to write response to client")?;
        Ok(())
    });

    let mut child = Command::new(codex_utils_cargo_bin::cargo_bin("codex-stdio-to-uds")?)
        .arg(&socket_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn codex-stdio-to-uds")?;
    {
        let mut stdin = child
            .stdin
            .take()
            .context("codex-stdio-to-uds missing stdin pipe")?;
        stdin
            .write_all(b"request")
            .context("failed to write request to codex-stdio-to-uds stdin")?;
    }

    let timeout = Duration::from_secs(5);
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .context("failed to poll codex-stdio-to-uds status")?
        {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().ok();
            return Err(anyhow::anyhow!(
                "codex-stdio-to-uds did not exit within {timeout:?}"
            ));
        }
        thread::sleep(Duration::from_millis(10));
    };

    let mut stdout = String::new();
    child
        .stdout
        .take()
        .context("codex-stdio-to-uds missing stdout pipe")?
        .read_to_string(&mut stdout)
        .context("failed to read codex-stdio-to-uds stdout")?;

    let mut stderr = String::new();
    child
        .stderr
        .take()
        .context("codex-stdio-to-uds missing stderr pipe")?
        .read_to_string(&mut stderr)
        .context("failed to read codex-stdio-to-uds stderr")?;

    assert!(
        status.success(),
        "codex-stdio-to-uds exited with {status}: {stderr}"
    );
    assert_eq!(stdout, "response");

    let received = rx
        .recv_timeout(Duration::from_secs(5))
        .context("server did not receive data in time")?;
    assert_eq!(received, b"request");

    let server_result = server_thread
        .join()
        .map_err(|_| anyhow::anyhow!("server thread panicked"))?;
    server_result.context("server failed")?;

    Ok(())
}
