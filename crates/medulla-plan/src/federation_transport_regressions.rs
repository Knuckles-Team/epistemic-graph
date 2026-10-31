//! Certificate dependency migration transport regressions.

use super::{pinned_agent, ValidatedHttpJsonTarget};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

fn accept_bounded(server: TcpListener) -> std::net::TcpStream {
    server.set_nonblocking(true).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        match server.accept() {
            Ok((socket, _)) => return socket,
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("local transport did not connect: {error}"),
        }
    }
}

fn read_request(server: TcpListener) -> (std::net::TcpStream, String) {
    let mut socket = accept_bounded(server);
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut request = [0; 2048];
    let size = socket.read(&mut request).unwrap();
    (
        socket,
        String::from_utf8_lossy(&request[..size]).into_owned(),
    )
}

#[test]
fn pinned_destination_ignores_dns_and_proxy_and_refuses_redirects() {
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let redirected = TcpListener::bind("127.0.0.1:0").unwrap();
    redirected.set_nonblocking(true).unwrap();
    let target = ValidatedHttpJsonTarget {
        addresses: vec![server.local_addr().unwrap()],
        https_only: false,
    };
    let destination = redirected.local_addr().unwrap();
    let (finished, completion) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let (mut socket, request) = read_request(server);
        assert!(request.starts_with("GET /pinned HTTP/1.1"));
        write!(socket, "HTTP/1.1 302 Found\r\nLocation: http://{destination}/escape\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
        finished.send(()).unwrap();
    });
    let config = ureq::Agent::config_builder()
        .proxy(Some(ureq::Proxy::new("http://127.0.0.1:1").unwrap()))
        .max_redirects(5)
        .timeout_global(Some(Duration::from_secs(3)));
    let agent = pinned_agent(&target, config);
    assert!(agent.config().proxy().is_none());
    assert_eq!(agent.config().max_redirects(), 0);
    let response = agent.get("http://must-not-resolve.invalid/pinned").call();
    assert!(response.is_err() || response.unwrap().status().is_redirection());
    completion
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("local HTTP peer must finish");
    assert_eq!(
        redirected.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[test]
fn pinned_https_target_refuses_cleartext_before_connecting() {
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    server.set_nonblocking(true).unwrap();
    let target = ValidatedHttpJsonTarget {
        addresses: vec![server.local_addr().unwrap()],
        https_only: true,
    };
    let agent = pinned_agent(&target, ureq::Agent::config_builder());
    assert!(agent
        .get("http://must-not-resolve.invalid/")
        .call()
        .is_err());
    assert_eq!(
        server.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[test]
fn migrated_http_json_rejects_oversized_content_length() {
    use medulla_types::wire::{ForeignSourceSpec, HttpFieldMap};
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/data", server.local_addr().unwrap());
    let origin = format!("http://{}/", server.local_addr().unwrap());
    let _allow = crate::federation_tests::MockHttpAllowGuard::new(&origin);
    let (finished, completion) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let (mut socket, _) = read_request(server);
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 67108865\r\nConnection: close\r\n\r\n")
            .unwrap();
        finished.send(()).unwrap();
    });
    let spec = ForeignSourceSpec::HttpJson {
        url,
        json_path: String::new(),
        field_map: HttpFieldMap {
            id: "id".into(),
            score: None,
            columns: Default::default(),
        },
    };
    let result = crate::federation::source_for(&spec).fetch();
    completion
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("local HTTP peer must finish");
    assert!(result.unwrap_err().contains("response exceeds limit"));
}
