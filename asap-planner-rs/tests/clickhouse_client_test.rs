use std::io::{Read, Write};
use std::net::TcpListener;

use asap_planner::clickhouse_client::infer_metadata_columns;

/// Spawn a single-shot HTTP server that returns a hardcoded `system.columns`
/// response, then verify that `infer_metadata_columns` correctly excludes the
/// time column and value columns and returns the rest sorted.
///
/// Table: hits
///   EventTime       DateTime   ← excluded (time_column)
///   ResolutionWidth UInt16     ← excluded (value_column)
///   OS              UInt8      ← metadata
///   RegionID        UInt32     ← metadata
///
/// Expected result: ["OS", "RegionID"]
#[test]
fn test_infer_metadata_columns_via_mock() {
    let body = concat!(
        "{\"name\":\"EventTime\",\"type\":\"DateTime\"}\n",
        "{\"name\":\"ResolutionWidth\",\"type\":\"UInt16\"}\n",
        "{\"name\":\"OS\",\"type\":\"UInt8\"}\n",
        "{\"name\":\"RegionID\",\"type\":\"UInt32\"}\n",
    );

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0u8; 4096];
        let _ = stream.read(&mut buf);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/x-ndjson\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).unwrap();
    });

    let url = format!("http://127.0.0.1:{}", port);
    let result = infer_metadata_columns(
        &url,
        "default",
        "hits",
        "EventTime",
        &["ResolutionWidth".to_string()],
    )
    .unwrap();

    server.join().unwrap();
    assert_eq!(result, vec!["OS".to_string(), "RegionID".to_string()]);
}
