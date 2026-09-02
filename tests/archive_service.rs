//! Integration coverage for the article archive application boundary.

use sha2::{Digest, Sha256};
use werrss::application::archive_service::ArchiveService;

#[test]
fn archive_service_returns_persistable_content_and_a_matching_hash() {
    let archived = ArchiveService::default().archive(
        r#"<article><h1>标题</h1><p style="color:red">正文</p><img data-original="https://cdn.example.test/a.jpg"><script>bad()</script></article>"#,
    );

    let expected_html = r#"<article><h1>标题</h1><p>正文</p><img src="https://cdn.example.test/a.jpg" /></article>"#;
    let expected_hash = format!("{:x}", Sha256::digest(expected_html.as_bytes()));

    assert_eq!(archived.html(), expected_html);
    assert_eq!(archived.content_hash(), Some(expected_hash.as_str()));
    assert_eq!(archived.external_assets().len(), 1);
}
