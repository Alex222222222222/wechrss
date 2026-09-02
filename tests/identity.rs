//! Integration coverage for the public identity-resolution boundary.

use werrss::{
    acquisition::identity::{
        decode_biz, extract_identity_from_html, resolve_from_url, IdentityError, IdentityMethod,
    },
    domain::source::{SourceError, VerifiedWechatArticleUrl},
};

fn article_url(value: &str) -> VerifiedWechatArticleUrl {
    value.parse().expect("test URL should be valid")
}

#[test]
fn short_article_page_uses_canonical_metadata_without_network_access() {
    let source = article_url("https://mp.weixin.qq.com/s/short-token");
    let html = r#"
        <html>
          <head>
            <meta property="og:url" content="https://mp.weixin.qq.com/s/long?__biz=MTIzNDU%3D">
            <meta name="twitter:title" content="备用标题">
          </head>
        </html>
    "#;

    let identity = extract_identity_from_html(source.clone(), source, html)
        .expect("metadata parsing should succeed")
        .expect("canonical identity should be found");

    assert_eq!(identity.bid(), "12345");
    assert_eq!(identity.book_id(), "MP_WXS_12345");
    assert_eq!(identity.title(), Some("备用标题"));
    assert_eq!(identity.method(), IdentityMethod::HtmlCanonical);
}

#[test]
fn unsafe_urls_and_unusable_identity_values_are_rejected() {
    assert_eq!(
        "http://mp.weixin.qq.com/s/short?__biz=MTIzNDU%3D".parse::<VerifiedWechatArticleUrl>(),
        Err(SourceError::InvalidArticleUrl)
    );
    assert_eq!(decode_biz("bG9naW4="), Err(IdentityError::InvalidBiz));

    let source = article_url("https://mp.weixin.qq.com/s/short-token");
    assert_eq!(
        resolve_from_url(source),
        Err(IdentityError::MissingIdentity)
    );
}

#[test]
fn non_article_wechat_paths_do_not_resolve_an_identity_from_query_parameters() {
    let non_article =
        article_url("https://mp.weixin.qq.com/cgi-bin/appmsg?__biz=MTIzNDU%3D&action=list_ex");

    assert_eq!(
        resolve_from_url(non_article),
        Err(IdentityError::InvalidArticleUrl(
            SourceError::InvalidArticleUrl,
        ))
    );
}

#[test]
fn non_article_resolved_urls_are_rejected_even_with_a_valid_html_identity() {
    let source = article_url("https://mp.weixin.qq.com/s/short-token");
    let resolved = article_url("https://mp.weixin.qq.com/cgi-bin/appmsg");
    let html = r#"<script>window.biz = "MTIzNDU=";</script>"#;

    assert_eq!(
        extract_identity_from_html(source, resolved, html),
        Err(IdentityError::UnsafeRedirect)
    );
}
