//! Integration coverage for the public archive-sanitization boundary.

use werrss::archive::sanitizer::HtmlSanitizer;

#[test]
fn sanitized_content_is_safe_to_pass_to_rss_and_reports_external_images() {
    let result = HtmlSanitizer.sanitize(
        r#"<section class="article" style="background:url(javascript:bad)"><h2>标题</h2><img data-src="https://mmbiz.qpic.cn/image.jpg" /><a href="https://mp.weixin.qq.com/s/example" target="_blank">原文</a></section>"#,
    );

    assert_eq!(
        result.html(),
        r#"<section class="article"><h2>标题</h2><img src="https://mmbiz.qpic.cn/image.jpg" /><a href="https://mp.weixin.qq.com/s/example">原文</a></section>"#
    );
    assert_eq!(result.external_assets().len(), 1);
    assert_eq!(
        result.external_assets()[0].as_str(),
        "https://mmbiz.qpic.cn/image.jpg"
    );
}
