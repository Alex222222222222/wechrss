//! Rewrites approved sanitized image URLs to stable local asset routes.
//!
//! The input is the exact output of [`crate::archive::sanitizer::sanitize`].
//! Rewriting therefore operates only on serialized `src` attributes and never
//! reparses or broadens the HTML allowlist. Unresolved URLs are left intact so
//! a best-effort asset failure cannot make an otherwise valid article vanish.

use std::collections::HashMap;

use url::Url;

/// Replaces selected sanitized image `src` values with stable local paths.
pub fn rewrite_sanitized_html(html: &str, replacements: &[(Url, String)]) -> String {
    if replacements.is_empty() || html.is_empty() {
        return html.to_owned();
    }

    let replacements = replacements
        .iter()
        .map(|(source, target)| (escape_attribute(source.as_str()), target.clone()))
        .collect::<HashMap<_, _>>();
    let mut output = String::with_capacity(html.len());
    let mut cursor = 0;
    while let Some(relative_start) = html[cursor..].find(" src=\"") {
        let attribute_start = cursor + relative_start;
        let value_start = attribute_start + " src=\"".len();
        let Some(relative_end) = html[value_start..].find('"') else {
            break;
        };
        let value_end = value_start + relative_end;
        output.push_str(&html[cursor..value_start]);
        if let Some(target) = replacements.get(&html[value_start..value_end]) {
            output.push_str(&escape_attribute(target));
        } else {
            output.push_str(&html[value_start..value_end]);
        }
        output.push('"');
        cursor = value_end + 1;
    }
    output.push_str(&html[cursor..]);
    output
}

fn escape_attribute(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            character => escaped.push(character),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_only_matching_image_sources() {
        let source = "https://cdn.example/image.jpg?size=1&mode=fit"
            .parse::<Url>()
            .expect("source URL should parse");
        let html = "<a href=\"https://cdn.example/image.jpg?size=1&mode=fit\"><img src=\"https://cdn.example/image.jpg?size=1&amp;mode=fit\" alt=\"cover\" /></a>";

        let rewritten = rewrite_sanitized_html(html, &[(source, "/assets/asset-1".to_owned())]);

        assert_eq!(
            rewritten,
            "<a href=\"https://cdn.example/image.jpg?size=1&mode=fit\"><img src=\"/assets/asset-1\" alt=\"cover\" /></a>"
        );
    }

    #[test]
    fn leaves_unresolved_and_malformed_attributes_unchanged() {
        let source = "https://cdn.example/known.png".parse::<Url>().unwrap();
        let html = "<img src=\"https://cdn.example/other.png\" /><div data-src=\"https://cdn.example/known.png\">text</div>";

        assert_eq!(
            rewrite_sanitized_html(html, &[(source, "/assets/known".to_owned())]),
            html
        );
    }
}
