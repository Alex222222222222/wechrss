//! Minimal authenticated administration pages.
//!
//! The panel deliberately remains a small client of `/api/admin/*`: it does
//! not embed database or acquisition logic, and it never renders passwords,
//! public feed bearer tokens, or upstream credentials.

use axum::response::Html;

use super::auth::AdminSession;

/// Renders the public login page. Credentials are submitted to the JSON API.
pub fn login_page() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Werrss admin login</title></head>
<body><main><h1>Werrss admin</h1><form id="login"><label>Username <input name="username" autocomplete="username" required></label><label>Password <input name="password" type="password" autocomplete="current-password" required></label><button>Sign in</button><p id="error" role="alert"></p></form></main>
<script>document.querySelector('#login').addEventListener('submit',async event=>{event.preventDefault();const form=new FormData(event.target);const response=await fetch('/api/admin/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({username:form.get('username'),password:form.get('password')})});if(response.ok){location='/admin'}else{document.querySelector('#error').textContent='Sign-in failed; check the credentials or try again later.'}});</script></body></html>"#,
    )
}

/// Renders the authenticated source-management panel.
pub fn admin_page(session: &AdminSession) -> Html<String> {
    let username = escape_html(session.username());
    let csrf = escape_html(session.csrf_token());
    let csrf_json = serde_json::to_string(&csrf).expect("escaped CSRF token should serialize");
    let template = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Werrss admin</title></head>
<body><main><h1>Werrss admin</h1><p>Signed in as <strong>__USERNAME__</strong></p><button id="logout">Sign out</button><h2>WeRead authentication</h2><p>Log in to <a href="https://weread.qq.com" target="_blank" rel="noreferrer">weread.qq.com</a> in a desktop browser. In Developer Tools, open the Network tab, select a request to <code>/web/mp/articles</code>, and copy the complete <code>Cookie</code> request-header value without the <code>Cookie:</code> prefix. It should include at least <code>wr_vid</code>, <code>wr_skey</code>, and <code>wr_rt</code>. Paste that value below. Tokens are encrypted for storage and are never shown again. Set the expiry to the access-session expiry shown by your authentication source. Use the existing account ID to replace credentials without changing source references.</p><form id="weread-account"><label>Account ID (optional for a new account) <input name="account_id" type="text" autocomplete="off"></label><label>Display name <input name="display_name" required autocomplete="off"></label><label>WeRead Cookie header <textarea name="cookie_header" rows="4" required autocomplete="off"></textarea></label><label>Access token expiry <input name="access_expires_at" type="datetime-local" required></label><button>Save WeRead account</button></form><p id="account-result" role="status"></p><h2>Sources</h2><p id="error" role="alert"></p><ul id="sources"></ul><h2>Add source</h2><form id="create"><label>Book ID <input name="book_id" required></label><label>Name <input name="display_name" required></label><label>Article URL <input name="article_url" type="url" required></label><label>WeRead account ID <input name="account_id" type="text"></label><button>Add source</button></form></main>
<script>
const csrf=__CSRF__;const headers={'content-type':'application/json','x-csrf-token':csrf};const list=document.querySelector('#sources');const error=document.querySelector('#error');const accountResult=document.querySelector('#account-result');
async function request(path,options={}){return fetch(path,{...options,headers:{...headers,...(options.headers||{})}})}
async function load(){const response=await fetch('/api/admin/sources');if(!response.ok){error.textContent='Unable to load sources.';return}const sources=await response.json();list.replaceChildren(...sources.map(source=>{const item=document.createElement('li');item.textContent=`${source.display_name} (${source.book_id}) — ${source.scheduling_gate} — ${source.enabled?'enabled':'paused'} `;const pause=document.createElement('button');pause.textContent=source.enabled?'Pause':'Enable';pause.onclick=async()=>{await request(`/api/admin/sources/${source.id}/enabled`,{method:'POST',body:JSON.stringify({enabled:!source.enabled})});load()};item.append(pause);const gate=document.createElement('button');gate.textContent='Clear gate';gate.onclick=async()=>{await request(`/api/admin/sources/${source.id}/gate`,{method:'POST',body:JSON.stringify({gate:'ready'})});load()};item.append(gate);const token=document.createElement('button');token.textContent='Create feed link';token.onclick=async()=>{const result=await request(`/api/admin/sources/${source.id}/feed-token`,{method:'POST'});if(result.ok){const value=await result.json();const link=document.createElement('code');link.textContent=` ${value.feed_path}`;item.append(link)}};item.append(token);const history=document.createElement('button');history.textContent='Sync history';history.onclick=async()=>{const result=await fetch(`/api/admin/sources/${source.id}/sync-runs`);if(result.ok){const runs=await result.json();item.append(document.createTextNode(` history: ${runs.map(run=>run.outcome).join(', ')||'none'}`))}};item.append(history);return item}))}
document.querySelector('#create').addEventListener('submit',async event=>{event.preventDefault();const form=new FormData(event.target);const account=form.get('account_id');const response=await request('/api/admin/sources',{method:'POST',body:JSON.stringify({book_id:form.get('book_id'),display_name:form.get('display_name'),article_url:form.get('article_url'),account_id:account?account:null})});if(response.ok){event.target.reset();load()}else{error.textContent='Source could not be added.'}});
document.querySelector('#weread-account').addEventListener('submit',async event=>{event.preventDefault();accountResult.textContent='';const form=new FormData(event.target);const account=form.get('account_id');const path=account?`/api/admin/weread/accounts/${encodeURIComponent(account)}`:'/api/admin/weread/accounts';const response=await request(path,{method:account?'PUT':'POST',body:JSON.stringify({account_id:account?account:null,display_name:form.get('display_name'),cookie_header:form.get('cookie_header'),access_expires_at:new Date(form.get('access_expires_at')).toISOString()})});if(response.ok){const value=await response.json();event.target.reset();accountResult.textContent=`Saved account ${value.account_id}; use this ID when adding a source.`}else{accountResult.textContent='WeRead account could not be saved; check the values and try again.'}});
document.querySelector('#logout').addEventListener('click',async()=>{await request('/api/admin/logout',{method:'POST'});location='/admin/login'});load();
</script></body></html>"#;
    Html(
        template
            .replace("__USERNAME__", &username)
            .replace("__CSRF__", &csrf_json),
    )
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::web::auth::AdminAuthenticator;
    use secrecy::SecretString;

    #[test]
    fn admin_page_escapes_identity_and_embeds_csrf_only() {
        let auth = AdminAuthenticator::new(
            "<admin>".to_owned(),
            SecretString::new("password".to_owned().into_boxed_str()),
            SecretString::new("signing-key".to_owned().into_boxed_str()),
        )
        .unwrap();
        let (session, _) = auth
            .login(
                "<admin>",
                "password",
                "test",
                "2026-09-01T00:00:00Z".parse().unwrap(),
            )
            .unwrap();
        let body = admin_page(&session).0;
        assert!(body.contains("&lt;admin&gt;"));
        assert!(!body.contains("correct horse"));
        assert!(body.contains("cookie_header"));
        assert!(body.contains("/web/mp/articles"));
    }
}
