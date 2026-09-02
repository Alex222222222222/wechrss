//! Minimal authenticated administration pages.
//!
//! The panel deliberately remains a small client of `/api/admin/*`: it does
//! not embed database or acquisition logic, and it never renders passwords,
//! public feed bearer tokens, or upstream credentials.

use axum::response::Html;

use crate::domain::credentials::WeReadAccount;

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
<body><main><h1>Werrss admin</h1><p>Signed in as <strong>__USERNAME__</strong></p><button id="logout">Sign out</button><h2>WeRead authentication</h2><p>Log in to <a href="https://weread.qq.com" target="_blank" rel="noreferrer">weread.qq.com</a> in a desktop browser. In Developer Tools, open the Network tab, select a request to <code>/web/mp/articles</code>, and copy the complete <code>Cookie</code> request-header value without the <code>Cookie:</code> prefix. It should include at least <code>wr_vid</code>, <code>wr_skey</code>, and <code>wr_rt</code>. Paste that value below. Tokens are encrypted for storage and are never shown again. Set the expiry to the access-session expiry shown by your authentication source. Use the existing account ID to replace credentials without changing source references. Display name is optional when the cookie contains <code>wr_name</code>; it will be percent-decoded automatically.</p><form id="weread-account"><label>Account ID (optional for a new account) <input name="account_id" type="text" autocomplete="off"></label><label>Display name (optional; defaults to wr_name) <input name="display_name" type="text" autocomplete="off"></label><label>WeRead Cookie header <textarea name="cookie_header" rows="4" required autocomplete="off"></textarea></label><label>Access token expiry <input name="access_expires_at" type="datetime-local" required></label><button>Save WeRead account</button></form><p id="account-result" role="status"></p><h2>WeRead accounts</h2><p><a href="/admin/weread/accounts">Manage all WeRead accounts</a></p><p id="account-list-error" role="alert"></p><ul id="weread-accounts"></ul><h2>Sources</h2><p id="error" role="alert"></p><ul id="sources"></ul><h2>Add source</h2><form id="create"><label>Book ID <input name="book_id" required></label><label>Name <input name="display_name" required></label><label>Article URL <input name="article_url" type="url" required></label><label>WeRead account ID (optional; blank chooses randomly) <input name="account_id" type="text"></label><button>Add source</button></form></main>
<script>
const csrf=__CSRF__;const headers={'content-type':'application/json','x-csrf-token':csrf};const list=document.querySelector('#sources');const error=document.querySelector('#error');const accountResult=document.querySelector('#account-result');const accountList=document.querySelector('#weread-accounts');const accountListError=document.querySelector('#account-list-error');
async function request(path,options={}){return fetch(path,{...options,headers:{...headers,...(options.headers||{})}})}
async function apiErrorMessage(response,fallback){try{const value=await response.json();if(typeof value.error==='string'&&value.error.trim()){return value.error}}catch{}return fallback}
async function load(){const response=await fetch('/api/admin/sources');if(!response.ok){error.textContent='Unable to load sources.';return}const sources=await response.json();list.replaceChildren(...sources.map(source=>{const item=document.createElement('li');item.textContent=`${source.display_name} (${source.book_id}) — ${source.scheduling_gate} — ${source.enabled?'enabled':'paused'} `;const pause=document.createElement('button');pause.textContent=source.enabled?'Pause':'Enable';pause.onclick=async()=>{await request(`/api/admin/sources/${source.id}/enabled`,{method:'POST',body:JSON.stringify({enabled:!source.enabled})});load()};item.append(pause);const gate=document.createElement('button');gate.textContent='Clear gate';gate.onclick=async()=>{await request(`/api/admin/sources/${source.id}/gate`,{method:'POST',body:JSON.stringify({gate:'ready'})});load()};item.append(gate);const token=document.createElement('button');token.textContent='Create feed link';token.onclick=async()=>{const result=await request(`/api/admin/sources/${source.id}/feed-token`,{method:'POST'});if(result.ok){const value=await result.json();const link=document.createElement('code');link.textContent=` ${value.feed_path}`;item.append(link)}};item.append(token);const history=document.createElement('button');history.textContent='Sync history';history.onclick=async()=>{const result=await fetch(`/api/admin/sources/${source.id}/sync-runs`);if(result.ok){const runs=await result.json();item.append(document.createTextNode(` history: ${runs.map(run=>run.outcome).join(', ')||'none'}`))}};item.append(history);return item}))}
async function loadAccounts(){const response=await fetch('/api/admin/weread/accounts');if(!response.ok){accountListError.textContent='Unable to load WeRead accounts.';return}const accounts=await response.json();accountList.replaceChildren(...accounts.map(account=>{const item=document.createElement('li');item.textContent=`${account.display_name} (${account.account_id}) — ${account.status} `;const edit=document.createElement('a');edit.href=`/admin/weread/accounts/${encodeURIComponent(account.account_id)}`;edit.textContent='Edit';item.append(edit);const enabled=document.createElement('button');enabled.textContent=account.status==='disabled'?'Enable':'Disable';enabled.onclick=async()=>{const result=await request(`/api/admin/weread/accounts/${account.account_id}/enabled`,{method:'POST',body:JSON.stringify({enabled:account.status==='disabled'})});if(!result.ok){accountListError.textContent=await apiErrorMessage(result,'Unable to change account status.');return}loadAccounts()};item.append(enabled);const remove=document.createElement('button');remove.textContent='Delete';remove.onclick=async()=>{if(!confirm(`Delete ${account.display_name}?`)){return}const result=await request(`/api/admin/weread/accounts/${account.account_id}`,{method:'DELETE'});if(!result.ok){accountListError.textContent=await apiErrorMessage(result,'Unable to delete account.');return}loadAccounts()};item.append(remove);return item}))}
document.querySelector('#create').addEventListener('submit',async event=>{event.preventDefault();const form=new FormData(event.target);const account=form.get('account_id');const response=await request('/api/admin/sources',{method:'POST',body:JSON.stringify({book_id:form.get('book_id'),display_name:form.get('display_name'),article_url:form.get('article_url'),account_id:account?account:null})});if(response.ok){event.target.reset();load()}else{error.textContent='Source could not be added.'}});
document.querySelector('#weread-account').addEventListener('submit',async event=>{event.preventDefault();accountResult.textContent='';const form=new FormData(event.target);const account=form.get('account_id');const displayName=form.get('display_name');const path=account?`/api/admin/weread/accounts/${encodeURIComponent(account)}`:'/api/admin/weread/accounts';const response=await request(path,{method:account?'PUT':'POST',body:JSON.stringify({account_id:account?account:null,display_name:displayName||null,cookie_header:form.get('cookie_header'),access_expires_at:new Date(form.get('access_expires_at')).toISOString()})});if(response.ok){const value=await response.json();event.target.reset();accountResult.textContent=`Saved account ${value.account_id}; use this ID when adding a source.`;loadAccounts()}else{accountResult.textContent=await apiErrorMessage(response,'WeRead account could not be saved; check the values and try again.')}});
document.querySelector('#logout').addEventListener('click',async()=>{await request('/api/admin/logout',{method:'POST'});location='/admin/login'});load();loadAccounts();
</script></body></html>"#;
    Html(
        template
            .replace("__USERNAME__", &username)
            .replace("__CSRF__", &csrf_json),
    )
}

/// Renders the authenticated WeRead account list page.
pub fn weread_accounts_page(session: &AdminSession) -> Html<String> {
    let username = escape_html(session.username());
    let template = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>WeRead accounts — Werrss admin</title></head>
<body><main><p><a href="/admin/">← Back to admin</a></p><h1>WeRead accounts</h1><p>Signed in as <strong>__USERNAME__</strong></p><p><a href="/admin/#weread-account">Add a WeRead account</a></p><p id="error" role="alert"></p><ul id="accounts"></ul></main>
<script>
const list=document.querySelector('#accounts');const error=document.querySelector('#error');
async function loadAccounts(){const response=await fetch('/api/admin/weread/accounts');if(!response.ok){error.textContent='Unable to load WeRead accounts.';return}const accounts=await response.json();if(!accounts.length){const empty=document.createElement('li');empty.textContent='No WeRead accounts have been added.';list.replaceChildren(empty);return}list.replaceChildren(...accounts.map(account=>{const item=document.createElement('li');item.textContent=`${account.display_name} (${account.account_id}) — ${account.status} `;const edit=document.createElement('a');edit.href=`/admin/weread/accounts/${encodeURIComponent(account.account_id)}`;edit.textContent='Manage';item.append(edit);return item}))}
loadAccounts();
</script></body></html>"#;
    Html(template.replace("__USERNAME__", &username))
}

/// Renders the account-specific credential replacement page.
pub fn weread_account_page(session: &AdminSession, account: &WeReadAccount) -> Html<String> {
    let username = escape_html(session.username());
    let csrf = escape_html(session.csrf_token());
    let csrf_json = serde_json::to_string(&csrf).expect("escaped CSRF token should serialize");
    let account_id = account.account_id().to_string();
    let display_name = escape_html(account.display_name());
    let expires_at = escape_html(&account.access_expires_at().to_rfc3339());
    let status = if account.disabled() {
        "disabled"
    } else {
        "enabled"
    };
    let template = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>WeRead account — Werrss admin</title></head>
<body><main><p><a href="/admin/">← Back to admin</a></p><h1>WeRead account</h1><p>Signed in as <strong>__USERNAME__</strong></p><dl><dt>Account ID</dt><dd><code>__ACCOUNT_ID__</code></dd><dt>Status</dt><dd>__STATUS__</dd></dl><h2>Replace credentials</h2><p>Paste a complete fresh Cookie request-header value from WeRead Developer Tools. The existing encrypted cookies are never displayed. The expiry must be the new access-session expiry. You may update the display name at the same time; leave it empty only when the cookie contains a usable <code>wr_name</code>.</p><form id="account"><label>Display name <input name="display_name" value="__DISPLAY_NAME__" required></label><label>New WeRead Cookie header <textarea name="cookie_header" rows="6" required autocomplete="off"></textarea></label><label>Access token expiry <input id="expiry" name="access_expires_at" type="datetime-local" data-value="__EXPIRES_AT__" required></label><button>Save changes</button></form><p id="result" role="status"></p><h2>Danger zone</h2><button id="toggle">__TOGGLE__ account</button> <button id="delete">Delete account</button></main>
<script>
const accountId='__ACCOUNT_ID__';const csrf=__CSRF__;const headers={'content-type':'application/json','x-csrf-token':csrf};const result=document.querySelector('#result');document.querySelector('#expiry').value=new Date(document.querySelector('#expiry').dataset.value).toISOString().slice(0,16);
const request=(path,options={})=>fetch(path,{...options,headers:{...headers,...(options.headers||{})}});
document.querySelector('#account').addEventListener('submit',async event=>{event.preventDefault();const form=new FormData(event.target);const response=await request(`/api/admin/weread/accounts/${accountId}`,{method:'PUT',body:JSON.stringify({account_id:accountId,display_name:form.get('display_name'),cookie_header:form.get('cookie_header'),access_expires_at:new Date(form.get('access_expires_at')).toISOString()})});if(response.ok){result.textContent='Account updated.';location.reload()}else{try{const body=await response.json();result.textContent=body.error||'Account could not be updated.'}catch{result.textContent='Account could not be updated.'}}});
document.querySelector('#toggle').addEventListener('click',async()=>{const response=await request(`/api/admin/weread/accounts/${accountId}/enabled`,{method:'POST',body:JSON.stringify({enabled:__ENABLED__})});if(response.ok){location.reload()}else{result.textContent='Account status could not be changed.'}});
document.querySelector('#delete').addEventListener('click',async()=>{if(!confirm('Delete this WeRead account permanently?')){return}const response=await request(`/api/admin/weread/accounts/${accountId}`,{method:'DELETE'});if(response.ok){location='/admin/'}else{result.textContent='Account could not be deleted.'}});
</script></body></html>"#;
    Html(
        template
            .replace("__USERNAME__", &username)
            .replace("__ACCOUNT_ID__", &account_id)
            .replace("__ACCOUNT_ID__", &account_id)
            .replace("__STATUS__", status)
            .replace("__DISPLAY_NAME__", &display_name)
            .replace("__EXPIRES_AT__", &expires_at)
            .replace(
                "__TOGGLE__",
                if account.disabled() {
                    "Enable"
                } else {
                    "Disable"
                },
            )
            .replace(
                "__ENABLED__",
                if account.disabled() { "true" } else { "false" },
            )
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
        assert!(body.contains("href=\"/admin/weread/accounts\""));
    }

    #[test]
    fn admin_page_surfaces_api_error_messages_with_a_safe_fallback() {
        let auth = AdminAuthenticator::new(
            "admin".to_owned(),
            SecretString::new("password".to_owned().into_boxed_str()),
            SecretString::new("signing-key".to_owned().into_boxed_str()),
        )
        .unwrap();
        let (session, _) = auth
            .login(
                "admin",
                "password",
                "test",
                "2026-09-01T00:00:00Z".parse().unwrap(),
            )
            .unwrap();
        let body = admin_page(&session).0;

        assert!(body.contains("async function apiErrorMessage(response,fallback)"));
        assert!(body.contains("typeof value.error==='string'"));
        assert!(body.contains("return fallback"));
        assert!(body.contains("accountResult.textContent=await apiErrorMessage(response,"));
        assert!(body.contains("id=\"weread-accounts\""));
        assert!(body.contains("/api/admin/weread/accounts"));
        assert!(body.contains("account.display_name"));
        assert!(body.contains("account.status"));
        assert!(body.contains("accountResult.textContent=`Saved account ${value.account_id}; use this ID when adding a source.`;loadAccounts()"));
    }

    #[test]
    fn account_page_exposes_safe_lifecycle_controls_without_credentials() {
        let auth = AdminAuthenticator::new(
            "admin".to_owned(),
            SecretString::new("password".to_owned().into_boxed_str()),
            SecretString::new("signing-key".to_owned().into_boxed_str()),
        )
        .unwrap();
        let (session, _) = auth
            .login(
                "admin",
                "password",
                "test",
                "2026-09-01T00:00:00Z".parse().unwrap(),
            )
            .unwrap();
        let account = WeReadAccount::from_parts(
            crate::domain::credentials::WeReadAccountId::from_uuid(uuid::Uuid::from_u128(1)),
            "Primary".to_owned(),
            2,
            "2026-10-01T00:00:00Z".parse().unwrap(),
            false,
        );
        let body = weread_account_page(&session, &account).0;
        assert!(body.contains("Replace credentials"));
        assert!(body.contains("/api/admin/weread/accounts/${accountId}"));
        assert!(body.contains("/api/admin/weread/accounts/${accountId}/enabled"));
        assert!(body.contains("method:'DELETE'"));
        assert!(body.contains("data-value=\"2026-10-01T00:00:00+00:00\""));
        assert!(!body.contains("access-token"));
    }

    #[test]
    fn accounts_page_lists_accounts_from_authenticated_api_without_credentials() {
        let auth = AdminAuthenticator::new(
            "admin".to_owned(),
            SecretString::new("password".to_owned().into_boxed_str()),
            SecretString::new("signing-key".to_owned().into_boxed_str()),
        )
        .unwrap();
        let (session, _) = auth
            .login(
                "admin",
                "password",
                "test",
                "2026-09-01T00:00:00Z".parse().unwrap(),
            )
            .unwrap();
        let body = weread_accounts_page(&session).0;
        assert!(body.contains("<h1>WeRead accounts</h1>"));
        assert!(body.contains("href=\"/admin/\""));
        assert!(body.contains("/api/admin/weread/accounts"));
        assert!(body.contains("/admin/weread/accounts/${encodeURIComponent(account.account_id)}"));
        assert!(!body.contains("password"));
        assert!(!body.contains("cookie_header"));
    }
}
