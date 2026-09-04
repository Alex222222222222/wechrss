//! Server-rendered administration pages.
//!
//! The panel deliberately remains a small client of `/api/admin/*`: it does
//! not embed database or acquisition logic, and it never renders passwords,
//! public feed bearer tokens, or upstream credentials. The styles are kept
//! inline so the administrator can use the panel in a minimal deployment
//! without an additional static-file server or frontend build step.

use axum::response::Html;

use crate::domain::{credentials::WeReadAccount, source::Source};

use super::auth::AdminSession;

const STYLES: &str = r##"<style>
:root {
  color-scheme: light;
  --ink: #172033;
  --muted: #667085;
  --subtle: #98a2b3;
  --line: #e4e7ec;
  --surface: #ffffff;
  --surface-soft: #f8fafc;
  --canvas: #f3f6fb;
  --brand: #3855d8;
  --brand-dark: #2f45b6;
  --brand-soft: #eef1ff;
  --success: #087443;
  --success-soft: #e8f7ef;
  --warning: #b54708;
  --warning-soft: #fff5e8;
  --danger: #b42318;
  --danger-soft: #fff0ee;
  --shadow: 0 18px 45px rgba(16, 24, 40, .08);
  --radius-lg: 20px;
  --radius-md: 12px;
  --radius-sm: 8px;
}

* { box-sizing: border-box; }
html { min-width: 320px; scroll-behavior: smooth; }
body {
  min-height: 100vh;
  margin: 0;
  color: var(--ink);
  background:
    radial-gradient(circle at 8% 0%, rgba(83, 111, 232, .12), transparent 34rem),
    var(--canvas);
  font: 15px/1.55 Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

a { color: var(--brand-dark); text-decoration: none; }
a:hover { text-decoration: underline; }
button, input, textarea { font: inherit; }
button, .button {
  display: inline-flex;
  min-height: 40px;
  align-items: center;
  justify-content: center;
  gap: 8px;
  border: 1px solid transparent;
  border-radius: var(--radius-sm);
  padding: 9px 14px;
  color: var(--ink);
  background: var(--surface);
  font-weight: 650;
  line-height: 1.2;
  cursor: pointer;
  transition: border-color .16s ease, background .16s ease, box-shadow .16s ease, transform .16s ease;
}
button:hover, .button:hover { text-decoration: none; transform: translateY(-1px); }
button:focus-visible, .button:focus-visible, a:focus-visible, input:focus-visible, textarea:focus-visible {
  outline: 3px solid rgba(56, 85, 216, .24);
  outline-offset: 2px;
}
button:disabled, .button[aria-disabled="true"] { cursor: not-allowed; opacity: .58; transform: none; }
.button-primary { color: #fff; background: var(--brand); box-shadow: 0 5px 14px rgba(56, 85, 216, .22); }
.button-primary:hover { background: var(--brand-dark); }
.button-secondary { border-color: #cdd5ff; color: var(--brand-dark); background: var(--brand-soft); }
.button-secondary:hover { border-color: #aebaff; background: #e5e9ff; }
.button-quiet { border-color: var(--line); background: var(--surface); }
.button-quiet:hover { border-color: #c5cad4; background: var(--surface-soft); }
.button-danger { border-color: #f1b8b2; color: var(--danger); background: var(--danger-soft); }
.button-danger:hover { border-color: #e9958c; background: #ffe5e1; }
.button-link { min-height: auto; border: 0; padding: 0; color: var(--brand-dark); background: transparent; }
.button-link:hover { transform: none; }

.site-header {
  border-bottom: 1px solid rgba(228, 231, 236, .84);
  background: rgba(255, 255, 255, .84);
  backdrop-filter: blur(16px);
}
.header-inner, .page-shell { width: min(1160px, calc(100% - 40px)); margin: 0 auto; }
.header-inner { min-height: 72px; display: flex; align-items: center; justify-content: space-between; gap: 20px; }
.brand { display: inline-flex; align-items: center; gap: 11px; color: var(--ink); }
.brand:hover { text-decoration: none; }
.brand-mark {
  display: grid;
  width: 37px;
  height: 37px;
  place-items: center;
  border-radius: 11px;
  color: #fff;
  background: linear-gradient(135deg, #6078ed, #324dcc);
  box-shadow: 0 7px 14px rgba(56, 85, 216, .24);
  font-size: 18px;
  font-weight: 800;
}
.brand-copy { display: grid; gap: 0; font-weight: 780; letter-spacing: -.02em; }
.brand-copy small { color: var(--muted); font-size: 11px; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }
.header-actions { display: flex; align-items: center; gap: 14px; }
.identity { display: inline-flex; align-items: center; gap: 7px; color: var(--muted); font-size: 13px; }
.identity::before { width: 7px; height: 7px; border-radius: 50%; background: #12b76a; content: ""; }

.page-shell { padding: 42px 0 72px; }
.page-header { display: flex; align-items: flex-end; justify-content: space-between; gap: 28px; margin-bottom: 30px; }
.page-header h1 { margin: 5px 0 8px; color: var(--ink); font-size: clamp(28px, 4vw, 38px); letter-spacing: -.045em; line-height: 1.1; }
.page-header p { max-width: 680px; margin: 0; color: var(--muted); }
.page-header-actions, .form-actions, .resource-actions { display: flex; flex-wrap: wrap; align-items: center; gap: 9px; }
.page-header-actions { justify-content: flex-end; }
.kicker { margin: 0; color: var(--brand); font-size: 11px; font-weight: 800; letter-spacing: .13em; text-transform: uppercase; }
.eyebrow { color: var(--muted); font-size: 13px; }
.muted { color: var(--muted); }
.small { color: var(--muted); font-size: 12px; }

.card {
  border: 1px solid rgba(228, 231, 236, .95);
  border-radius: var(--radius-lg);
  padding: 24px;
  background: var(--surface);
  box-shadow: var(--shadow);
}
.card-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 18px; margin-bottom: 20px; }
.card-header h2, .card-header h3 { margin: 0 0 5px; color: var(--ink); font-size: 18px; letter-spacing: -.02em; }
.card-header p { margin: 0; color: var(--muted); font-size: 13px; }
.card-icon { display: grid; width: 38px; height: 38px; flex: 0 0 auto; place-items: center; border-radius: 11px; color: var(--brand-dark); background: var(--brand-soft); font-weight: 800; }
.card-icon.green { color: var(--success); background: var(--success-soft); }
.card-icon.orange { color: var(--warning); background: var(--warning-soft); }
.card-icon.red { color: var(--danger); background: var(--danger-soft); }
.layout-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 20px; }
.full-width { grid-column: 1 / -1; }
.stats { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; margin-bottom: 20px; }
.stat { display: flex; align-items: center; gap: 14px; border: 1px solid var(--line); border-radius: var(--radius-md); padding: 17px; background: rgba(255, 255, 255, .7); }
.stat-value { display: block; color: var(--ink); font-size: 25px; font-weight: 780; letter-spacing: -.04em; line-height: 1.1; }
.stat-label { display: block; margin-top: 4px; color: var(--muted); font-size: 12px; }
.stat-icon { display: grid; width: 35px; height: 35px; flex: 0 0 auto; place-items: center; border-radius: 10px; color: var(--brand); background: var(--brand-soft); font-size: 13px; font-weight: 800; }
.stat-icon.green { color: var(--success); background: var(--success-soft); }
.stat-icon.orange { color: var(--warning); background: var(--warning-soft); }

.notice { display: flex; gap: 11px; border-radius: var(--radius-md); margin: 0 0 20px; padding: 13px 14px; color: var(--muted); background: var(--surface-soft); font-size: 13px; }
.notice strong { color: var(--ink); }
.notice.info { border: 1px solid #d8defe; color: #4352a4; background: var(--brand-soft); }
.notice.warning { border: 1px solid #f9d8a9; color: #8d480d; background: var(--warning-soft); }
.notice-icon { font-weight: 800; }
.feedback { border-radius: var(--radius-sm); margin: 14px 0 0; padding: 10px 12px; font-size: 13px; }
.feedback[hidden] { display: none; }
.feedback.error { color: var(--danger); background: var(--danger-soft); }
.feedback.success { color: var(--success); background: var(--success-soft); }
.feedback.info { color: #4352a4; background: var(--brand-soft); }

form { display: grid; gap: 17px; }
.field-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 17px 15px; }
.field-grid .wide { grid-column: 1 / -1; }
.field-grid .wide > .small { display: block; margin-top: -3px; }
label { display: grid; gap: 7px; color: var(--ink); font-size: 13px; font-weight: 650; }
.label-row { display: flex; align-items: baseline; justify-content: space-between; gap: 10px; }
.label-hint { color: var(--subtle); font-size: 11px; font-weight: 500; }
input, textarea {
  width: 100%;
  border: 1px solid #d0d5dd;
  border-radius: var(--radius-sm);
  padding: 10px 11px;
  color: var(--ink);
  background: #fff;
  transition: border-color .16s ease, box-shadow .16s ease;
}
input { min-height: 42px; }
textarea { min-height: 106px; resize: vertical; }
input::placeholder, textarea::placeholder { color: #98a2b3; }
input:hover, textarea:hover { border-color: #98a2b3; }
input:focus, textarea:focus { border-color: var(--brand); box-shadow: 0 0 0 3px rgba(56, 85, 216, .12); outline: 0; }
fieldset { min-width: 0; border: 0; margin: 0; padding: 0; }
legend { margin-bottom: 15px; color: var(--ink); font-size: 14px; font-weight: 760; }
.form-section + .form-section { border-top: 1px solid var(--line); padding-top: 21px; }
.form-actions { border-top: 1px solid var(--line); padding-top: 18px; }
.form-actions .button-primary { margin-left: auto; }

.resource-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 13px; }
.resource-card { border: 1px solid var(--line); border-radius: var(--radius-md); padding: 16px; background: var(--surface); }
.resource-card:hover { border-color: #c6cff8; box-shadow: 0 7px 20px rgba(16, 24, 40, .06); }
.resource-card-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; margin-bottom: 12px; }
.resource-card h3 { overflow: hidden; margin: 0; color: var(--ink); font-size: 15px; text-overflow: ellipsis; white-space: nowrap; }
.resource-card p { margin: 0; }
.resource-meta { display: grid; gap: 5px; margin: 0 0 15px; }
.resource-meta-row { display: flex; align-items: baseline; justify-content: space-between; gap: 10px; color: var(--muted); font-size: 12px; }
.resource-meta-row strong { overflow: hidden; color: var(--ink); font-size: 12px; font-weight: 600; text-align: right; text-overflow: ellipsis; white-space: nowrap; }
.resource-actions { border-top: 1px solid var(--line); padding-top: 13px; }
.resource-actions .button { min-height: 35px; padding: 7px 10px; font-size: 12px; }
.resource-actions .button-link { padding: 7px 2px; }
.resource-actions .spacer { flex: 1; }
.feed-result { overflow-wrap: anywhere; margin-top: 11px !important; font-size: 12px; }
.feed-result a { font-weight: 650; }
details.history { border-top: 1px solid var(--line); margin-top: 13px; padding-top: 11px; color: var(--muted); font-size: 12px; }
details.history summary { color: var(--brand-dark); cursor: pointer; font-weight: 650; }
details.history ul { display: grid; gap: 4px; margin: 9px 0 0; padding-left: 18px; }
.status-chip { display: inline-flex; flex: 0 0 auto; align-items: center; gap: 6px; border-radius: 999px; padding: 4px 9px; color: var(--muted); background: var(--surface-soft); font-size: 11px; font-weight: 750; letter-spacing: .02em; text-transform: capitalize; }
.status-chip::before { width: 6px; height: 6px; border-radius: 50%; background: var(--subtle); content: ""; }
.status-chip.enabled, .status-chip.active, .status-chip.ready { color: var(--success); background: var(--success-soft); }
.status-chip.enabled::before, .status-chip.active::before, .status-chip.ready::before { background: #12b76a; }
.status-chip.paused, .status-chip.disabled { color: var(--muted); background: #f2f4f7; }
.status-chip.warning, .status-chip.expired, .status-chip.blocked { color: var(--warning); background: var(--warning-soft); }
.status-chip.warning::before, .status-chip.expired::before, .status-chip.blocked::before { background: #f79009; }
.status-chip.error { color: var(--danger); background: var(--danger-soft); }
.status-chip.error::before { background: #f04438; }
.loading-state, .empty-state, .error-state { grid-column: 1 / -1; border: 1px dashed #cdd5df; border-radius: var(--radius-md); padding: 28px 18px; color: var(--muted); text-align: center; background: var(--surface-soft); }
.empty-state strong { display: block; margin-bottom: 4px; color: var(--ink); }
.error-state { border-color: #f1b8b2; color: var(--danger); background: var(--danger-soft); }
code { border-radius: 5px; padding: 2px 5px; color: #344054; background: #f2f4f7; font-size: .9em; }
.definition-list { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 13px; margin: 0; }
.definition-list div { border: 1px solid var(--line); border-radius: var(--radius-sm); padding: 12px; background: var(--surface-soft); }
.definition-list dt { color: var(--muted); font-size: 11px; font-weight: 700; letter-spacing: .04em; text-transform: uppercase; }
.definition-list dd { margin: 4px 0 0; color: var(--ink); font-weight: 700; }
.danger-zone { border-color: #f4d1cd; box-shadow: none; }
.danger-zone .card-header h2 { color: var(--danger); }
.breadcrumb { display: flex; flex-wrap: wrap; align-items: center; gap: 7px; margin-bottom: 23px; color: var(--muted); font-size: 13px; }
.breadcrumb span { color: var(--subtle); }
.split-panel { display: grid; grid-template-columns: minmax(0, 1.35fr) minmax(260px, .65fr); gap: 20px; align-items: start; }
.side-stack { display: grid; gap: 20px; }

.auth-page { display: grid; min-height: 100vh; place-items: center; padding: 28px 20px; }
.auth-layout { display: grid; grid-template-columns: minmax(0, 420px) minmax(230px, 290px); gap: 22px; align-items: stretch; width: min(760px, 100%); }
.auth-card { border: 1px solid rgba(228, 231, 236, .95); border-radius: 24px; padding: clamp(25px, 6vw, 42px); background: rgba(255, 255, 255, .94); box-shadow: var(--shadow); }
.auth-card .brand { margin-bottom: 40px; }
.auth-intro h1 { margin: 7px 0 9px; color: var(--ink); font-size: 30px; letter-spacing: -.045em; line-height: 1.1; }
.auth-intro p { margin: 0 0 27px; color: var(--muted); }
.auth-card .form-actions { margin-top: 5px; }
.auth-card .form-actions .button-primary { width: 100%; margin: 0; }
.auth-note { display: flex; flex-direction: column; justify-content: flex-end; border-radius: var(--radius-lg); padding: 23px; color: #dbe3ff; background: linear-gradient(155deg, #263b9f, #18265f); box-shadow: var(--shadow); }
.auth-note strong { display: block; margin-bottom: 6px; color: #fff; font-size: 16px; }
.auth-note p { margin: 0; color: #bfcaff; font-size: 13px; }
.auth-note ul { display: grid; gap: 10px; margin: 18px 0 0; padding: 0; list-style: none; color: #dbe3ff; font-size: 13px; }
.auth-note li { display: flex; gap: 8px; }
.auth-note li::before { color: #9eafff; content: "✓"; font-weight: 800; }

@media (max-width: 840px) {
  .layout-grid, .split-panel, .auth-layout { grid-template-columns: 1fr; }
  .auth-note { min-height: 170px; }
}
@media (max-width: 620px) {
  .header-inner, .page-shell { width: min(100% - 28px, 1160px); }
  .header-inner { min-height: 62px; }
  .identity { display: none; }
  .page-shell { padding: 27px 0 48px; }
  .page-header { display: grid; align-items: start; gap: 16px; margin-bottom: 22px; }
  .page-header-actions { justify-content: flex-start; }
  .stats, .resource-grid, .field-grid, .definition-list { grid-template-columns: 1fr; }
  .full-width, .field-grid .wide { grid-column: auto; }
  .card { padding: 18px; border-radius: 16px; }
  .form-actions .button-primary { width: 100%; margin-left: 0; }
  .auth-page { padding: 14px; }
  .auth-card { padding: 25px 20px; }
  .auth-card .brand { margin-bottom: 32px; }
  .auth-note { display: none; }
}
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { scroll-behavior: auto !important; transition-duration: .01ms !important; animation-duration: .01ms !important; }
}
</style>"##;

/// Renders the public login page. Credentials are submitted to the JSON API.
pub fn login_page() -> Html<String> {
    let template = r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Sign in — Werrss admin</title>__STYLES__</head>
<body class="auth-page"><main class="auth-layout"><section class="auth-card" aria-labelledby="login-title"><a class="brand" href="/admin/login"><span class="brand-mark" aria-hidden="true">W</span><span class="brand-copy">Werrss<small>Admin console</small></span></a><div class="auth-intro"><p class="kicker">Private reader</p><h1 id="login-title">Welcome back</h1><p>Sign in to manage your sources, credentials, and feed links.</p></div><form id="login"><label><span>Username</span><input name="username" autocomplete="username" required autofocus></label><label><span>Password</span><input name="password" type="password" autocomplete="current-password" required></label><div class="form-actions"><button class="button-primary" type="submit"><span>Sign in</span></button></div><p id="error" class="feedback error" role="alert" hidden></p></form></section><aside class="auth-note" aria-label="About the admin console"><strong>A quiet place to manage your feeds.</strong><p>Everything here is designed for one trusted administrator.</p><ul><li>Protected source and account controls</li><li>Copyable RSS feed links</li><li>No secrets displayed after saving</li></ul></aside></main>
<script>
const loginForm=document.querySelector('#login');const loginError=document.querySelector('#error');const loginButton=loginForm.querySelector('button');
loginForm.addEventListener('submit',async event=>{event.preventDefault();loginError.hidden=true;loginButton.disabled=true;loginButton.setAttribute('aria-busy','true');const form=new FormData(event.target);try{const response=await fetch('/api/admin/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({username:form.get('username'),password:form.get('password')})});if(response.ok){location='/admin/';return}loginError.textContent='Sign-in failed; check the credentials or try again later.';loginError.hidden=false}catch{loginError.textContent='The admin service could not be reached. Check the connection and try again.';loginError.hidden=false}finally{loginButton.disabled=false;loginButton.removeAttribute('aria-busy')}});
</script></body></html>"##;
    Html(template.replace("__STYLES__", STYLES))
}

/// Renders the authenticated source-management panel.
pub fn admin_page(session: &AdminSession) -> Html<String> {
    let username = escape_html(session.username());
    let csrf = escape_html(session.csrf_token());
    let csrf_json = serde_json::to_string(&csrf).expect("escaped CSRF token should serialize");
    let template = r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Dashboard — Werrss admin</title>__STYLES__</head>
<body><header class="site-header"><div class="header-inner"><a class="brand" href="/admin/"><span class="brand-mark" aria-hidden="true">W</span><span class="brand-copy">Werrss<small>Admin console</small></span></a><div class="header-actions"><span class="identity">Signed in as <strong>__USERNAME__</strong></span><button id="logout" class="button button-quiet" type="button">Sign out</button></div></div></header><main class="page-shell"><section class="page-header"><div><p class="kicker">Workspace overview</p><h1>Good to see you.</h1><p>Keep your feeds healthy, credentials current, and delivery links close at hand.</p></div><div class="page-header-actions"><a class="button button-secondary" href="#create">Add source</a><a class="button button-quiet" href="/admin/weread/accounts">Manage accounts</a></div></section><section class="stats" aria-label="Workspace summary"><div class="stat"><span class="stat-icon">S</span><span><strong id="source-count" class="stat-value">—</strong><span class="stat-label">Sources</span></span></div><div class="stat"><span class="stat-icon green">A</span><span><strong id="active-source-count" class="stat-value">—</strong><span class="stat-label">Active sources</span></span></div><div class="stat"><span class="stat-icon orange">W</span><span><strong id="account-count" class="stat-value">—</strong><span class="stat-label">WeRead accounts</span></span></div></section><div class="layout-grid"><section class="card" id="weread-account-card"><div class="card-header"><div><h2>Connect WeRead</h2><p>Enroll a browser session for authenticated source sync.</p></div><span class="card-icon green" aria-hidden="true">W</span></div><div class="notice info"><span class="notice-icon" aria-hidden="true">i</span><span>Cookies are encrypted before storage and are never shown again. You can replace them later from the account page.</span></div><form id="weread-account"><label><span class="label-row"><span>Account ID</span><span class="label-hint">Optional for a new account</span></span><input name="account_id" type="text" autocomplete="off" placeholder="Leave blank to create an ID"></label><label><span class="label-row"><span>Display name</span><span class="label-hint">Optional when wr_name is present</span></span><input name="display_name" type="text" autocomplete="off" placeholder="e.g. Personal account"></label><label><span>WeRead Cookie header</span><textarea name="cookie_header" rows="4" required autocomplete="off" placeholder="wr_vid=…; wr_skey=…; wr_rt=…"></textarea></label><label><span>Access token expiry</span><input name="access_expires_at" type="datetime-local" required></label><div class="form-actions"><button class="button-primary" type="submit">Save account</button></div></form><p id="account-result" class="feedback" role="status" hidden></p></section><section class="card" id="create"><div class="card-header"><div><h2>Add a source</h2><p>Start with a Book ID or resolve one from an article URL.</p></div><span class="card-icon" aria-hidden="true">+</span></div><div class="notice"><span class="notice-icon" aria-hidden="true">↗</span><span>Leave the account ID blank to let each sync choose a random enabled account.</span></div><form id="source-create"><label><span class="label-row"><span>Book ID</span><span class="label-hint">Optional with an article URL</span></span><input name="book_id" type="text" autocomplete="off" placeholder="e.g. MP_WXS_2103095721"></label><label><span>Name</span><input name="display_name" type="text" autocomplete="off" placeholder="Defaults to the resolved account name"></label><label><span>Article URL</span><input name="article_url" type="url" autocomplete="url" placeholder="https://mp.weixin.qq.com/s/…"></label><label><span class="label-row"><span>WeRead account ID</span><span class="label-hint">Optional</span></span><input name="account_id" type="text" autocomplete="off" placeholder="Pin this source to one account"></label><div class="form-actions"><button class="button-primary" type="submit">Add source</button></div></form><p id="error" class="feedback" role="alert" hidden></p></section><section class="card full-width" id="source-list"><div class="card-header"><div><h2>Sources</h2><p>Monitor scheduling gates and create public feed links.</p></div><a class="button button-quiet" href="#create">New source</a></div><div id="sources" class="resource-grid" aria-live="polite" aria-busy="true"><div class="loading-state">Loading sources…</div></div></section><section class="card full-width" id="account-list"><div class="card-header"><div><h2>WeRead accounts</h2><p>Enabled accounts are available to unbound source-sync jobs.</p></div><a class="button button-quiet" href="/admin/weread/accounts">View all</a></div><div id="weread-accounts" class="resource-grid" aria-live="polite" aria-busy="true"><div class="loading-state">Loading accounts…</div></div><p id="account-list-error" class="feedback" role="alert" hidden></p></section></div></main>
<script>
const csrf=__CSRF__;const headers={'content-type':'application/json','x-csrf-token':csrf};const list=document.querySelector('#sources');const error=document.querySelector('#error');const accountResult=document.querySelector('#account-result');const accountList=document.querySelector('#weread-accounts');const accountListError=document.querySelector('#account-list-error');const sourceCount=document.querySelector('#source-count');const activeSourceCount=document.querySelector('#active-source-count');const accountCount=document.querySelector('#account-count');
async function request(path,options={}){return fetch(path,{...options,headers:{...headers,...(options.headers||{})}})}
async function apiErrorMessage(response,fallback){try{const value=await response.json();if(typeof value.error==='string'&&value.error.trim()){return value.error}}catch{}return fallback}
function feedback(target,message,kind='error'){target.textContent=message;target.className=`feedback ${kind}`;target.hidden=!message}
function stateMessage(message,kind='loading'){const item=document.createElement('div');item.className=`${kind}-state`;item.textContent=message;return item}
function button(label,kind,handler){const item=document.createElement('button');item.type='button';item.className=`button ${kind}`;item.textContent=label;item.addEventListener('click',handler);return item}
function chip(label,kind){const item=document.createElement('span');item.className=`status-chip ${kind}`;item.textContent=label;return item}
function meta(label,value){const row=document.createElement('div');row.className='resource-meta-row';const name=document.createElement('span');name.textContent=label;const content=document.createElement('strong');content.textContent=value;row.append(name,content);return row}
function renderSource(source){
  const item=document.createElement('article');item.className='resource-card';
  const header=document.createElement('div');header.className='resource-card-header';
  const title=document.createElement('h3');title.textContent=source.display_name;
  header.append(title,chip(source.enabled?'Enabled':'Paused',source.enabled?'enabled':'paused'));item.append(header);
  const details=document.createElement('div');details.className='resource-meta';details.append(meta('Book ID',source.book_id),meta('Scheduling',source.scheduling_gate));item.append(details);
  const actions=document.createElement('div');actions.className='resource-actions';
  const edit=document.createElement('a');edit.className='button button-quiet';edit.href=`/admin/sources/${encodeURIComponent(source.id)}`;edit.textContent='Edit';actions.append(edit);
  const mutate=async(action,fallback)=>{try{const response=await action();if(!response.ok){feedback(error,await apiErrorMessage(response,fallback));return null}return response}catch{feedback(error,'The admin service could not be reached. Try again.');return null}};
  const toggle=button(source.enabled?'Pause':'Enable','button-quiet',async()=>{toggle.disabled=true;const response=await mutate(()=>request(`/api/admin/sources/${source.id}/enabled`,{method:'POST',body:JSON.stringify({enabled:!source.enabled})}),'Unable to change source status.');if(response){await load()}toggle.disabled=false});actions.append(toggle);
  const gate=button(source.scheduling_gate==='ready'?'Gate ready':'Clear gate','button-quiet',async()=>{gate.disabled=true;const response=await mutate(()=>request(`/api/admin/sources/${source.id}/gate`,{method:'POST',body:JSON.stringify({gate:'ready'})}),'Unable to clear source gate.');if(response){await load()}gate.disabled=false});gate.disabled=source.scheduling_gate==='ready';actions.append(gate);
  const spacer=document.createElement('span');spacer.className='spacer';actions.append(spacer);
  const token=button('Create feed link','button-secondary',async()=>{token.disabled=true;feedback(error,'');const response=await mutate(()=>request(`/api/admin/sources/${source.id}/feed-token`,{method:'POST'}),'Unable to create a feed link.');if(response){try{const value=await response.json();const href=value.feed_url||value.feed_path;if(!href){throw new Error('missing feed URL')}const link=document.createElement('a');link.href=href;link.textContent=href;link.target='_blank';link.rel='noreferrer';feedResult.replaceChildren(link);feedResult.hidden=false}catch{feedback(error,'The feed link response was invalid.')}}token.disabled=false});actions.append(token);item.append(actions);
  const feedResult=document.createElement('p');feedResult.className='feedback success feed-result';feedResult.hidden=true;item.append(feedResult);
  const history=document.createElement('details');history.className='history';const summary=document.createElement('summary');summary.textContent='Show sync history';history.append(summary);const historyList=document.createElement('ul');history.append(historyList);
  history.addEventListener('toggle',async()=>{if(!history.open||history.dataset.loaded){return}history.dataset.loaded='true';const message=document.createElement('li');message.textContent='Loading history…';historyList.replaceChildren(message);try{const response=await fetch(`/api/admin/sources/${source.id}/sync-runs`);if(!response.ok){throw new Error('history request failed')}const runs=await response.json();historyList.replaceChildren();if(!runs.length){const empty=document.createElement('li');empty.textContent='No synchronization runs yet.';historyList.append(empty);return}runs.forEach(run=>{const runMessage=document.createElement('li');runMessage.textContent=run.outcome;historyList.append(runMessage)})}catch{historyList.replaceChildren();const failure=document.createElement('li');failure.textContent='History is temporarily unavailable.';historyList.append(failure)}});item.append(history);return item}
async function load(){list.setAttribute('aria-busy','true');list.replaceChildren(stateMessage('Loading sources…'));try{const response=await fetch('/api/admin/sources');if(!response.ok){throw new Error('source list failed')}const sources=await response.json();sourceCount.textContent=sources.length;activeSourceCount.textContent=sources.filter(source=>source.enabled).length;if(!sources.length){list.replaceChildren(stateMessage('No sources yet.','empty'));const strong=document.createElement('strong');strong.textContent='Your first feed is one step away.';list.firstChild.prepend(strong)}else{list.replaceChildren(...sources.map(renderSource))}feedback(error,'')}catch{sourceCount.textContent='—';activeSourceCount.textContent='—';list.replaceChildren(stateMessage('Sources could not be loaded. Refresh and try again.','error'));feedback(error,'Unable to load sources.')}finally{list.setAttribute('aria-busy','false')}}
function renderAccount(account){const item=document.createElement('article');item.className='resource-card';const header=document.createElement('div');header.className='resource-card-header';const title=document.createElement('h3');title.textContent=account.display_name;header.append(title,chip(account.status,account.status==='active'?'active':account.status==='disabled'?'disabled':'warning'));item.append(header);const details=document.createElement('div');details.className='resource-meta';details.append(meta('Account ID',account.account_id),meta('Current status',account.status));item.append(details);const actions=document.createElement('div');actions.className='resource-actions';const edit=document.createElement('a');edit.className='button button-quiet';edit.href=`/admin/weread/accounts/${encodeURIComponent(account.account_id)}`;edit.textContent='Manage';actions.append(edit);item.append(actions);return item}
async function loadAccounts(){accountList.setAttribute('aria-busy','true');accountList.replaceChildren(stateMessage('Loading accounts…'));try{const response=await fetch('/api/admin/weread/accounts');if(!response.ok){throw new Error('account list failed')}const accounts=await response.json();accountCount.textContent=accounts.length;if(!accounts.length){accountList.replaceChildren(stateMessage('No WeRead accounts yet.','empty'));const strong=document.createElement('strong');strong.textContent='Add an account to enable authenticated sync.';accountList.firstChild.prepend(strong)}else{accountList.replaceChildren(...accounts.map(renderAccount))}feedback(accountListError,'')}catch{accountCount.textContent='—';accountList.replaceChildren(stateMessage('Accounts could not be loaded. Refresh and try again.','error'));feedback(accountListError,'Unable to load WeRead accounts.')}finally{accountList.setAttribute('aria-busy','false')}}
document.querySelector('#source-create').addEventListener('submit', async event => {
  event.preventDefault();
  const submit = event.target.querySelector('button[type="submit"]');
  submit.disabled = true;
  feedback(error, '');
  const form = new FormData(event.target);
  const account = form.get('account_id');
  const value = name => {
    const field = form.get(name);
    return field && field.trim() ? field.trim() : null;
  };
  try {
    const response = await request('/api/admin/sources', {
      method: 'POST',
      body: JSON.stringify({
        book_id: value('book_id'),
        display_name: value('display_name'),
        article_url: value('article_url'),
        account_id: account ? account : null,
      }),
    });
    if (response.ok) {
      event.target.reset();
      await load();
      feedback(error, 'Source added.', 'success');
    } else {
      feedback(error, await apiErrorMessage(response, 'Source could not be added.'));
    }
  } catch {
    feedback(error, 'The admin service could not be reached. Try again.');
  } finally {
    submit.disabled = false;
  }
});
document.querySelector('#weread-account').addEventListener('submit',async event=>{event.preventDefault();const submit=event.target.querySelector('button[type="submit"]');submit.disabled=true;feedback(accountResult,'');const form=new FormData(event.target);const account=form.get('account_id');const displayName=form.get('display_name');const path=account?`/api/admin/weread/accounts/${encodeURIComponent(account)}`:'/api/admin/weread/accounts';try{const response=await request(path,{method:account?'PUT':'POST',body:JSON.stringify({account_id:account?account:null,display_name:displayName&&displayName.trim()?displayName.trim():null,cookie_header:form.get('cookie_header'),access_expires_at:new Date(form.get('access_expires_at')).toISOString()})});if(response.ok){const value=await response.json();event.target.reset();feedback(accountResult,`Saved account ${value.account_id}; use this ID when adding a source.`,'success');await loadAccounts()}else{feedback(accountResult,await apiErrorMessage(response,'WeRead account could not be saved; check the values and try again.'))}}catch{feedback(accountResult,'The admin service could not be reached. Try again.')}finally{submit.disabled=false}});
document.querySelector('#logout').addEventListener('click',async()=>{const logout=document.querySelector('#logout');logout.disabled=true;try{await request('/api/admin/logout',{method:'POST'})}finally{location='/admin/login'}});load();loadAccounts();
</script></body></html>"##;
    Html(
        template
            .replace("__STYLES__", STYLES)
            .replace("__USERNAME__", &username)
            .replace("__CSRF__", &csrf_json),
    )
}

/// Renders the authenticated source configuration and lifecycle page.
pub fn source_page(session: &AdminSession, source: &Source) -> Html<String> {
    let username = escape_html(session.username());
    let csrf = escape_html(session.csrf_token());
    let csrf_json = serde_json::to_string(&csrf).expect("escaped CSRF token should serialize");
    let source_id = escape_html(&source.id().to_string());
    let book_id = escape_html(source.book_id());
    let display_name = escape_html(source.display_name());
    let article_url = escape_html(
        source
            .article_url()
            .map(ToString::to_string)
            .unwrap_or_default()
            .as_str(),
    );
    let account_id = escape_html(
        source
            .account_id()
            .map(|value| value.to_string())
            .unwrap_or_default()
            .as_str(),
    );
    let status = if source.enabled() {
        "enabled"
    } else {
        "paused"
    };
    let gate = escape_html(source.scheduling_gate().as_str());
    let template = r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Edit source — Werrss admin</title>__STYLES__</head>
<body><header class="site-header"><div class="header-inner"><a class="brand" href="/admin/"><span class="brand-mark" aria-hidden="true">W</span><span class="brand-copy">Werrss<small>Admin console</small></span></a><div class="header-actions"><span class="identity">Signed in as <strong>__USERNAME__</strong></span><a class="button button-quiet" href="/admin/">Dashboard</a></div></div></header><main class="page-shell"><nav class="breadcrumb" aria-label="Breadcrumb"><a href="/admin/">Dashboard</a><span>/</span><span>Edit source</span></nav><section class="page-header"><div><p class="kicker">Source settings</p><h1>Edit source</h1><p>Update how this source is identified, scheduled, and delivered.</p></div><span class="status-chip __STATUS_CLASS__">__STATUS__</span></section><div class="split-panel"><section class="card"><div class="card-header"><div><h2>Configuration</h2><p>Changes take effect on the next synchronization cycle.</p></div><span class="card-icon" aria-hidden="true">⚙</span></div><form id="source"><fieldset class="form-section"><legend>Feed identity</legend><div class="field-grid"><label><span>Book ID</span><input name="book_id" value="__BOOK_ID__" required></label><label><span>Name</span><input name="display_name" value="__DISPLAY_NAME__" required></label><label class="wide"><span class="label-row"><span>Article URL</span><span class="label-hint">Optional for Book ID-only sources</span></span><input name="article_url" type="url" value="__ARTICLE_URL__"><span class="small">Clear it when the source is identified only by Book ID.</span></label><label class="wide"><span class="label-row"><span>WeRead account ID</span><span class="label-hint">Optional</span></span><input name="account_id" value="__ACCOUNT_ID__"><span class="small">Clear it to let the worker choose an enabled account.</span></label></div></fieldset><fieldset class="form-section"><legend>Delivery policy</legend><div class="field-grid"><label><span>Sync interval (seconds)</span><input name="sync_interval_seconds" type="number" min="1" value="__SYNC_INTERVAL__" required></label><label><span>RSS item limit</span><input name="rss_item_limit" type="number" min="1" value="__RSS_ITEM_LIMIT__" required></label><label><span>Priority</span><input name="priority" type="number" value="__PRIORITY__" required></label><label><span>Maximum attempts</span><input name="max_attempts" type="number" min="1" value="__MAX_ATTEMPTS__" required></label></div></fieldset><div class="form-actions"><a class="button button-quiet" href="/admin/">Cancel</a><button class="button-primary" type="submit">Save changes</button></div></form><p id="result" class="feedback" role="status" hidden></p></section><aside class="side-stack"><section class="card"><div class="card-header"><div><h2>Runtime status</h2><p>Current scheduling state.</p></div><span class="card-icon green" aria-hidden="true">✓</span></div><dl class="definition-list"><div><dt>Status</dt><dd>__STATUS__</dd></div><div><dt>Gate</dt><dd>__GATE__</dd></div><div><dt>Revision</dt><dd>__REVISION__</dd></div></dl><div class="form-actions"><button id="toggle" class="button-secondary" type="button">__TOGGLE__ source</button><button id="clear-gate" class="button-quiet" type="button">Clear gate</button></div></section><section class="card danger-zone"><div class="card-header"><div><h2>Danger zone</h2><p>Deleting removes this source and its stored articles.</p></div><span class="card-icon red" aria-hidden="true">!</span></div><button id="delete" class="button-danger" type="button">Delete source</button></section></aside></div></main>
<script>
const sourceId='__SOURCE_ID__';const csrf=__CSRF__;const headers={'content-type':'application/json','x-csrf-token':csrf};const result=document.querySelector('#result');
const request=(path,options={})=>fetch(path,{...options,headers:{...headers,...(options.headers||{})}});
async function apiErrorMessage(response,fallback){try{const value=await response.json();if(typeof value.error==='string'&&value.error.trim()){return value.error}}catch{}return fallback}
async function runControlAction(control,action,fallback){control.disabled=true;try{const response=await action();if(response.ok){return true}result.textContent=await apiErrorMessage(response,fallback)}catch{result.textContent='The admin service could not be reached. Try again.'}finally{control.disabled=false}result.className='feedback error';result.hidden=false;return false}
document.querySelector('#source').addEventListener('submit',async event=>{event.preventDefault();const submit=event.target.querySelector('button[type="submit"]');submit.disabled=true;result.hidden=true;const form=new FormData(event.target);const value=name=>{const field=form.get(name);return field&&field.toString().trim()?field.toString().trim():null};try{const response=await request(`/api/admin/sources/${sourceId}`,{method:'PUT',body:JSON.stringify({book_id:value('book_id'),display_name:value('display_name'),article_url:value('article_url'),account_id:value('account_id'),sync_interval_seconds:Number(form.get('sync_interval_seconds')),rss_item_limit:Number(form.get('rss_item_limit')),priority:Number(form.get('priority')),max_attempts:Number(form.get('max_attempts'))})});if(response.ok){result.textContent='Source updated.';result.className='feedback success';result.hidden=false;setTimeout(()=>location.reload(),350)}else{result.textContent=await apiErrorMessage(response,'Source could not be updated.');result.className='feedback error';result.hidden=false}}catch{result.textContent='The admin service could not be reached. Try again.';result.className='feedback error';result.hidden=false}finally{submit.disabled=false}});
document.querySelector('#toggle').addEventListener('click',async()=>{const control=document.querySelector('#toggle');if(await runControlAction(control,()=>request(`/api/admin/sources/${sourceId}/enabled`,{method:'POST',body:JSON.stringify({enabled:__NEXT_ENABLED__})}),'Source status could not be changed.')){location.reload()}});
document.querySelector('#clear-gate').addEventListener('click',async()=>{const control=document.querySelector('#clear-gate');if(await runControlAction(control,()=>request(`/api/admin/sources/${sourceId}/gate`,{method:'POST',body:JSON.stringify({gate:'ready'})}),'Source gate could not be cleared.')){location.reload()}});
document.querySelector('#delete').addEventListener('click',async()=>{if(!confirm('Delete this source and its stored articles permanently?')){return}const control=document.querySelector('#delete');if(await runControlAction(control,()=>request(`/api/admin/sources/${sourceId}`,{method:'DELETE'}),'Source could not be deleted.')){location='/admin/'}});
</script></body></html>"##;
    Html(
        template
            .replace("__STYLES__", STYLES)
            .replace("__USERNAME__", &username)
            .replace("__SOURCE_ID__", &source_id)
            .replace("__BOOK_ID__", &book_id)
            .replace("__DISPLAY_NAME__", &display_name)
            .replace("__ARTICLE_URL__", &article_url)
            .replace("__ACCOUNT_ID__", &account_id)
            .replace("__STATUS__", status)
            .replace("__STATUS_CLASS__", status)
            .replace("__GATE__", &gate)
            .replace("__REVISION__", &source.feed_revision().to_string())
            .replace(
                "__SYNC_INTERVAL__",
                &source.sync_interval().num_seconds().to_string(),
            )
            .replace("__RSS_ITEM_LIMIT__", &source.rss_item_limit().to_string())
            .replace("__PRIORITY__", &source.priority().to_string())
            .replace("__MAX_ATTEMPTS__", &source.max_attempts().to_string())
            .replace(
                "__TOGGLE__",
                if source.enabled() { "Pause" } else { "Enable" },
            )
            .replace(
                "__NEXT_ENABLED__",
                if source.enabled() { "false" } else { "true" },
            )
            .replace("__CSRF__", &csrf_json),
    )
}

/// Renders the authenticated WeRead account list page.
pub fn weread_accounts_page(session: &AdminSession) -> Html<String> {
    let username = escape_html(session.username());
    let template = r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>WeRead accounts — Werrss admin</title>__STYLES__</head>
<body><header class="site-header"><div class="header-inner"><a class="brand" href="/admin/"><span class="brand-mark" aria-hidden="true">W</span><span class="brand-copy">Werrss<small>Admin console</small></span></a><div class="header-actions"><span class="identity">Signed in as <strong>__USERNAME__</strong></span><a class="button button-quiet" href="/admin/">Dashboard</a></div></div></header><main class="page-shell"><nav class="breadcrumb" aria-label="Breadcrumb"><a href="/admin/">Dashboard</a><span>/</span><span>WeRead accounts</span></nav><section class="page-header"><div><p class="kicker">Credentials</p><h1>WeRead accounts</h1><p>Keep authenticated browser sessions healthy without exposing stored cookies.</p></div><a class="button button-primary" href="/admin/#weread-account-card">Add account</a></section><section class="card"><div class="card-header"><div><h2>Account directory</h2><p>Active accounts can be selected for unbound source synchronization.</p></div><span class="card-icon green" aria-hidden="true">W</span></div><div class="notice info"><span class="notice-icon" aria-hidden="true">i</span><span>Credentials are encrypted at rest. Manage an account to replace its cookie header or change its status.</span></div><p id="error" class="feedback" role="alert" hidden></p><div id="accounts" class="resource-grid" aria-live="polite" aria-busy="true"><div class="loading-state">Loading accounts…</div></div></section></main>
<script>
const list=document.querySelector('#accounts');const error=document.querySelector('#error');
function stateMessage(message,kind='loading'){const item=document.createElement('div');item.className=`${kind}-state`;item.textContent=message;return item}
async function loadAccounts(){list.setAttribute('aria-busy','true');try{const response=await fetch('/api/admin/weread/accounts');if(!response.ok){throw new Error('account list failed')}const accounts=await response.json();if(!accounts.length){list.replaceChildren(stateMessage('No WeRead accounts have been added.','empty'));const strong=document.createElement('strong');strong.textContent='Add your first account from the dashboard.';list.firstChild.prepend(strong);return}list.replaceChildren(...accounts.map(account=>{const item=document.createElement('article');item.className='resource-card';const header=document.createElement('div');header.className='resource-card-header';const title=document.createElement('h3');title.textContent=account.display_name;const status=document.createElement('span');status.className=`status-chip ${account.status==='active'?'active':account.status==='disabled'?'disabled':'warning'}`;status.textContent=account.status;header.append(title,status);item.append(header);const details=document.createElement('div');details.className='resource-meta';const row=document.createElement('div');row.className='resource-meta-row';const label=document.createElement('span');label.textContent='Account ID';const value=document.createElement('strong');value.textContent=account.account_id;row.append(label,value);details.append(row);item.append(details);const edit=document.createElement('a');edit.className='button button-quiet';edit.href=`/admin/weread/accounts/${encodeURIComponent(account.account_id)}`;edit.textContent='Manage account';item.append(edit);return item}))}catch{list.replaceChildren(stateMessage('Accounts could not be loaded. Refresh and try again.','error'));error.textContent='Unable to load WeRead accounts.';error.className='feedback error';error.hidden=false}finally{list.setAttribute('aria-busy','false')}}
loadAccounts();
</script></body></html>"##;
    Html(
        template
            .replace("__STYLES__", STYLES)
            .replace("__USERNAME__", &username),
    )
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
        "active"
    };
    let template = r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>WeRead account — Werrss admin</title>__STYLES__</head>
<body><header class="site-header"><div class="header-inner"><a class="brand" href="/admin/"><span class="brand-mark" aria-hidden="true">W</span><span class="brand-copy">Werrss<small>Admin console</small></span></a><div class="header-actions"><span class="identity">Signed in as <strong>__USERNAME__</strong></span><a class="button button-quiet" href="/admin/weread/accounts">Accounts</a></div></div></header><main class="page-shell"><nav class="breadcrumb" aria-label="Breadcrumb"><a href="/admin/">Dashboard</a><span>/</span><a href="/admin/weread/accounts">WeRead accounts</a><span>/</span><span>Manage account</span></nav><section class="page-header"><div><p class="kicker">Credential settings</p><h1>Manage WeRead account</h1><p>Rotate the browser session or update its display name.</p></div><span class="status-chip __STATUS_CLASS__">__STATUS__</span></section><div class="split-panel"><section class="card"><div class="card-header"><div><h2>Account details</h2><p>Account ID <code>__ACCOUNT_ID__</code></p></div><span class="card-icon green" aria-hidden="true">W</span></div><div class="notice info"><span class="notice-icon" aria-hidden="true">i</span><span>For security, the stored cookie is never displayed. Paste a complete fresh Cookie request-header value when rotating credentials.</span></div><form id="account"><label><span class="label-row"><span>Display name</span><span class="label-hint">Optional when wr_name is present</span></span><input name="display_name" value="__DISPLAY_NAME__"><span class="small">You may leave this blank if the cookie contains a usable <code>wr_name</code>.</span></label><label><span>New WeRead Cookie header</span><textarea name="cookie_header" rows="7" required autocomplete="off" placeholder="wr_vid=…; wr_skey=…; wr_rt=…"></textarea></label><label><span>Access token expiry</span><input id="expiry" name="access_expires_at" type="datetime-local" data-value="__EXPIRES_AT__" required></label><div class="form-actions"><a class="button button-quiet" href="/admin/weread/accounts">Cancel</a><button class="button-primary" type="submit">Save changes</button></div></form><p id="result" class="feedback" role="status" hidden></p></section><aside class="side-stack"><section class="card"><div class="card-header"><div><h2>Account status</h2><p>Disabled accounts are skipped by random selection.</p></div><span class="card-icon orange" aria-hidden="true">●</span></div><button id="toggle" class="button-secondary" type="button">__TOGGLE__ account</button></section><section class="card danger-zone"><div class="card-header"><div><h2>Danger zone</h2><p>Deleting removes the stored credentials permanently.</p></div><span class="card-icon red" aria-hidden="true">!</span></div><button id="delete" class="button-danger" type="button">Delete account</button></section></aside></div></main>
<script>
const accountId='__ACCOUNT_ID__';const csrf=__CSRF__;const headers={'content-type':'application/json','x-csrf-token':csrf};const result=document.querySelector('#result');document.querySelector('#expiry').value=new Date(document.querySelector('#expiry').dataset.value).toISOString().slice(0,16);
const request=(path,options={})=>fetch(path,{...options,headers:{...headers,...(options.headers||{})}});
async function apiErrorMessage(response,fallback){try{const value=await response.json();if(typeof value.error==='string'&&value.error.trim()){return value.error}}catch{}return fallback}
async function runControlAction(control,action,fallback){control.disabled=true;try{const response=await action();if(response.ok){return true}result.textContent=await apiErrorMessage(response,fallback)}catch{result.textContent='The admin service could not be reached. Try again.'}finally{control.disabled=false}result.className='feedback error';result.hidden=false;return false}
document.querySelector('#account').addEventListener('submit',async event=>{event.preventDefault();const submit=event.target.querySelector('button[type="submit"]');submit.disabled=true;result.hidden=true;const form=new FormData(event.target);try{const response=await request(`/api/admin/weread/accounts/${accountId}`,{method:'PUT',body:JSON.stringify({account_id:accountId,display_name:form.get('display_name')&&form.get('display_name').trim()?form.get('display_name').trim():null,cookie_header:form.get('cookie_header'),access_expires_at:new Date(form.get('access_expires_at')).toISOString()})});if(response.ok){result.textContent='Account updated.';result.className='feedback success';result.hidden=false;setTimeout(()=>location.reload(),350)}else{result.textContent=await apiErrorMessage(response,'Account could not be updated.');result.className='feedback error';result.hidden=false}}catch{result.textContent='The admin service could not be reached. Try again.';result.className='feedback error';result.hidden=false}finally{submit.disabled=false}});
document.querySelector('#toggle').addEventListener('click',async()=>{const control=document.querySelector('#toggle');if(await runControlAction(control,()=>request(`/api/admin/weread/accounts/${accountId}/enabled`,{method:'POST',body:JSON.stringify({enabled:__ENABLED__})}),'Account status could not be changed.')){location.reload()}});
document.querySelector('#delete').addEventListener('click',async()=>{if(!confirm('Delete this WeRead account permanently?')){return}const control=document.querySelector('#delete');if(await runControlAction(control,()=>request(`/api/admin/weread/accounts/${accountId}`,{method:'DELETE'}),'Account could not be deleted.')){location='/admin/'}});
</script></body></html>"##;
    Html(
        template
            .replace("__STYLES__", STYLES)
            .replace("__USERNAME__", &username)
            .replace("__ACCOUNT_ID__", &account_id)
            .replace("__STATUS__", status)
            .replace("__STATUS_CLASS__", status)
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

    fn session_for(username: &str) -> AdminSession {
        let auth = AdminAuthenticator::new(
            username.to_owned(),
            SecretString::new("password".to_owned().into_boxed_str()),
            SecretString::new("signing-key".to_owned().into_boxed_str()),
        )
        .unwrap();
        auth.login(
            username,
            "password",
            "test",
            "2026-09-01T00:00:00Z".parse().unwrap(),
        )
        .unwrap()
        .0
    }

    fn session() -> AdminSession {
        session_for("admin")
    }

    #[test]
    fn login_page_has_accessible_sign_in_form_and_responsive_styles() {
        let body = login_page().0;
        assert!(body.contains("<html lang=\"en\">"));
        assert!(body.contains("autocomplete=\"username\""));
        assert!(body.contains("autocomplete=\"current-password\""));
        assert!(body.contains("prefers-reduced-motion"));
    }

    #[test]
    fn admin_page_renders_navigation_and_workspace_summary() {
        let body = admin_page(&session()).0;
        assert!(body.contains("href=\"/admin/weread/accounts\""));
        assert!(body.contains("id=\"source-count\""));
        assert!(body.contains("id=\"active-source-count\""));
        assert!(body.contains("id=\"account-count\""));
        assert!(body.contains("id=\"sources\" class=\"resource-grid\""));
        assert!(body.contains("id=\"weread-accounts\" class=\"resource-grid\""));
    }

    #[test]
    fn admin_page_escapes_username_in_the_navigation() {
        let body = admin_page(&session_for("<admin> & operator")).0;
        assert!(body.contains("&lt;admin&gt; &amp; operator"));
        assert!(!body.contains("<admin> & operator"));
    }

    #[test]
    fn admin_page_keeps_credentials_out_of_markup_and_uses_api_error_messages() {
        let body = admin_page(&session()).0;
        assert!(!body.contains("correct horse"));
        assert!(!body.contains("wr_skey=secret"));
        assert!(body.contains("async function apiErrorMessage(response,fallback)"));
        assert!(body.contains("typeof value.error==='string'"));
        assert!(body.contains("return fallback"));
        assert!(body.contains("accountResult"));
        assert!(body.contains("/api/admin/weread/accounts"));
    }

    #[test]
    fn admin_page_uses_absolute_feed_url_when_the_api_provides_one() {
        let body = admin_page(&session()).0;
        assert!(body.contains("const href=value.feed_url||value.feed_path"));
        assert!(body.contains("link.target='_blank'"));
    }

    #[test]
    fn source_page_groups_configuration_and_lifecycle_controls() {
        let source = Source::new(crate::domain::source::NewSource::test_default())
            .expect("test source should be valid");
        let body = source_page(&session(), &source).0;
        assert!(body.contains("<h1>Edit source</h1>"));
        assert!(body.contains("<legend>Feed identity</legend>"));
        assert!(body.contains("<legend>Delivery policy</legend>"));
        assert!(body.contains("id=\"toggle\""));
        assert!(body.contains("id=\"clear-gate\""));
        assert!(body.contains("id=\"delete\""));
        assert!(!body.contains("cookie_header"));
    }

    #[test]
    fn source_page_restores_lifecycle_controls_after_request_failures() {
        let source = Source::new(crate::domain::source::NewSource::test_default())
            .expect("test source should be valid");
        let body = source_page(&session(), &source).0;

        assert!(body.contains("async function runControlAction(control,action,fallback)"));
        assert!(body.contains(
            "catch{result.textContent='The admin service could not be reached. Try again.'}"
        ));
        assert!(body.contains("finally{control.disabled=false}"));
        assert_eq!(body.matches("if(await runControlAction(control").count(), 3);
    }

    #[test]
    fn source_page_escapes_values_and_supports_unbound_sources() {
        let mut spec = crate::domain::source::NewSource::test_default();
        spec.book_id = "book<&\"".to_owned();
        spec.display_name = "Name<&\"".to_owned();
        spec.article_url = None;
        let source = Source::new(spec).expect("test source should be valid");
        let body = source_page(&session(), &source).0;
        assert!(body.contains("name=\"book_id\" value=\"book&lt;&amp;&quot;\""));
        assert!(body.contains("name=\"display_name\" value=\"Name&lt;&amp;&quot;\""));
        assert!(body.contains("name=\"article_url\" type=\"url\" value=\"\""));
        assert!(body.contains("name=\"account_id\" value=\"\""));
    }

    #[test]
    fn account_list_page_has_empty_state_and_safe_account_navigation() {
        let body = weread_accounts_page(&session()).0;
        assert!(body.contains("<h1>WeRead accounts</h1>"));
        assert!(body.contains("href=\"/admin/\""));
        assert!(body.contains("No WeRead accounts have been added."));
        assert!(body.contains("/admin/weread/accounts/${encodeURIComponent(account.account_id)}"));
        assert!(!body.contains("cookie_header"));
    }

    #[test]
    fn account_page_restores_lifecycle_controls_after_request_failures() {
        let account = WeReadAccount::from_parts(
            crate::domain::credentials::WeReadAccountId::from_uuid(uuid::Uuid::from_u128(1)),
            "Primary".to_owned(),
            2,
            "2026-10-01T00:00:00Z".parse().unwrap(),
            false,
        );
        let body = weread_account_page(&session(), &account).0;

        assert!(body.contains("async function runControlAction(control,action,fallback)"));
        assert!(body.contains(
            "catch{result.textContent='The admin service could not be reached. Try again.'}"
        ));
        assert!(body.contains("finally{control.disabled=false}"));
        assert_eq!(body.matches("if(await runControlAction(control").count(), 2);
    }

    #[test]
    fn account_page_exposes_rotation_and_lifecycle_controls_without_credentials() {
        let account = WeReadAccount::from_parts(
            crate::domain::credentials::WeReadAccountId::from_uuid(uuid::Uuid::from_u128(1)),
            "Primary".to_owned(),
            2,
            "2026-10-01T00:00:00Z".parse().unwrap(),
            false,
        );
        let body = weread_account_page(&session(), &account).0;
        assert!(body.contains("Account details"));
        assert!(body.contains("Optional when wr_name is present"));
        assert!(body.contains("/api/admin/weread/accounts/${accountId}"));
        assert!(body.contains("/api/admin/weread/accounts/${accountId}/enabled"));
        assert!(body.contains("method:'DELETE'"));
        assert!(body.contains("data-value=\"2026-10-01T00:00:00+00:00\""));
        assert!(!body.contains("access-token"));
    }
}
