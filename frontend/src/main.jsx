import React, {useEffect, useRef, useState} from 'react';
import {createRoot} from 'react-dom/client';
import {readEvents} from './sse';
import './style.css';
import './tools.css';
import Diagnostics from './RequestDiagnostics.jsx';
import MessageContent from './MessageContent.jsx';
import {createTrace, recordTrace} from './diagnostics';
import {Artifact, ToolActivity, mergeToolStep} from './ToolActivity.jsx';

const API = (import.meta.env.VITE_API_URL || (import.meta.env.DEV ? 'http://127.0.0.1:8080' : '')).replace(/\/$/, '');

function App() {
  const [key, setKey] = useState('');
  const [menuOpen, setMenuOpen] = useState(false);
  const [draftKey, setDraftKey] = useState('');
  const [chats, setChats] = useState([]);
  const [chat, setChat] = useState(null);
  const [messages, setMessages] = useState([]);
  const [prompt, setPrompt] = useState('');
  const [busy, setBusy] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [phase, setPhase] = useState('');
  const [trace, setTrace] = useState(null);
  const [model, setModel] = useState(null);
  const [toolsEnabled,setToolsEnabled] = useState(true);
  const [toolSteps,setToolSteps] = useState([]);
  const [files,setFiles] = useState([]);
  const [moreChats, setMoreChats] = useState(false);
  const [moreMessages, setMoreMessages] = useState(false);
  const abort = useRef(null), bottom = useRef(null), selected = useRef(null);
  const activeRun = useRef(null);

  async function api(path, options = {}, access = key) {
    const response = await fetch(API + path, {...options, headers: {
      'Content-Type': 'application/json', Authorization: `Bearer ${access}`, ...options.headers}});
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      throw new Error(typeof body.detail === 'string' ? body.detail : `Request failed (${response.status})`);
    }
    return response;
  }
  async function refresh(access = key, offset = 0) {
    const rows = await (await api(`/v1/conversations?offset=${offset}`, {}, access)).json();
    setChats(old => offset ? [...old, ...rows] : rows); setMoreChats(rows.length === 50);
    if (!offset) setChat(current => current ? rows.find(item => item.id === current.id) || current : current);
  }
  async function unlock(e) {
    e.preventDefault(); setError(''); setLoading(true);
    try {
      const status = await (await api('/v1/status', {}, draftKey)).json();
      await refresh(draftKey); setModel(status); setKey(draftKey); setDraftKey('');
    } catch(e) { setError(e.message); } finally { setLoading(false); }
  }
  async function openChat(item, older = false) {
    if (busy) return;
    if (item.id !== chat?.id) setTrace(null);
    setLoading(true); setError(''); selected.current = item.id;
    try {
      const before = older && messages.length ? `?before=${messages[0].seq}` : '';
      const rows = await (await api(`/v1/conversations/${item.id}/messages${before}`)).json();
      const [steps,attachments,active] = await Promise.all([
        api(`/v1/conversations/${item.id}/tools`).then(r=>r.json()),
        api(`/v1/conversations/${item.id}/files`).then(r=>r.json()),
        model?.durable_execution ? api(`/v1/conversations/${item.id}/active-run`).then(r=>r.json()) : null]);
      if (selected.current !== item.id) return;
      setToolSteps(steps); setFiles(attachments);
      setMenuOpen(false); setChat(item); setMessages(old => older ? [...rows, ...old] : rows); setMoreMessages(rows.length === 100);
      if (active && !older) void resumeRun(active.id);
    } catch(e) { setError(e.message); } finally { if (selected.current === item.id) setLoading(false); }
  }
  async function watchRun(response, requestId, assistantId) {
    let cursor = 0, failures = 0, reload = false;
    const observe = (event, data = {}) => setTrace(current => recordTrace(current, event, data));
    while (true) {
      try {
        if (!response) response=await api(`/v1/runs/${requestId}/events?after=${cursor}`,{signal:abort.current.signal});
        await readEvents(response.body, (event, data, id) => {
          if (id && id <= cursor) return;
          if (id) cursor = id;
          observe(event, data);
          if (event === 'queued') setPhase(data.position ? `Queued · ${data.position} ahead` : 'Preparing');
          if (event === 'starting') setPhase('Starting model…');
          if (event === 'started' || (event === 'status' && data.stage === 'agent_round')) setPhase('Working');
          if (event === 'tool') setToolSteps(old=>mergeToolStep(old,data));
          if (event === 'artifact') setFiles(old=>old.some(f=>f.id===data.id) ? old : [...old,data]);
          if (event === 'replace') setMessages(old=>old.map(m=>m.id===assistantId ? {...m,content:data.text} : m));
          if (event === 'token') setMessages(old=>old.map(m=>m.id===assistantId ? {...m,content:m.content+data.text} : m));
          if (event === 'done' || event === 'error') {
            reload = !!data.reload;
            setMessages(old=>old.map(m=>m.run_id===requestId ? {...m,run_status:data.run_status || (event==='done'?'done':'failed'),
              ...(m.id===assistantId ? {pending:false,incomplete:event==='error'} : {})} : m));
            setPhase(event==='done' ? 'Saved' : data.run_status==='cancelled' ? 'Cancelled' : 'Failed');
            if (event==='error') setError(data.message);
          }
        });
        if (reload && selected.current) setMessages(await (await api(`/v1/conversations/${selected.current}/messages`)).json());
        return;
      } catch(e) {
        if (!model?.durable_execution || e.name==='AbortError' || ++failures>5) throw e;
        observe('browser',{stage:'reconnecting',attempt:failures});
        setPhase('Reconnecting · agent continues on the server');
        response=null;
        await new Promise(resolve=>setTimeout(resolve,Math.min(failures*1000,5000)));
      }
    }
  }
  async function resumeRun(requestId) {
    setBusy(true); activeRun.current=requestId; abort.current=new AbortController();
    const assistantId=`pending-${requestId}`;
    setTrace(createTrace(requestId));
    setMessages(old=>[...old.filter(m=>!(m.role==='assistant' && m.run_id===requestId)),
      {id:assistantId,run_id:requestId,role:'assistant',content:'',pending:true}]);
    try {
      const response=await api(`/v1/runs/${requestId}/events`,{signal:abort.current.signal});
      await watchRun(response,requestId,assistantId);
    } catch(e) {setError('Connection interrupted. The agent can continue; reload history to reconnect.');}
    finally {activeRun.current=null;abort.current=null;setBusy(false);await refresh().catch(()=>{});}
  }
  async function stopRun() {
    if (model?.durable_execution && activeRun.current) {
      try {
        await api(`/v1/runs/${activeRun.current}/cancel`,{method:'POST'});
        setPhase('Cancellation requested · waiting for cleanup');
        setTrace(current=>recordTrace(current,'browser',{stage:'cancellation_requested'}));
      } catch(e) {setError(`Could not request cancellation: ${e.message}`);}
    } else abort.current?.abort();
  }
  function newChat() { setToolSteps([]); setFiles([]); setTrace(null); setPhase(''); setMenuOpen(false); selected.current = null; setChat(null); setMessages([]); setMoreMessages(false); setPrompt(''); setError(''); }
  async function upload(event) {
    const file = event.target.files[0]; event.target.value = '';
    if (!file) return;
    if (file.size > 2*1024*1024) {setError('Choose a file of 2 MB or less.');return}
    setLoading(true);setError('');
    try {
      let current = chat;
      if (!current) {current = await (await api('/v1/conversations',{method:'POST'})).json();setChat(current);selected.current=current.id}
      const data = await new Promise((resolve,reject)=>{
        const reader=new FileReader();reader.onload=()=>resolve(reader.result.split(',')[1]);reader.onerror=reject;reader.readAsDataURL(file);
      });
      const saved=await (await api(`/v1/conversations/${current.id}/files`,{method:'POST',body:JSON.stringify({name:file.name.slice(0,100),data})})).json();
      setFiles(old=>[...old,saved]);setToolsEnabled(true);
      await refresh();
    } catch(e) {setError(e.message)} finally {setLoading(false)}
  }
  async function send(e) {
    e.preventDefault(); if (busy || !prompt.trim()) return;
    setBusy(true); setError(''); setPhase('Connecting');
    const text = prompt.trim(), requestId = crypto.randomUUID();
    const assistantId = crypto.randomUUID(); let accepted = false;
    setTrace(createTrace(requestId));
    const observe = (event, data = {}) => setTrace(current => recordTrace(current, event, data));
    abort.current = new AbortController();
    try {
      let current = chat;
      if (!current) {
        observe('browser', {stage:'creating_conversation'});
        current = await (await api('/v1/conversations', {method:'POST', signal: abort.current.signal})).json();
        setChat(current); selected.current = current.id;
      }
      observe('browser', {stage:'submitting_prompt'});
      const response = await api(`/v1/conversations/${current.id}/messages`, {
        method:'POST', body:JSON.stringify({request_id:requestId, content:text,enable_tools:toolsEnabled && !!model?.tools_configured}), signal:abort.current.signal});
      accepted = true; setPrompt('');
      activeRun.current=requestId;
      observe('browser', {stage:'stream_connected'});
      setMessages(old => [...old, {id:requestId,run_id:requestId, role:'user', content:text}, {id:assistantId,run_id:requestId, role:'assistant', content:'', pending:true}]);
      await watchRun(response,requestId,assistantId);
    } catch(e) {
      observe('browser', {stage:e.name === 'AbortError' ? 'cancelled' : 'error', message:e.message});
      setError(model?.durable_execution ? 'Connection interrupted. The request may still be running; reload history before retrying.' : e.name==='AbortError' ? 'Generation stopped. Reload history to confirm its final state.' : e.message);
      if (accepted) setMessages(old => old.map(m => m.id === assistantId ? {...m, pending:false, incomplete:true} : m));
      setPhase('');
    } finally {
      abort.current = null; activeRun.current=null; setBusy(false);
      await refresh().catch(e => setError(e.message));
      await api('/v1/status').then(r=>r.json()).then(setModel).catch(()=>{});
    }
  }
  useEffect(() => { bottom.current?.scrollIntoView({behavior:'smooth'}); }, [messages.length, phase]);
  useEffect(() => () => abort.current?.abort(), []);

  const replies = new Set(messages.filter(m => m.role === 'assistant').map(m => m.run_id));
  const progressLabel = trace?.stage === 'model_retry' && trace.retryAt
    ? 'Rate limited · automatic retry pending' : trace?.title;

  if (!key) return <div className="lock-page"><div className="brand"><span className="mark">s</span> stream</div>
    <form className="unlock" onSubmit={unlock}><div className="eyebrow">YOUR PERSONAL WORKSPACE</div><h1>A little room<br/>to think out loud.</h1>
      <p>Pick up a conversation. Follow an idea.<br/>Your chat, on your own model.</p>
      <label htmlFor="key">Access key</label><input id="key" type="password" autoComplete="off" value={draftKey} onChange={e=>setDraftKey(e.target.value)} placeholder="Enter your personal access key" required/>
      <button className="primary" disabled={loading || !API}>{loading ? 'Connecting…' : 'Open workspace →'}</button>
      <small>Your key stays in memory for this tab. Refreshing locks the workspace.</small>
      {!API && <p className="error">The API address has not been configured yet.</p>}{error && <p className="error" role="alert">{error}</p>}
    </form><div className="lock-footer">One conversation at a time.</div></div>;

  return <div className="workspace"><aside className={menuOpen ? 'menu-open' : ''}><div className="brand"><span className="mark">s</span> stream</div>
    <button className="new-chat" onClick={newChat} disabled={busy || loading}>＋ New conversation</button>
    <div className="eyebrow recent">RECENT CONVERSATIONS</div>
    <nav aria-label="Conversations">{chats.map(c=><button key={c.id} className={chat?.id===c.id?'chat-link selected':'chat-link'} onClick={()=>openChat(c)} disabled={busy || loading}><span>◷</span><span>{c.title}</span></button>)}
      {!chats.length && <p className="empty-list">Your conversations will appear here.</p>}{moreChats && <button onClick={()=>refresh(key,chats.length).catch(e=>setError(e.message))}>Load more</button>}</nav>
    <div className="account"><div className="avatar">A</div><div><strong>Personal workspace</strong><small>Access key protected</small></div><button title="Lock workspace" aria-label="Lock workspace" disabled={busy} onClick={()=>{setKey('');newChat();setChats([])}}>↗</button></div>
  </aside><main><header><button className="menu-toggle" aria-label="Toggle conversations" aria-expanded={menuOpen} onClick={()=>setMenuOpen(!menuOpen)}>☰</button><div><strong>{chat?.title || 'New conversation'}</strong><span>Private workspace</span></div><div className="status"><i className={model?.ready?'online':''}/>{model?.ready?'Qwen · Vercel Gateway':'Model unavailable'}</div></header>
    <section className="conversation" aria-label="Messages">
      {moreMessages && <button disabled={loading} onClick={()=>openChat(chat,true)}>Load earlier messages</button>}
      {!messages.length && <div className="welcome"><span className="spark">✳</span><div className="eyebrow">SPACE FOR YOUR NEXT IDEA</div><h1>What’s on your mind?</h1><p>Ask a question, work through a problem,<br/>or start somewhere unexpected.</p>
        <div className="suggestions">{['Explain a tricky concept','Help me think through an idea','Give me a writing prompt'].map(t=><button key={t} onClick={()=>setPrompt(t)}>{t}<span>↗</span></button>)}</div></div>}
      {messages.map(m=><article key={m.id} className={`message ${m.role}`}>
        <div className="message-label">{m.role==='user'?'YOU':'STREAM'}{m.incomplete || (m.run_status && m.run_status!=='done') ? <span> · {m.run_status || 'incomplete'}</span> : null}</div>
        {m.role==='assistant' && trace?.requestId===m.run_id && <Diagnostics trace={trace}/>}
        {m.role==='assistant' && <ToolActivity steps={toolSteps.filter(s=>s.run_id===m.run_id)}/>}
        <div className="message-text">{m.content ? (m.role==='assistant' ? <MessageContent text={m.content}/> : m.content) : (m.pending?(progressLabel || 'Waiting for server status…'):'')}{m.pending && m.content && <span className="cursor"/>}</div>
        {(m.role==='assistant' || !replies.has(m.run_id)) && <>
          {m.role==='user' && <ToolActivity steps={toolSteps.filter(s=>s.run_id===m.run_id)}/>}
          {files.filter(f=>f.run_id===m.run_id).map(f=><Artifact key={f.id} file={f} api={api}/>)}
        </>}
      </article>)}{trace && !messages.some(m=>m.role==='assistant' && m.run_id===trace.requestId) && <Diagnostics trace={trace}/>}<div ref={bottom}/>

    </section><div className="composer-area">{error && <div className="error" role="alert">{error}{chat && !busy && <button onClick={()=>openChat(chat)}>Reload history</button>}</div>}
      <div className="tool-controls"><label><input type="checkbox" checked={toolsEnabled && !!model?.tools_configured} disabled={busy || loading || !model?.tools_configured} onChange={e=>setToolsEnabled(e.target.checked)}/> Code tools</label>
        <label className="upload-button">＋ Add file<input type="file" aria-label="Add data file" disabled={busy || loading || !model?.tools_configured} onChange={upload}/></label>
        <details className="runtime-details"><summary>Temporal · {model?.temporal_connected?'connected':'connecting'}</summary>
          <div><p>The agent continues on the server if this tab closes. Reopen the conversation to reconnect.</p>
          <p>{model?.tools_configured?'Tools run in an isolated sandbox, started only when needed.':'Code tools unavailable.'}</p>
          <p>Fast mode: brief action summaries and real tool results; private reasoning is not displayed.</p>
          {model?.sandbox_budget && <p>Sandbox use: {Math.ceil(model.sandbox_budget.daily_seconds/60)}/60 min today · {Math.ceil(model.sandbox_budget.total_seconds/60)}/600 min total</p>}</div>
        </details>
      </div>
      {files.some(f=>!f.run_id) && <details className="input-files"><summary>Conversation files · {files.filter(f=>!f.run_id).length}</summary>{files.filter(f=>!f.run_id).map(f=><Artifact key={f.id} file={f} api={api}/>)}<small>Files are sent to E2B only when a tool runs. 2 MB per file.</small></details>}
      <form className="composer" onSubmit={send}><textarea aria-label="Message" placeholder="Message Stream…" maxLength={4096} value={prompt} disabled={busy || loading} onChange={e=>setPrompt(e.target.value)} onKeyDown={e=>{if(e.key==='Enter'&&!e.shiftKey&&!e.nativeEvent.isComposing){e.preventDefault();send(e)}}}/>
        <div className="composer-bottom"><span>{busy?(progressLabel || phase):loading?'Loading…':phase || 'Shift + Enter for a new line'}</span>{busy?<button type="button" className="send" onClick={stopRun} aria-label="Stop generation">■</button>:<button className="send" disabled={!prompt.trim()||loading} aria-label="Send message">↑</button>}</div></form><p className="disclaimer">Answers can be imperfect. Check the details that matter.</p>
    </div></main></div>;
}
createRoot(document.getElementById('root')).render(<App/>);
