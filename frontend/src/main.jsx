import React, {useEffect, useRef, useState} from 'react';
import {createRoot} from 'react-dom/client';
import {readEvents} from './sse';
import './style.css';

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
  const [model, setModel] = useState(null);
  const [moreChats, setMoreChats] = useState(false);
  const [moreMessages, setMoreMessages] = useState(false);
  const abort = useRef(null), bottom = useRef(null), selected = useRef(null);

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
    setLoading(true); setError(''); selected.current = item.id;
    try {
      const before = older && messages.length ? `?before=${messages[0].seq}` : '';
      const rows = await (await api(`/v1/conversations/${item.id}/messages${before}`)).json();
      if (selected.current !== item.id) return;
      setMenuOpen(false); setChat(item); setMessages(old => older ? [...rows, ...old] : rows); setMoreMessages(rows.length === 100);
    } catch(e) { setError(e.message); } finally { if (selected.current === item.id) setLoading(false); }
  }
  function newChat() { setMenuOpen(false); selected.current = null; setChat(null); setMessages([]); setMoreMessages(false); setPrompt(''); setError(''); }
  async function send(e) {
    e.preventDefault(); if (busy || !prompt.trim()) return;
    setBusy(true); setError(''); setPhase('Connecting');
    const text = prompt.trim(), requestId = crypto.randomUUID();
    const assistantId = crypto.randomUUID(); let accepted = false;
    abort.current = new AbortController();
    try {
      let current = chat;
      if (!current) {
        current = await (await api('/v1/conversations', {method:'POST', signal: abort.current.signal})).json();
        setChat(current); selected.current = current.id;
      }
      const response = await api(`/v1/conversations/${current.id}/messages`, {
        method:'POST', body:JSON.stringify({request_id:requestId, content:text}), signal:abort.current.signal});
      accepted = true; setPrompt('');
      setMessages(old => [...old, {id:requestId, role:'user', content:text}, {id:assistantId, role:'assistant', content:'', pending:true}]);
      await readEvents(response.body, (event, data) => {
        if (event === 'queued') setPhase(data.position ? `Queued · ${data.position} ahead` : 'Preparing');
        if (event === 'starting') setPhase('Starting model… The first response may take a few minutes');
        if (event === 'started') setPhase('Generating');
        if (event === 'token') setMessages(old => old.map(m => m.id === assistantId ? {...m, content:m.content + data.text} : m));
        if (event === 'done') {
          setMessages(old => old.map(m => m.id === assistantId ? {...m, pending:false} : m));
          setPhase(data.finish_reason === 'length' ? 'Response limit reached' : 'Saved');
        }
        if (event === 'error') throw new Error(data.message);
      });
    } catch(e) {
      setError(e.name === 'AbortError' ? 'Generation stopped. Partial text is not saved; reload history to check the final state.' : e.message);
      if (accepted) setMessages(old => old.map(m => m.id === assistantId ? {...m, pending:false, incomplete:true} : m));
      setPhase('');
    } finally {
      abort.current = null; setBusy(false);
      await refresh().catch(e => setError(e.message));
    }
  }
  useEffect(() => { bottom.current?.scrollIntoView({behavior:'smooth'}); }, [messages.length, phase]);
  useEffect(() => () => abort.current?.abort(), []);

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
  </aside><main><header><button className="menu-toggle" aria-label="Toggle conversations" aria-expanded={menuOpen} onClick={()=>setMenuOpen(!menuOpen)}>☰</button><div><strong>{chat?.title || 'New conversation'}</strong><span>Private workspace</span></div><div className="status"><i className={model?.ready?'online':''}/>{model?.mode==='on-demand'?'Starts on demand':model?.ready?'Model ready':'Model unavailable'}</div></header>
    <section className="conversation" aria-label="Messages">
      {moreMessages && <button disabled={loading} onClick={()=>openChat(chat,true)}>Load earlier messages</button>}
      {!messages.length && <div className="welcome"><span className="spark">✳</span><div className="eyebrow">SPACE FOR YOUR NEXT IDEA</div><h1>What’s on your mind?</h1><p>Ask a question, work through a problem,<br/>or start somewhere unexpected.</p>
        <div className="suggestions">{['Explain a tricky concept','Help me think through an idea','Give me a writing prompt'].map(t=><button key={t} onClick={()=>setPrompt(t)}>{t}<span>↗</span></button>)}</div></div>}
      {messages.map(m=><article key={m.id} className={`message ${m.role}`}><div className="message-label">{m.role==='user'?'YOU':'STREAM'}{m.incomplete || (m.run_status && m.run_status!=='done') ? <span> · {m.run_status || 'incomplete'}</span> : null}</div><div className="message-text">{m.content || (m.pending?'Thinking…':'')}{m.pending && m.content && <span className="cursor"/>}</div></article>)}<div ref={bottom}/>
    </section><div className="composer-area">{error && <div className="error" role="alert">{error}{chat && !busy && <button onClick={()=>openChat(chat)}>Reload history</button>}</div>}
      <form className="composer" onSubmit={send}><textarea aria-label="Message" placeholder="Message Stream…" maxLength={4096} value={prompt} disabled={busy || loading} onChange={e=>setPrompt(e.target.value)} onKeyDown={e=>{if(e.key==='Enter'&&!e.shiftKey&&!e.nativeEvent.isComposing){e.preventDefault();send(e)}}}/>
        <div className="composer-bottom"><span>{busy?phase:loading?'Loading…':phase || 'Shift + Enter for a new line'}</span>{busy?<button type="button" className="send" onClick={()=>abort.current?.abort()} aria-label="Stop generation">■</button>:<button className="send" disabled={!prompt.trim()||loading} aria-label="Send message">↑</button>}</div></form><p className="disclaimer">Answers can be imperfect. Check the details that matter.</p>
    </div></main></div>;
}
createRoot(document.getElementById('root')).render(<App/>);
