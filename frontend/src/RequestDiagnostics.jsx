import React, {useEffect, useState} from 'react';
import {duration} from './diagnostics';

export default function Diagnostics({trace}) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    setNow(Date.now());
    if (trace.finishedAt != null) return;
    const timer = setInterval(() => setNow(Date.now()), 250);
    return () => clearInterval(timer);
  }, [trace.requestId, trace.finishedAt]);
  const end = trace.finishedAt ?? now;
  const streamingMs = trace.firstTokenAt != null ? (trace.lastTokenAt - trace.firstTokenAt) : 0;
  const rate = streamingMs > 0 ? ((trace.chunks - 1) / (streamingMs / 1000)).toFixed(1) : '—';
  const recent = trace.lastServerAt ? Math.max(0, now - trace.lastServerAt) : null;
  const retrySeconds = trace.stage === 'model_retry' && trace.retryAt ? Math.max(0, Math.ceil((trace.retryAt-now)/1000)) : null;
  const title = retrySeconds == null ? trace.title : retrySeconds ? `Rate limited · retrying in ${retrySeconds}s` : 'Resuming inference…';
  return <section className={`diagnostics ${trace.finishedAt != null ? 'settled' : 'live'}`} aria-label="Agent progress">
    <div className="diagnostic-heading"><div className="diagnostic-kicker">AGENT ACTIVITY</div><span>{trace.finishedAt != null ? 'Finished' : 'Live'}</span></div>
    <div className="diagnostic-current" role="status"><strong>{title}</strong><p>{trace.detail}</p></div>
    <div className="diagnostic-summary"><span>Elapsed <b>{duration(Math.max(0, end - trace.startedAt))}</b></span><span>Model calls <b>{trace.modelCalls}</b></span><span>Retries <b>{trace.retries}</b></span></div>
    <details className="diagnostic-details"><summary>Debug metrics and event timeline · {trace.events.length} entries</summary>
    <dl className="diagnostic-stats">
      <div><dt>Total elapsed</dt><dd>{duration(Math.max(0, end - trace.startedAt))}</dd></div>
      <div><dt>First token to browser</dt><dd>{duration(trace.firstTokenAt == null ? null : trace.firstTokenAt - trace.startedAt)}</dd></div>
      <div><dt>Model calls / reconnects</dt><dd>{trace.modelCalls} / {trace.reconnects}</dd></div>
      <div><dt>Stream chunks</dt><dd>{trace.chunks}</dd></div>
      <div><dt>Output characters</dt><dd>{trace.characters}</dd></div>
      <div><dt>Stream throughput</dt><dd>{rate} <small>chunks/s</small></dd></div>
    </dl>
    <div className="diagnostic-connection">{trace.finishedAt != null ? 'Stream closed' : trace.lastServerAt ? `Last server event ${duration(recent)} ago` : 'Waiting for first server event'} · {trace.heartbeatCount} heartbeats{trace.lastHttpStatus ? ` · Last upstream HTTP ${trace.lastHttpStatus}` : ''}</div>
    {trace.finishedAt == null && recent > 20000 && <p className="diagnostic-warning">No server events for over 20 seconds. The connection may be stalled; completion has not been confirmed.</p>}
    <p className="token-accounting">{trace.usage?.completion_tokens != null ? `Model token usage: ${trace.usage.prompt_tokens} input · ${trace.usage.completion_tokens} output · ${trace.usage.total_tokens} total.` : 'Exact token usage arrives from Vercel AI Gateway at completion. Live counters show chunks, not tokens.'}</p>
    <details className="diagnostic-timeline" open><summary>Event timeline · {trace.events.length} entries</summary><ol>{trace.events.map((item, index) => <li key={index}><time>+{duration(item.at - trace.startedAt)}</time><div><strong>{item.title}</strong><p>{item.detail}</p><small>{item.source || 'Fly / gateway'}{item.serverElapsed != null ? ` · server +${duration(item.serverElapsed)}` : ''}</small></div></li>)}</ol></details>
    <div className="diagnostic-id">Request <code>{trace.requestId}</code>{trace.model && <><br/>Model <code>{trace.model}</code></>}</div>
    <small className="diagnostic-footnote">Live execution events, not private reasoning. Tool code and results remain in conversation history.</small>
    </details>
  </section>;
}
