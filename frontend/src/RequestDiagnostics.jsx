import React, {useEffect, useState} from 'react';
import {duration} from './diagnostics';

function WorkerState({workers}) {
  if (!workers) return <>No current worker snapshot. The readiness result remains authoritative.</>;
  if (workers.unhealthy) return <>Runpod reports an unhealthy worker. Waiting for a healthy replacement.</>;
  if (workers.throttled) return <>GPU capacity is throttled. Waiting for Runpod to allocate capacity.</>;
  if (workers.initializing) return <>Worker initializing. Runpod groups image/model downloads and startup under this state.</>;
  if (workers.running) return <>Worker running. Waiting for the readiness probe to confirm vLLM is healthy.</>;
  if (workers.idle) return <>Worker idle / scaled down. Waiting for it to resume.</>;
  return <>No worker assigned yet. Waiting for scheduling and provisioning.</>;
}

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
  return <section className={`diagnostics ${trace.finishedAt != null ? 'settled' : 'live'}`} aria-label="Request diagnostics">
    <div className="diagnostic-heading"><div className="diagnostic-kicker">REQUEST DIAGNOSTICS</div><span>{trace.finishedAt != null ? 'Finished' : 'Live'}</span></div>
    <div className="diagnostic-current" role="status"><strong>{trace.title}</strong><p>{trace.detail}</p></div>
    <div className="diagnostic-summary"><span>Elapsed <b>{duration(Math.max(0, end - trace.startedAt))}</b></span><span>First token <b>{duration(trace.firstTokenAt == null ? null : trace.firstTokenAt - trace.startedAt)}</b></span><span>Chunks <b>{trace.chunks}</b></span></div>
    <details className="diagnostic-details"><summary>Debug metrics and event timeline · {trace.events.length} entries</summary>
    <dl className="diagnostic-stats">
      <div><dt>Total elapsed</dt><dd>{duration(Math.max(0, end - trace.startedAt))}</dd></div>
      <div><dt>First token to browser</dt><dd>{duration(trace.firstTokenAt == null ? null : trace.firstTokenAt - trace.startedAt)}</dd></div>
      <div><dt>Readiness attempts</dt><dd>{trace.attempts}</dd></div>
      <div><dt>Stream chunks</dt><dd>{trace.chunks}</dd></div>
      <div><dt>Output characters</dt><dd>{trace.characters}</dd></div>
      <div><dt>Stream throughput</dt><dd>{rate} <small>chunks/s</small></dd></div>
    </dl>
    <div className="diagnostic-connection">{trace.finishedAt != null ? 'Stream closed' : trace.lastServerAt ? `Last server event ${duration(recent)} ago` : 'Waiting for first server event'} · {trace.heartbeatCount} heartbeats{trace.lastHttpStatus ? ` · Last upstream HTTP ${trace.lastHttpStatus}` : ''}</div>
    {trace.finishedAt == null && recent > 20000 && <p className="diagnostic-warning">No server events for over 20 seconds. The connection may be stalled; completion has not been confirmed.</p>}
    {trace.workers && !trace.modelReadyAt && <div className="worker-observation"><strong>Runpod observation · {duration(Math.max(0, end - trace.workerObservedAt))} old</strong><p><WorkerState workers={trace.workers}/></p><div className="worker-counts">{Object.entries(trace.workers).map(([name, count]) => <span key={name}>{name} <b>{count}</b></span>)}</div><small>A snapshot of provider state, not a live GPU meter. Counts can lag; no progress percentage is available.</small></div>}
    <p className="token-accounting">{trace.usage?.completion_tokens != null ? `Model token usage: ${trace.usage.prompt_tokens} input · ${trace.usage.completion_tokens} output · ${trace.usage.total_tokens} total.` : `${trace.promptTokens != null ? `${trace.promptTokens} prompt tokens. ` : ''}Exact output token count arrives from vLLM at completion. Live counters show chunks, not tokens.`}</p>
    <details className="diagnostic-timeline" open><summary>Event timeline · {trace.events.length} entries</summary><ol>{trace.events.map((item, index) => <li key={index}><time>+{duration(item.at - trace.startedAt)}</time><div><strong>{item.title}</strong><p>{item.detail}</p><small>{item.source || 'Runpod'}{item.serverElapsed != null ? ` · server +${duration(item.serverElapsed)}` : ''}</small></div></li>)}</ol></details>
    <div className="diagnostic-id">Request <code>{trace.requestId}</code>{trace.model && <><br/>Model <code>{trace.model}</code></>}</div>
    <small className="diagnostic-footnote">Diagnostics stay in this tab for the latest request. Conversation text persists in Neon. Diagnostics do not keep the GPU awake after the request.</small>
    </details>
  </section>;
}
