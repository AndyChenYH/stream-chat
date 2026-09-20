import React, {useEffect, useState} from 'react';

export function readableResult(raw) {
  try {
    const result=JSON.parse(raw);
    if ('stdout' in result || 'stderr' in result || 'error' in result) {
      return [result.exit_code != null ? `Exit code: ${result.exit_code}` : '', result.stdout,
        result.stderr && `stderr:\n${result.stderr}`, ...(result.results || []),
        result.error && `Error: ${result.error}`, result.message].filter(Boolean).join('\n') || '(No output)';
    }
    return JSON.stringify(result,null,2);
  } catch { return raw; }
}

export function executionStatus(step) {
  if (step.status !== 'done' || !step.result) return step.status;
  try {
    const result=JSON.parse(step.result);
    return result.error || (result.exit_code != null && result.exit_code !== 0) ? 'error' : 'done';
  } catch { return step.status; }
}

export function mergeToolStep(previous, next) {
  const index = previous.findIndex(s => s.run_id === next.run_id && s.step === next.step);
  return index < 0 ? [...previous, next] : previous.map((s, i) => i === index ? {...s, ...next} : s);
}

export function ToolActivity({steps}) {
  if (!steps.length) return null;
  return <section className="tool-activity" aria-label="Agent tool activity">
    <div className="activity-label">TOOL CALLS · {steps.length}</div>
    {steps.map(step => <details key={`${step.run_id}-${step.step}`} data-status={executionStatus(step)} open={['running','error','failed'].includes(executionStatus(step))}>
      <summary><span>{step.name === 'python' ? 'Python' : step.name === 'terminal' ? 'Terminal' : 'Save file'} · step {step.step}</span><b>{executionStatus(step) === 'error' ? 'Code error' : executionStatus(step)}</b></summary>
      <div className="tool-section-label">{step.name === 'publish_file' ? 'File' : 'Code / command'}</div>
      <pre>{Object.values(step.arguments || {}).join('\n')}</pre>
      {step.result && <><div className="tool-section-label">Result</div><pre className="tool-result">{readableResult(step.result)}</pre></>}
    </details>)}
  </section>;
}

export function Artifact({file, api}) {
  const [url, setUrl] = useState(null), [error, setError] = useState('');
  useEffect(() => {
    let active = true, created;
    if (file.mime_type === 'image/png') api(`/v1/files/${file.id}`).then(r=>r.blob()).then(blob=>{
      created = URL.createObjectURL(blob);
      if (active) setUrl(created); else URL.revokeObjectURL(created);
    }).catch(e=>{if (active) setError(e.message)});
    return () => {active = false; if (created) URL.revokeObjectURL(created)};
  }, [file.id]);
  async function download() {
    try {
      const blob = await (await api(`/v1/files/${file.id}`)).blob();
      const link = document.createElement('a'), objectUrl = URL.createObjectURL(blob);
      link.href = objectUrl; link.download = file.name; link.click();
      setTimeout(()=>URL.revokeObjectURL(objectUrl),1000);
    } catch(e) { setError(e.message); }
  }
  return <div className="artifact">{url && <img src={url} alt={file.name}/>}
    <button type="button" onClick={download}>↓ {file.name} <small>{Math.ceil(file.size/1024)} KB</small></button>
    {error && <small role="alert">{error}</small>}
  </div>;
}
