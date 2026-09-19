import React, {useEffect, useState} from 'react';

export function mergeToolStep(previous, next) {
  const index = previous.findIndex(s => s.run_id === next.run_id && s.step === next.step);
  return index < 0 ? [...previous, next] : previous.map((s, i) => i === index ? {...s, ...next} : s);
}

export function ToolActivity({steps}) {
  if (!steps.length) return null;
  return <section className="tool-activity" aria-label="Code execution">
    {steps.map(step => <details key={`${step.run_id}-${step.step}`} open={step.status === 'running'}>
      <summary><span>{step.name === 'python' ? 'Python' : step.name === 'terminal' ? 'Terminal' : 'Save file'} · step {step.step}</span><b>{step.status}</b></summary>
      <pre>{Object.values(step.arguments || {}).join('\n')}</pre>
      {step.result && <pre className="tool-result">{step.result}</pre>}
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
