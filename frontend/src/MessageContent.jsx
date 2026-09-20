import React from 'react';
import Markdown from 'react-markdown';

// Model output is untrusted: no raw HTML, remote image requests, or executable URLs.
export default function MessageContent({text}) {
  return <div className="markdown"><Markdown skipHtml components={{
    img: ({alt}) => <span>[Image: {alt || 'not loaded'}]</span>,
    a: ({href, children}) => <a href={href} target="_blank" rel="noopener noreferrer">{children}</a>,
  }}>{text}</Markdown></div>;
}
