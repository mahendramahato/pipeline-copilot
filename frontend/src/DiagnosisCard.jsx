import { Alert, Book, Check, Cross, Database, Wrench } from './Icons.jsx'

const pretty = (s) => s.replaceAll('_', ' ')

// The structured Diagnosis, after the output guardrail (every evidence quote was
// checked against real tool output) and incident memory.
export default function DiagnosisCard({ d, toolServers }) {
  const g = d.grounding ?? {}
  const total = g.checked ?? d.evidence.length
  const verified = total - (g.ungrounded ?? 0)
  const pct = total ? Math.round((verified / total) * 100) : 0

  return (
    <section className={`diagnosis conf-${d.confidence}`}>
      <header className="dx-head">
        <div>
          <span className="eyebrow">Diagnosis</span>
          <h3>{pretty(d.category)}</h3>
        </div>
        <span className={`pill conf conf-${d.confidence}`}>
          {d.confidence} confidence
          {g.confidence_lowered_from && <span className="lowered"> · lowered from {g.confidence_lowered_from}</span>}
        </span>
      </header>

      <p className="dx-summary">{d.summary}</p>

      <div className="meter" title="Evidence quotes found verbatim in real tool output">
        <div className="meter-bar"><span style={{ width: `${pct}%` }} className={pct === 100 ? 'full' : ''} /></div>
        <span className="meter-label">
          <strong>{verified}/{total}</strong> evidence quotes verified against tool output
        </span>
      </div>

      <div className="dx-section">
        <h4>Root cause</h4>
        <p>{d.root_cause}</p>
      </div>

      <div className="dx-section">
        <h4>Evidence</h4>
        <ul className="evidence">
          {d.evidence.map((e, i) => (
            <li key={i} className={e.grounded ? 'ok' : 'bad'}>
              <span className="ev-mark" title={e.grounded ? 'Found verbatim in tool output' : 'NOT found in tool output'}>
                {e.grounded ? <Check size={13} /> : <Cross size={13} />}
              </span>
              <div className="ev-body">
                <span className={`tool-chip s-${toolServers?.[e.tool] ?? 'agent'}`}>{e.tool}</span>
                <code className="quote">{e.quote}</code>
                <p className="muted">{e.meaning}</p>
              </div>
            </li>
          ))}
        </ul>
      </div>

      <div className="dx-grid">
        <div className="dx-section">
          <h4>Impact</h4>
          <p>{d.impact}</p>
        </div>
        <div className="dx-section fix">
          <h4><Wrench size={14} /> Suggested fix <span className="tag">for a human to apply</span></h4>
          <p>{d.suggested_fix}</p>
        </div>
      </div>

      {d.runbooks_used?.length > 0 && (
        <div className="chips">
          {d.runbooks_used.map((r) => <span key={r} className="chip"><Book size={12} /> {r}</span>)}
        </div>
      )}

      {d.unverified?.length > 0 && (
        <div className="dx-section unverified">
          <h4><Alert size={14} /> Not verified</h4>
          <ul>{d.unverified.map((u, i) => <li key={i}>{u}</li>)}</ul>
        </div>
      )}

      {d.memory && <footer className="dx-foot"><Database size={13} /> {d.memory}</footer>}
    </section>
  )
}
