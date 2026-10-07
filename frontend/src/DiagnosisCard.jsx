// The structured Diagnosis, after the output guardrail (each evidence quote checked
// against real tool output) and incident memory.
export default function DiagnosisCard({ d }) {
  const g = d.grounding ?? {}
  const verified = (g.checked ?? 0) - (g.ungrounded ?? 0)

  return (
    <section className="diagnosis">
      <header>
        <span className="label">Diagnosis</span>
        <span className="badge">{d.category.replaceAll('_', ' ')}</span>
        <span className={`badge conf-${d.confidence}`}>confidence: {d.confidence}</span>
        {g.confidence_lowered_from && (
          <span className="badge warn">lowered from {g.confidence_lowered_from}</span>
        )}
      </header>

      <p className="summary">{d.summary}</p>

      <h4>Root cause</h4>
      <p>{d.root_cause}</p>

      <h4>
        Evidence <span className="muted">· {verified}/{g.checked ?? 0} quotes verified against tool output</span>
      </h4>
      <ul className="evidence">
        {d.evidence.map((e, i) => (
          <li key={i} className={e.grounded ? 'ok' : 'bad'}>
            <span className="mark" title={e.grounded ? 'Found verbatim in tool output' : 'NOT found in tool output'}>
              {e.grounded ? '✓' : '✗'}
            </span>
            <div>
              <code className="tool">{e.tool}</code> <q>{e.quote}</q>
              <div className="muted">{e.meaning}</div>
            </div>
          </li>
        ))}
      </ul>

      <h4>Impact</h4>
      <p>{d.impact}</p>

      <h4>Suggested fix <span className="muted">· for a human to apply</span></h4>
      <p>{d.suggested_fix}</p>

      {d.runbooks_used?.length > 0 && (
        <p className="chips">
          {d.runbooks_used.map((r) => <span key={r} className="chip">{r}</span>)}
        </p>
      )}

      {d.unverified?.length > 0 && (
        <>
          <h4>Not verified</h4>
          <ul className="unverified">
            {d.unverified.map((u, i) => <li key={i}>{u}</li>)}
          </ul>
        </>
      )}

      {d.memory && <footer className="muted">Incident memory: {d.memory}</footer>}
    </section>
  )
}
