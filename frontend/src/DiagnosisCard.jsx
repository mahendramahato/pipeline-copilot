import { Alert, Wrench } from './Icons.jsx'

// Shown only when an investigation found a problem: the three things an on-call
// engineer acts on. (The full diagnosis, with verified evidence, still exists on
// the backend for the output guardrail and incident memory.)
export default function DiagnosisCard({ d }) {
  return (
    <section className={`diagnosis conf-${d.confidence}`}>
      <header className="dx-head">
        <span className="eyebrow"><Alert size={13} /> Diagnosis</span>
      </header>

      <div className="dx-section">
        <h4>Root cause</h4>
        <p>{d.root_cause}</p>
      </div>

      <div className="dx-grid">
        <div className="dx-section">
          <h4>Impact</h4>
          <p>{d.impact}</p>
        </div>
        <div className="dx-section fix">
          <h4><Wrench size={14} /> Suggested fix</h4>
          <p>{d.suggested_fix}</p>
        </div>
      </div>
    </section>
  )
}
