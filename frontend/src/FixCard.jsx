import { Alert, Check, Cross, Wrench } from './Icons.jsx'

// One action as a readable line (same wording as remediation.describe on the backend)
export function describe(a) {
  if (a.action === 'clear_task') return `Re-run ${a.task_id} in ${a.run_id}`
  if (a.action === 'backfill_date') return `Backfill ${a.date}`
  if (a.action === 'restart_container') return `Restart container ${a.container}`
  return a.action
}

const OUTCOME = {
  resolved: { label: 'Resolved', tone: 'ok' },
  partially_resolved: { label: 'Partly resolved', tone: 'warn' },
  not_resolved: { label: 'Not resolved', tone: 'bad' },
}

// The fix half of an incident: proposed actions → human decision → execution → verification.
// plan: vetted actions. waiting: true while the run is paused for approval.
export default function FixCard({ plan, waiting, approval, remediation, progress, onDecide, deciding }) {
  if (!plan || (!plan.actions.length && !plan.note)) return null
  const outcome = remediation && OUTCOME[remediation.outcome]
  const status = outcome ? outcome
    : approval ? (approval.approved ? { label: deciding ? 'Running' : 'Approved', tone: 'accent' } : { label: 'Rejected', tone: 'muted' })
      : waiting ? { label: 'Needs your approval', tone: 'warn' }
        : plan.executable === false && plan.actions.length ? { label: 'Manual', tone: 'muted' }
          : null

  return (
    <section className="fixcard">
      <header className="dx-head">
        <span className="eyebrow"><Wrench size={13} /> {remediation ? 'Fix applied' : 'Proposed fix'}</span>
        {status && <span className={`pill tone-${status.tone}`}>{status.label}</span>}
      </header>

      {plan.actions.length > 0 && (
        <ol className="fix-steps">
          {plan.actions.map((a, i) => {
            const result = remediation?.results?.[i]
            return (
              <li key={i}>
                <div className="fix-line">
                  {result ? (result.verified ? <Check size={15} className="ok" /> : <Cross size={15} className="bad" />)
                    : <span className="step-n">{i + 1}</span>}
                  <strong>{describe(a)}</strong>
                  <span className={`risk risk-${a.risk}`}>{a.risk} risk</span>
                </div>
                <p className="muted">{a.why}</p>
                {result && <p className={result.verified ? 'verify ok' : 'verify bad'}>{result.verification}</p>}
              </li>
            )
          })}
        </ol>
      )}

      {plan.note && <p className="fix-note"><Alert size={14} /> {plan.note}</p>}

      {progress.length > 0 && !remediation && (
        <ul className="progress">{progress.map((p, i) => <li key={i}>{p.text}</li>)}</ul>
      )}

      {remediation?.still_failing?.length > 0 && (
        <div className="still-failing">
          <h4>Still failing</h4>
          <ul>{remediation.still_failing.map((c, i) => <li key={i}>{c.check}: {c.detail}</li>)}</ul>
        </div>
      )}

      {waiting && !approval && (
        onDecide ? (
          <div className="fix-actions">
            <button className="approve" onClick={() => onDecide(true)} disabled={deciding}>
              <Check size={15} /> Approve &amp; run
            </button>
            <button className="reject" onClick={() => onDecide(false)} disabled={deciding}>Reject</button>
            <span className="muted">Nothing runs until you approve. Each step is checked afterwards.</span>
          </div>
        ) : <p className="muted">Waiting for the owner to approve.</p>
      )}

      {approval?.note && <p className="muted">Note: {approval.note}</p>}
    </section>
  )
}
