import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import DiagnosisCard from './DiagnosisCard.jsx'

// Pair each tool call with its result (results arrive in call order per tool name)
function buildSteps(events) {
  const steps = []
  for (const ev of events) {
    if (ev.type === 'tool_call') steps.push({ name: ev.name, args: ev.args, chars: null })
    if (ev.type === 'tool_result') {
      const step = steps.find((s) => s.name === ev.name && s.chars === null)
      if (step) step.chars = ev.chars
    }
  }
  return steps
}

function argsPreview(args) {
  const text = Object.entries(args).map(([k, v]) => `${k}=${JSON.stringify(v)}`).join(', ')
  return text.length > 140 ? text.slice(0, 140) + '…' : text
}

// One question and everything the agent did to answer it
export default function Turn({ turn }) {
  const steps = buildSteps(turn.events)
  const answers = turn.events.filter((e) => e.type === 'answer')
  const diagnosis = turn.events.find((e) => e.type === 'diagnosis')
  const errors = turn.events.filter((e) => e.type === 'error')
  const running = steps.filter((s) => s.chars === null).length

  return (
    <article className="turn">
      <div className="question">{turn.question}</div>

      {steps.length > 0 && (
        <details className="steps" open={turn.pending}>
          <summary>
            {turn.pending ? 'Investigating' : 'Investigated'} · {steps.length} tool call{steps.length === 1 ? '' : 's'}
            {running > 0 && <span className="spinner" aria-label="running" />}
          </summary>
          <ol>
            {steps.map((s, i) => (
              <li key={i}>
                <span className={s.chars === null ? 'pending' : 'done'}>{s.chars === null ? '…' : '✓'}</span>
                <code>{s.name}</code>
                <span className="muted args">{argsPreview(s.args)}</span>
                {s.chars !== null && <span className="muted"> · {s.chars} chars</span>}
              </li>
            ))}
          </ol>
        </details>
      )}

      {turn.pending && steps.length === 0 && answers.length === 0 && (
        <p className="muted">Thinking<span className="spinner" /></p>
      )}

      {answers.map((a, i) => (
        <div key={i} className="answer">
          <ReactMarkdown remarkPlugins={[remarkGfm]}>{a.text}</ReactMarkdown>
        </div>
      ))}

      {diagnosis && <DiagnosisCard d={diagnosis.diagnosis} />}
      {turn.pending && answers.length > 0 && !diagnosis && (
        <p className="muted">Writing the structured diagnosis<span className="spinner" /></p>
      )}

      {errors.map((e, i) => <p key={i} className="error">✗ {e.message}</p>)}
    </article>
  )
}
