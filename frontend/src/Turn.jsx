import { useEffect, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import DiagnosisCard from './DiagnosisCard.jsx'
import { Alert, Check, Chevron, Clock } from './Icons.jsx'
import Robot3D from './Robot3D.jsx'

const SERVER_LABELS = { airflow: 'Airflow', athena: 'Athena', knowledge: 'Knowledge', agent: 'Agent' }

// Pair each tool call with its result (results arrive in call order per tool name)
function buildSteps(events) {
  const steps = []
  for (const ev of events) {
    if (ev.type === 'tool_call') steps.push({ name: ev.name, args: ev.args, server: ev.server ?? 'agent', chars: null })
    if (ev.type === 'tool_result') {
      const step = steps.find((s) => s.name === ev.name && s.chars === null)
      if (step) step.chars = ev.chars
    }
  }
  return steps
}

function argsPreview(args) {
  const text = Object.entries(args).map(([k, v]) => `${k}=${JSON.stringify(v)}`).join('  ')
  return text.length > 160 ? text.slice(0, 160) + '…' : text
}

const fmtSeconds = (ms) => (ms < 60000 ? `${Math.round(ms / 1000)}s` : `${Math.floor(ms / 60000)}m ${Math.round((ms % 60000) / 1000)}s`)

// Ticking clock while a turn runs; fixed duration once it's done
function useElapsed(startedAt, finishedAt) {
  const [now, setNow] = useState(Date.now())
  useEffect(() => {
    if (!startedAt || finishedAt) return
    const id = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(id)
  }, [startedAt, finishedAt])
  return startedAt ? (finishedAt ?? now) - startedAt : null
}

// One question and everything the agent did to answer it
export default function Turn({ turn, toolServers }) {
  const steps = buildSteps(turn.events)
  const answers = turn.events.filter((e) => e.type === 'answer')
  const diagnosis = turn.events.find((e) => e.type === 'diagnosis')
  // Show the diagnosis card only when the investigation actually found a problem
  const problem = diagnosis && diagnosis.diagnosis.category !== 'no_problem_found' ? diagnosis.diagnosis : null
  const errors = turn.events.filter((e) => e.type === 'error')
  const elapsed = useElapsed(turn.startedAt, turn.finishedAt)
  const sources = [...new Set(steps.map((s) => SERVER_LABELS[s.server] ?? s.server))]
  const [open, setOpen] = useState(false)
  const showSteps = turn.pending || open

  return (
    <article className="turn">
      <div className="question">{turn.question}</div>

      <div className="reply">
        <div className="avatar"><Robot3D size={28} /></div>
        <div className="reply-body">
          {steps.length > 0 && (
            <div className={`steps ${turn.pending ? 'live' : ''}`}>
              <button className="steps-head" onClick={() => setOpen((o) => !o)} disabled={turn.pending}>
                {!turn.pending && <Chevron size={14} className={`chev ${showSteps ? 'open' : ''}`} />}
                <span>
                  {turn.pending ? 'Investigating' : 'Investigated'} · {steps.length} tool call{steps.length === 1 ? '' : 's'}
                  {!turn.pending && sources.length > 0 && <span className="muted"> across {sources.join(', ')}</span>}
                </span>
                {elapsed !== null && <span className="timer"><Clock size={13} /> {fmtSeconds(elapsed)}</span>}
              </button>
              {showSteps && (
                <ol className="timeline">
                  {steps.map((s, i) => (
                    <li key={i} className={s.chars === null ? 'running' : 'done'}>
                      <span className={`dot s-${s.server}`} />
                      <span className={`src s-${s.server}`}>{SERVER_LABELS[s.server] ?? s.server}</span>
                      <code className="tname">{s.name}</code>
                      <span className="targs">{argsPreview(s.args)}</span>
                      <span className="tstat">
                        {s.chars === null ? <span className="spinner" /> : <><Check size={13} /> {s.chars.toLocaleString()} chars</>}
                      </span>
                    </li>
                  ))}
                </ol>
              )}
            </div>
          )}

          {turn.pending && steps.length === 0 && answers.length === 0 && (
            <p className="thinking">Reading your question<span className="dots"><i /><i /><i /></span></p>
          )}

          {answers.map((a, i) => (
            <div key={i} className="answer">
              <ReactMarkdown remarkPlugins={[remarkGfm]}>{a.text}</ReactMarkdown>
            </div>
          ))}

          {problem && <DiagnosisCard d={problem} />}
          {turn.pending && answers.length > 0 && !diagnosis && (
            <p className="thinking">Checking the findings<span className="dots"><i /><i /><i /></span></p>
          )}

          {errors.map((e, i) => <p key={i} className="error"><Alert size={15} /> {e.message}</p>)}
        </div>
      </div>
    </article>
  )
}
