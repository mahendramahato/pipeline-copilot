import { useEffect, useMemo, useRef, useState } from 'react'
import { getHealth, getMonitor, getShowcase, getShowcaseList, getThread, getThreads, logout, runMonitor, streamChat } from './api.js'
import { Book, Lock, Logo, Plus, Send } from './Icons.jsx'
import LoginModal from './LoginModal.jsx'
import Turn from './Turn.jsx'

const REPO_URL = 'https://github.com/mahendramahato/pipeline-copilot'

const EXAMPLES = [
  { tag: 'Freshness', q: 'Is the curated data up to date?' },
  { tag: 'Airflow', q: 'When did daily_lake_maintenance last succeed?' },
  { tag: 'Data', q: 'How many earthquakes were recorded yesterday?' },
  { tag: 'Schema', q: 'Has the raw_seismic schema changed recently?' },
]

// thread ids look like chat-YYYYMMDD-HHMMSS (UTC): group them by day for the sidebar
function threadDate(id) {
  const m = id.match(/^chat-(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})/)
  return m ? new Date(Date.UTC(+m[1], m[2] - 1, +m[3], +m[4], +m[5])) : null
}

function groupThreads(threads) {
  const startOfToday = new Date(); startOfToday.setHours(0, 0, 0, 0)
  const startOfYesterday = new Date(startOfToday); startOfYesterday.setDate(startOfToday.getDate() - 1)
  const groups = { Today: [], Yesterday: [], Earlier: [] }
  for (const t of threads) {
    const d = threadDate(t.thread_id)
    const key = !d ? 'Earlier' : d >= startOfToday ? 'Today' : d >= startOfYesterday ? 'Yesterday' : 'Earlier'
    groups[key].push({ ...t, time: d })
  }
  return Object.entries(groups).filter(([, list]) => list.length)
}

const fmtDate = (iso) => new Date(iso).toLocaleDateString([], { year: 'numeric', month: 'short', day: 'numeric' })

export default function App() {
  const [health, setHealth] = useState(null)
  const [showcase, setShowcase] = useState([])
  const [threads, setThreads] = useState([])
  // What the main panel shows: a recorded investigation, or the live chat
  const [view, setView] = useState({ kind: 'home' })    // home | showcase | chat
  const [threadId, setThreadId] = useState(null)         // live chat: null = new conversation
  const [turns, setTurns] = useState([])
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)
  const [showLogin, setShowLogin] = useState(false)
  const [monitor, setMonitor] = useState(null)
  const [checking, setChecking] = useState(false)
  const bottomRef = useRef(null)
  const inputRef = useRef(null)

  const owner = Boolean(health?.owner)

  const refreshThreads = () => getThreads().then(setThreads).catch(() => setThreads([]))
  const refreshMonitor = () => getMonitor().then(setMonitor).catch(() => setMonitor(null))

  async function loadSession() {
    try {
      const h = await getHealth()
      setHealth(h)
      if (h.owner) {
        refreshThreads()
        refreshMonitor()
        setView((v) => (v.kind === 'home' ? { kind: 'chat' } : v))
      }
    } catch (e) {
      setError(`API not reachable: ${e.message}`)
    }
  }

  useEffect(() => {
    loadSession()
    getShowcaseList().then(setShowcase).catch(() => {})
  }, [])

  // While signed in, refresh the health monitor's status every 2 minutes
  useEffect(() => {
    if (!owner) return
    const id = setInterval(refreshMonitor, 120000)
    return () => clearInterval(id)
  }, [owner])

  async function checkNow() {
    setChecking(true)
    try { setMonitor(await runMonitor()); refreshThreads() } catch (e) { setError(e.message) } finally { setChecking(false) }
  }

  // Keep the newest step in view while the agent works
  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' }) }, [turns])

  // Grow the composer with its content (up to a limit set in CSS)
  useEffect(() => {
    const el = inputRef.current
    if (el) { el.style.height = 'auto'; el.style.height = `${el.scrollHeight}px` }
  }, [input])

  // tool name -> MCP server, for coloring evidence chips
  const toolServers = useMemo(() => {
    const map = {}
    for (const [server, tools] of Object.entries(health?.servers ?? {})) for (const t of tools) map[t] = server
    return map
  }, [health])

  const groups = useMemo(() => groupThreads(threads), [threads])

  async function openShowcase(slug) {
    if (busy) return
    setError(null)
    try {
      const item = await getShowcase(slug)
      setView({ kind: 'showcase', item })
      setTurns(item.turns)
    } catch (e) {
      setError(e.message)
    }
  }

  async function openThread(id) {
    if (busy) return
    setError(null)
    try {
      const data = await getThread(id)
      setView({ kind: 'chat' })
      setThreadId(id)
      setTurns(data.turns)
    } catch (e) {
      setError(e.message)
    }
  }

  function newChat() {
    if (busy) return
    setView({ kind: 'chat' })
    setThreadId(null)
    setTurns([])
    setError(null)
    setTimeout(() => inputRef.current?.focus(), 0)
  }

  async function signOut() {
    await logout().catch(() => {})
    setThreads([])
    setThreadId(null)
    setTurns([])
    setView({ kind: 'home' })
    loadSession()
  }

  const updateLastTurn = (fn) => setTurns((ts) => ts.map((t, i) => (i === ts.length - 1 ? fn(t) : t)))

  async function send(text) {
    const message = text.trim()
    if (!message || busy) return
    setInput('')
    setError(null)
    setBusy(true)
    setTurns((ts) => [...ts, { question: message, events: [], pending: true, startedAt: Date.now() }])
    try {
      await streamChat({
        message,
        threadId,
        onEvent: (ev) => {
          if (ev.type === 'thread') setThreadId(ev.thread_id)
          else if (ev.type !== 'done') updateLastTurn((t) => ({ ...t, events: [...t.events, ev] }))
        },
      })
    } catch (e) {
      updateLastTurn((t) => ({ ...t, events: [...t.events, { type: 'error', message: e.message }] }))
    } finally {
      updateLastTurn((t) => ({ ...t, pending: false, finishedAt: Date.now() }))
      setBusy(false)
      refreshThreads()
    }
  }

  const title =
    view.kind === 'showcase' ? view.item.title
      : view.kind === 'chat' ? (turns[0]?.question ?? 'New conversation')
        : 'Pipeline Copilot'

  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="brand" onClick={() => !busy && setView(owner ? { kind: 'chat' } : { kind: 'home' })}>
          <Logo size={34} />
          <div>
            <div className="brand-name">Pipeline Copilot</div>
            <div className="brand-sub">On-call for data pipelines</div>
          </div>
        </div>

        {owner && <button className="new" onClick={newChat} disabled={busy}><Plus size={16} /> New conversation</button>}

        <nav>
          {showcase.length > 0 && (
            <div className="group">
              <div className="group-label">Featured investigations</div>
              {showcase.map((s) => (
                <button
                  key={s.slug}
                  className={`thread featured ${view.kind === 'showcase' && view.item.slug === s.slug ? 'active' : ''}`}
                  onClick={() => openShowcase(s.slug)}
                  disabled={busy}
                  title={s.title}
                >
                  <Book size={13} />
                  <span className="thread-title">{s.title}</span>
                </button>
              ))}
            </div>
          )}

          {owner && groups.map(([label, list]) => (
            <div key={label} className="group">
              <div className="group-label">{label}</div>
              {list.map((t) => (
                <button
                  key={t.thread_id}
                  className={`thread ${view.kind === 'chat' && t.thread_id === threadId ? 'active' : ''}`}
                  onClick={() => openThread(t.thread_id)}
                  disabled={busy}
                  title={t.thread_id}
                >
                  <span className="thread-title">{t.title}</span>
                  {t.time && (
                    <span className="thread-time">
                      {t.time.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                    </span>
                  )}
                </button>
              ))}
            </div>
          ))}
        </nav>

        <div className="status-card">
          <div className="status-row">
            <span className={`status-dot ${health ? 'on' : 'off'}`} />
            {health ? (owner ? 'Live · signed in' : 'Online') : 'API offline'}
            {health && <span className="model">{health.model}</span>}
          </div>
          {health && (
            <div className="servers">
              {Object.entries(health.servers ?? {}).map(([server, tools]) => (
                <span key={server} className={`server s-${server}`}>{server} · {tools.length}</span>
              ))}
            </div>
          )}
          {owner && monitor?.enabled && (() => {
            const failing = monitor.checks.filter((c) => !c.ok)
            return (
              <div className={`monitor ${failing.length ? 'bad' : 'ok'}`}>
                <div className="monitor-row">
                  <span>{monitor.checked_at
                    ? (failing.length ? `⚠ ${failing.length} of ${monitor.checks.length} checks failing`
                      : `✓ All ${monitor.checks.length} health checks passing`)
                    : 'Health monitor starting…'}</span>
                  <button className="link" onClick={checkNow} disabled={checking || busy}>{checking ? 'Checking…' : 'Check now'}</button>
                </div>
                {monitor.checked_at && (
                  <div className="monitor-time">
                    checked {new Date(monitor.checked_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                    {' '}· every {monitor.interval_minutes} min
                  </div>
                )}
                {failing.map((c) => <div key={c.check} className="monitor-fail" title={c.detail}>✗ {c.check}</div>)}
                {monitor.last_investigation && (
                  <button className="link" onClick={() => openThread(monitor.last_investigation.thread_id)} disabled={busy}>
                    Open last automatic investigation
                  </button>
                )}
              </div>
            )
          })()}
          <div className="readonly"><Lock size={12} /> Read-only: it investigates, you decide</div>
          {health?.auth_enabled && (
            owner
              ? <button className="link" onClick={signOut} disabled={busy}>Sign out</button>
              : <button className="signin" onClick={() => setShowLogin(true)}>Owner sign-in for live questions</button>
          )}
        </div>
      </aside>

      <main>
        <header className="topbar">
          <h2 title={title}>{title}</h2>
          {view.kind === 'showcase' && (
            <span className="replay-badge">Recorded {fmtDate(view.item.recorded)} · read-only replay</span>
          )}
          {view.kind === 'chat' && threadId && <span className="thread-id">{threadId}</span>}
        </header>

        <div className="conversation">
          {view.kind === 'home' && (
            <div className="empty landing">
              <Logo size={56} />
              <h1>An AI on-call assistant for data pipelines</h1>
              <p>
                It investigates the way an on-call engineer would: reading Airflow runs and logs,
                querying the data lake, checking the team's runbooks and past incidents. Then it
                returns a structured diagnosis in which every piece of evidence is verified against
                real tool output. All access is read-only.
              </p>
              {showcase.length > 0 && (
                <>
                  <h3 className="landing-sub">Replay a real investigation</h3>
                  <div className="showcase-cards">
                    {showcase.map((s) => (
                      <button key={s.slug} onClick={() => openShowcase(s.slug)}>
                        <span className="ex-tag">Recorded {fmtDate(s.recorded)}</span>
                        <strong>{s.title}</strong>
                        <span className="muted">{s.summary}</span>
                      </button>
                    ))}
                  </div>
                </>
              )}
              <p className="landing-links">
                <a href={REPO_URL} target="_blank" rel="noreferrer">Source, architecture and evals on GitHub →</a>
              </p>
            </div>
          )}

          {view.kind === 'chat' && turns.length === 0 && (
            <div className="empty">
              <Logo size={56} />
              <h1>What looks wrong in your pipeline?</h1>
              <p>
                Ask about DAG runs, task logs, tables or data quality. Pipeline Copilot investigates with
                read-only tools, checks the runbooks, and verifies every piece of evidence it cites.
              </p>
              <div className="examples">
                {EXAMPLES.map(({ tag, q }) => (
                  <button key={q} onClick={() => send(q)} disabled={busy || !health}>
                    <span className="ex-tag">{tag}</span>
                    <span>{q}</span>
                  </button>
                ))}
              </div>
            </div>
          )}

          {view.kind !== 'home' && turns.map((t, i) => <Turn key={i} turn={t} toolServers={toolServers} />)}
          {error && <p className="error banner">{error}</p>}
          <div ref={bottomRef} />
        </div>

        {view.kind === 'chat' && owner ? (
          <form className="composer" onSubmit={(e) => { e.preventDefault(); send(input) }}>
            <div className="composer-box">
              <textarea
                ref={inputRef}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={(e) => {
                  // Enter sends, Shift+Enter adds a new line
                  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(input) }
                }}
                placeholder={busy ? 'Investigating…' : 'Ask about your pipeline…'}
                rows={1}
                maxLength={4000}
                disabled={busy}
              />
              <button type="submit" disabled={busy || !input.trim()} aria-label="Send"><Send size={18} /></button>
            </div>
            <p className="hint">Enter to send · Shift+Enter for a new line · answers can take a minute on real incidents</p>
          </form>
        ) : (
          view.kind === 'showcase' && (
            <div className="replay-note">
              This is a recorded investigation, shown exactly as it ran.
              {health?.auth_enabled && !owner && <> Live questions are limited to the owner.</>}
            </div>
          )
        )}
      </main>

      {showLogin && (
        <LoginModal
          onClose={() => setShowLogin(false)}
          onSuccess={() => { setShowLogin(false); loadSession() }}
        />
      )}
    </div>
  )
}
