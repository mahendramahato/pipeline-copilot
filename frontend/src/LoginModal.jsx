import { useEffect, useRef, useState } from 'react'
import { login } from './api.js'
import { Lock } from './Icons.jsx'

// Owner sign-in. On success the server sets an HttpOnly session cookie.
export default function LoginModal({ onClose, onSuccess }) {
  const [password, setPassword] = useState('')
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(false)
  const inputRef = useRef(null)

  useEffect(() => { inputRef.current?.focus() }, [])

  async function submit(e) {
    e.preventDefault()
    setBusy(true)
    setError(null)
    try {
      await login(password)
      onSuccess()
    } catch (err) {
      setError(err.message)
      setBusy(false)
    }
  }

  return (
    <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <form className="modal" onSubmit={submit}>
        <div className="modal-icon"><Lock size={18} /></div>
        <h3>Owner sign-in</h3>
        <p className="muted">
          Live questions run real investigations against the pipeline and use paid model calls,
          so they're limited to the owner. Recorded investigations stay open to everyone.
        </p>
        <input
          ref={inputRef}
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Password"
          autoComplete="current-password"
          disabled={busy}
        />
        {error && <p className="error">{error}</p>}
        <div className="modal-actions">
          <button type="button" className="ghost" onClick={onClose}>Cancel</button>
          <button type="submit" className="primary" disabled={busy || !password}>Sign in</button>
        </div>
      </form>
    </div>
  )
}
