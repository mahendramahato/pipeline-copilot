import { useEffect, useRef, useState } from 'react'

// Clay-style 3D robot head with a headset for the empty-state title.
// Each part is its own SVG layer pushed along Z inside a preserve-3d stage, so
// dragging (or the arrow keys) spins it and shows real depth. A stack of
// darker shell slices gives the head thickness, and a back face covers the
// front parts once it turns around.

const DEPTH = 34 // head depth in viewBox units
const SLICES = Array.from({ length: DEPTH }, (_, i) => -1 - i) // -1 .. -DEPTH
// Ear cups as discs on the sides of the head, stacked outward for thickness
const EAR_LEFT = [41, 44, 47, 50, 53]
const EAR_RIGHT = [41, 43, 45, 47]

const Defs = () => (
  <defs>
    <radialGradient id="r3-shell" cx="35%" cy="25%" r="85%">
      <stop offset="0" stopColor="#e9a8ff" />
      <stop offset=".45" stopColor="#b026ff" />
      <stop offset="1" stopColor="#6b0fb3" />
    </radialGradient>
    <radialGradient id="r3-back" cx="65%" cy="25%" r="85%">
      <stop offset="0" stopColor="#d27cff" />
      <stop offset=".5" stopColor="#9a1fe6" />
      <stop offset="1" stopColor="#5a0c99" />
    </radialGradient>
    <linearGradient id="r3-wall" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stopColor="#a43ef0" />
      <stop offset="1" stopColor="#5a0c99" />
    </linearGradient>
    <linearGradient id="r3-screen" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stopColor="#4a4560" />
      <stop offset="1" stopColor="#2a2638" />
    </linearGradient>
    <radialGradient id="r3-eye" cx="35%" cy="30%" r="80%">
      <stop offset="0" stopColor="#fbffb0" />
      <stop offset=".6" stopColor="#e6f700" />
      <stop offset="1" stopColor="#b8c400" />
    </radialGradient>
    <linearGradient id="r3-ear" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0" stopColor="#4b4366" />
      <stop offset="1" stopColor="#1f1a2e" />
    </linearGradient>
    <radialGradient id="r3-ear-face" cx="40%" cy="35%" r="70%">
      <stop offset="0" stopColor="#5a5275" />
      <stop offset="1" stopColor="#2a2340" />
    </radialGradient>
    <linearGradient id="r3-boom" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stopColor="#e9d5ff" />
      <stop offset="1" stopColor="#a78bfa" />
    </linearGradient>
    <radialGradient id="r3-mic" cx="40%" cy="30%" r="75%">
      <stop offset="0" stopColor="#6b6585" />
      <stop offset="1" stopColor="#25202f" />
    </radialGradient>
    <radialGradient id="r3-shadow" cx="50%" cy="50%" r="50%">
      <stop offset="0" stopColor="#2e1065" stopOpacity=".35" />
      <stop offset="1" stopColor="#2e1065" stopOpacity="0" />
    </radialGradient>
    <filter id="r3-glow" x="-50%" y="-50%" width="200%" height="200%">
      <feGaussianBlur stdDeviation="2" />
    </filter>
  </defs>
)

// One flat layer of the model, `z` in viewBox units
function Layer({ z, unit, back = false, solid = false, side = 0, children }) {
  // side: -1 / 1 turns the layer to face left / right, centered mid-depth
  const t = side
    ? `translateZ(${(-DEPTH / 2) * unit}px) rotateY(${side * 90}deg) translateZ(${z * unit}px)`
    : `${back ? 'rotateY(180deg) ' : ''}translateZ(${z * unit}px)`
  const style = { transform: t }
  return (
    <svg className={`r3-layer ${solid ? 'solid' : ''}`} viewBox="0 0 120 120" style={style}>
      {children}
    </svg>
  )
}

const clampX = (x) => Math.max(-35, Math.min(35, x))

export default function Robot3D({ size = 120 }) {
  const unit = size / 120
  const [rot, setRot] = useState({ x: -6, y: -18 })
  const [dragging, setDragging] = useState(false)
  const drag = useRef(null)
  const spin = useRef({ vx: 0, vy: 0, raf: 0 })

  // Coast after a flick, easing the tilt back toward level
  const coast = () => {
    cancelAnimationFrame(spin.current.raf)
    const step = () => {
      const s = spin.current
      s.vy *= 0.94
      s.vx *= 0.9
      setRot((r) => ({ x: clampX(r.x + s.vx + (-6 - r.x) * 0.04), y: r.y + s.vy }))
      if (Math.abs(s.vy) > 0.05 || Math.abs(s.vx) > 0.05) s.raf = requestAnimationFrame(step)
    }
    spin.current.raf = requestAnimationFrame(step)
  }
  useEffect(() => () => cancelAnimationFrame(spin.current.raf), [])

  const onDown = (e) => {
    cancelAnimationFrame(spin.current.raf)
    e.currentTarget.setPointerCapture(e.pointerId)
    drag.current = { px: e.clientX, py: e.clientY }
    spin.current.vx = spin.current.vy = 0
    setDragging(true)
  }
  const onMove = (e) => {
    if (!drag.current) return
    const dx = e.clientX - drag.current.px
    const dy = e.clientY - drag.current.py
    drag.current = { px: e.clientX, py: e.clientY }
    spin.current.vy = dx * 0.6
    spin.current.vx = -dy * 0.4
    setRot((r) => ({ x: clampX(r.x - dy * 0.4), y: r.y + dx * 0.6 }))
  }
  const onUp = () => {
    if (!drag.current) return
    drag.current = null
    setDragging(false)
    coast()
  }
  const onKey = (e) => {
    const turn = { ArrowLeft: [0, -15], ArrowRight: [0, 15], ArrowUp: [8, 0], ArrowDown: [-8, 0] }[e.key]
    if (!turn) return
    e.preventDefault()
    setRot((r) => ({ x: clampX(r.x + turn[0]), y: r.y + turn[1] }))
  }

  return (
    <div
      className={`robot3d ${dragging ? 'dragging' : ''}`}
      style={{ width: size, height: size }}
      role="img"
      aria-label="Pipeline Copilot robot. Drag or use the arrow keys to rotate it."
      tabIndex={0}
      onPointerDown={onDown}
      onPointerMove={onMove}
      onPointerUp={onUp}
      onPointerCancel={onUp}
      onKeyDown={onKey}
    >
      <svg className="r3-shadow" viewBox="0 0 120 120">
        <Defs />
        <ellipse cx="60" cy="112" rx="38" ry="5" fill="url(#r3-shadow)" />
      </svg>

      <div className="r3-float">
        <div className="r3-stage" style={{
            transform: `rotateX(${rot.x}deg) rotateY(${rot.y}deg)`,
            transformOrigin: `50% 50% ${(-DEPTH / 2) * unit}px`,
          }}>
          {/* side walls hide the slice edges when seen side-on */}
          {[-1, 1].map((side) => (
            <Layer key={`w${side}`} z={40} unit={unit} side={side} solid>
              <rect x={60 - DEPTH / 2} y="30" width={DEPTH} height="64" rx="14" fill="url(#r3-wall)" />
            </Layer>
          ))}

          {/* ear cups on the sides of the head */}
          {EAR_LEFT.map((z, i) => (
            <Layer key={`l${z}`} z={z} unit={unit} side={-1} solid>
              <circle cx="60" cy="61" r="17" fill={i === EAR_LEFT.length - 1 ? 'url(#r3-ear-face)' : '#2a2340'} />
              {i === EAR_LEFT.length - 1 && <circle cx="58" cy="58" r="9" fill="#d9d4e8" opacity=".3" />}
            </Layer>
          ))}
          {EAR_RIGHT.map((z, i) => (
            <Layer key={`r${z}`} z={z} unit={unit} side={1} solid>
              <circle cx="60" cy="59" r="15" fill={i === EAR_RIGHT.length - 1 ? 'url(#r3-ear-face)' : '#2a2340'} />
            </Layer>
          ))}

          {/* head thickness */}
          {SLICES.map((z) => (
            <Layer key={z} z={z} unit={unit} solid>
              <rect x="20" y="22" width="80" height="80" rx="26" fill="#7a14c4" />
            </Layer>
          ))}

          {/* back of the head */}
          <Layer z={DEPTH + 1} unit={unit} back>
            <rect x="20" y="22" width="80" height="80" rx="26" fill="url(#r3-back)" />
            <path d="M40 30 Q66 22 88 30" stroke="#fff" strokeWidth="4" strokeLinecap="round" fill="none" opacity=".35" />
            <rect x="44" y="72" width="32" height="5" rx="2.5" fill="#5a0c99" />
            <rect x="44" y="81" width="32" height="5" rx="2.5" fill="#5a0c99" />
          </Layer>

          {/* face shell, with the ear cups peeking out behind it */}
          <Layer z={-DEPTH / 2} unit={unit} solid>
            <rect x="92" y="44" width="12" height="30" rx="6" fill="url(#r3-ear)" />
            <rect x="8" y="44" width="18" height="34" rx="7" fill="url(#r3-ear)" />
          </Layer>
          <Layer z={0} unit={unit}>
            <rect x="20" y="22" width="80" height="80" rx="26" fill="url(#r3-shell)" />
            <path d="M32 30 Q54 22 80 26" stroke="#fff" strokeWidth="4" strokeLinecap="round" fill="none" opacity=".45" />
          </Layer>

          {/* screen, set slightly proud of the shell */}
          <Layer z={2} unit={unit}>
            <rect x="30" y="34" width="60" height="50" rx="15" fill="url(#r3-screen)" />
            <rect x="30" y="34" width="60" height="50" rx="15" fill="none" stroke="#000" strokeOpacity=".25" strokeWidth="1.5" />
            <path d="M38 40 Q50 37 62 38" stroke="#fff" strokeWidth="2" strokeLinecap="round" fill="none" opacity=".15" />
          </Layer>

          {/* eyes float just above the screen */}
          <Layer z={4} unit={unit}>
            <g className="r3-eyes">
              <rect x="38" y="50" width="16" height="16" rx="4" fill="#e6f700" opacity=".45" filter="url(#r3-glow)" />
              <rect x="64" y="48" width="16" height="16" rx="4" fill="#e6f700" opacity=".45" filter="url(#r3-glow)" />
              <rect x="39" y="51" width="14" height="14" rx="3.5" fill="url(#r3-eye)" />
              <rect x="65" y="49" width="14" height="14" rx="3.5" fill="url(#r3-eye)" />
              <rect x="41" y="53" width="4" height="3" rx="1.5" fill="#fff" opacity=".7" />
              <rect x="67" y="51" width="4" height="3" rx="1.5" fill="#fff" opacity=".7" />
            </g>
          </Layer>

          {/* mic boom swings out in front of the chin */}
          <Layer z={9} unit={unit} solid>
            <path d="M99 66 Q112 92 90 99" stroke="#6d28d9" strokeWidth="5" strokeLinecap="round" fill="none" opacity=".35" />
            <path d="M99 66 Q112 92 90 99" stroke="url(#r3-boom)" strokeWidth="3.5" strokeLinecap="round" fill="none" />
            <ellipse cx="83" cy="100" rx="10" ry="6.5" transform="rotate(-12 83 100)" fill="url(#r3-mic)" />
            <ellipse cx="80" cy="98" rx="3.5" ry="1.6" fill="#fff" opacity=".25" />
          </Layer>
        </div>
      </div>
    </div>
  )
}
