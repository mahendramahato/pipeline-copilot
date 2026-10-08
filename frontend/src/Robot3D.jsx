// Clay-style 3D robot head with a headset for the empty-state title. Pure SVG:
// the depth comes from radial gradients, rim highlights and a contact shadow.
export default function Robot3D({ size = 120 }) {
  return (
    <svg className="robot3d" width={size} height={size} viewBox="0 0 120 120" aria-hidden="true">
      <defs>
        <radialGradient id="r3-shell" cx="35%" cy="25%" r="85%">
          <stop offset="0" stopColor="#e9a8ff" />
          <stop offset=".45" stopColor="#b026ff" />
          <stop offset="1" stopColor="#6b0fb3" />
        </radialGradient>
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

      <ellipse className="r3-shadow" cx="60" cy="112" rx="38" ry="5" fill="url(#r3-shadow)" />

      <g className="r3-body">
        {/* right ear cup, mostly behind the head */}
        <rect x="92" y="44" width="12" height="30" rx="6" fill="url(#r3-ear)" />

        {/* head shell */}
        <rect x="20" y="22" width="80" height="80" rx="26" fill="url(#r3-shell)" />
        <path d="M32 30 Q54 22 80 26" stroke="#fff" strokeWidth="4" strokeLinecap="round" fill="none" opacity=".45" />
        <rect x="20" y="22" width="80" height="80" rx="26" fill="none" stroke="#4c0880" strokeOpacity=".3" strokeWidth="1.5" />

        {/* screen */}
        <rect x="30" y="34" width="60" height="50" rx="15" fill="url(#r3-screen)" />
        <rect x="30" y="34" width="60" height="50" rx="15" fill="none" stroke="#000" strokeOpacity=".25" strokeWidth="1.5" />
        <path d="M38 40 Q50 37 62 38" stroke="#fff" strokeWidth="2" strokeLinecap="round" fill="none" opacity=".15" />

        {/* eyes */}
        <g className="r3-eyes">
          <rect x="38" y="50" width="16" height="16" rx="4" fill="#e6f700" opacity=".45" filter="url(#r3-glow)" />
          <rect x="64" y="48" width="16" height="16" rx="4" fill="#e6f700" opacity=".45" filter="url(#r3-glow)" />
          <rect x="39" y="51" width="14" height="14" rx="3.5" fill="url(#r3-eye)" />
          <rect x="65" y="49" width="14" height="14" rx="3.5" fill="url(#r3-eye)" />
          <rect x="41" y="53" width="4" height="3" rx="1.5" fill="#fff" opacity=".7" />
          <rect x="67" y="51" width="4" height="3" rx="1.5" fill="#fff" opacity=".7" />
        </g>

        {/* left ear cup: a short cylinder facing out */}
        <rect x="8" y="44" width="18" height="34" rx="7" fill="url(#r3-ear)" />
        <ellipse cx="10" cy="61" rx="6" ry="16" fill="url(#r3-ear-face)" />
        <ellipse cx="9" cy="61" rx="3" ry="10" fill="#d9d4e8" opacity=".35" />

        {/* headset mic boom */}
        <path d="M99 66 Q112 92 90 99" stroke="#6d28d9" strokeWidth="5" strokeLinecap="round" fill="none" opacity=".35" />
        <path d="M99 66 Q112 92 90 99" stroke="url(#r3-boom)" strokeWidth="3.5" strokeLinecap="round" fill="none" />
        <ellipse cx="83" cy="100" rx="10" ry="6.5" transform="rotate(-12 83 100)" fill="url(#r3-mic)" />
        <ellipse cx="80" cy="98" rx="3.5" ry="1.6" fill="#fff" opacity=".25" />
      </g>
    </svg>
  )
}
