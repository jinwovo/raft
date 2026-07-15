import { ClusterSnapshot, MessageEvent, NodeView } from '../lib/protocol';

const W = 1112;
const H = 470;

const ROLE_FILL: Record<string, string> = { LEADER: '#0e6b52', CANDIDATE: '#7a4d10', FOLLOWER: '#26324a' };
const ROLE_RING: Record<string, string> = { LEADER: '#10b981', CANDIDATE: '#f59e0b', FOLLOWER: '#5b6b85' };
const EVENT_COLOR: Record<string, string> = {
  'vote-req': '#f59e0b',
  'vote-rep': '#fbbf24',
  'prevote-req': '#a78bfa',
  'prevote-rep': '#c4b5fd',
  heartbeat: '#10b981',
  append: '#38bdf8',
  'append-rep': '#7dd3fc',
  snapshot: '#60a5fa',
  'snapshot-rep': '#93c5fd',
  'timeout-now': '#f472b6',
};

type Pos = { x: number; y: number };

function layout(nodes: NodeView[]): { pos: Record<string, Pos>; groups: number; hulls: { x: number; y: number; w: number; h: number }[] } {
  const sides = Array.from(new Set(nodes.map((n) => n.side))).sort((a, b) => a - b);
  const groups = sides.length;
  const pos: Record<string, Pos> = {};
  const hulls: { x: number; y: number; w: number; h: number }[] = [];
  sides.forEach((side, gi) => {
    const group = nodes.filter((n) => n.side === side);
    const cx = (W * (gi + 1)) / (groups + 1);
    const cy = H / 2 - 6;
    const r = Math.min(H * 0.32, (W / (groups + 1)) * 0.34);
    group.forEach((n, j) => {
      if (group.length === 1) {
        pos[n.id] = { x: cx, y: cy };
        return;
      }
      const a = -Math.PI / 2 + (2 * Math.PI * j) / group.length;
      pos[n.id] = { x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) };
    });
    if (groups > 1) {
      const xs = group.map((n) => pos[n.id].x);
      const ys = group.map((n) => pos[n.id].y);
      const pad = 58;
      const x = Math.min(...xs) - pad;
      const y = Math.min(...ys) - pad;
      hulls.push({ x, y, w: Math.max(...xs) + pad - x, h: Math.max(...ys) + pad + 14 - y });
    }
  });
  return { pos, groups, hulls };
}

/** One RPC in flight: a dot that rides a slightly bent path so req and rep take different lanes. */
function Packet({ e, pos }: { e: MessageEvent; pos: Record<string, Pos> }) {
  const a = pos[e.from];
  const b = pos[e.to];
  if (!a || !b) return null;
  const color = EVENT_COLOR[e.type] ?? '#64748b';
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  const len = Math.hypot(dx, dy) || 1;
  const off = Math.min(34, Math.max(10, len * 0.13));
  const cx = (a.x + b.x) / 2 - (dy / len) * off;
  const cy = (a.y + b.y) / 2 + (dx / len) * off;
  const d = `M ${a.x} ${a.y} Q ${cx} ${cy} ${b.x} ${b.y}`;
  const reply = e.type.endsWith('-rep');
  return (
    <g>
      <path d={d} fill="none" stroke={color} strokeOpacity={0.13} strokeWidth={1.3} />
      <g opacity={1}>
        {e.type === 'snapshot' ? (
          <rect x={-4.5} y={-3.5} width={9} height={7} rx={1.5} fill={color} />
        ) : e.type === 'timeout-now' ? (
          <path d="M 1.5 -5 L -3.5 1 L -0.5 1 L -1.5 5 L 3.5 -1 L 0.5 -1 Z" fill={color} />
        ) : reply ? (
          <circle r={3} fill="#0b0f17" stroke={color} strokeWidth={1.7} />
        ) : (
          <circle r={3.6} fill={color} />
        )}
        <animateMotion dur="0.5s" repeatCount="1" fill="freeze" path={d} />
        <animate attributeName="opacity" from="1" to="0" dur="0.5s" repeatCount="1" fill="freeze" />
      </g>
    </g>
  );
}

/**
 * A node, drawn at its own origin inside a translated <g> so a layout change (partition, join,
 * removal) glides instead of teleporting. The outer arc is the live election timer: it drains
 * toward an election and refills whenever a heartbeat lands; the leader runs none.
 */
function Node({ n, p, electionMax, onClick }: { n: NodeView; p: Pos; electionMax: number; onClick: () => void }) {
  if (!p) return null;
  const ring = ROLE_RING[n.role] ?? '#5b6b85';
  const fill = n.up ? ROLE_FILL[n.role] ?? '#26324a' : '#141a28';
  const ARC_R = 33.5;
  const C = 2 * Math.PI * ARC_R;
  const frac = n.electionIn >= 0 ? Math.min(1, n.electionIn / Math.max(1, electionMax)) : 0;
  const urgent = n.electionIn >= 0 && frac < 0.28;
  return (
    <g
      className="node"
      onClick={onClick}
      style={{ transform: `translate(${p.x}px, ${p.y}px)`, transition: 'transform 620ms cubic-bezier(.22,.9,.28,1)', cursor: 'pointer' }}
      opacity={n.up ? 1 : 0.55}
    >
      <title>
        {`${n.id} — ${n.up ? n.role : 'FROZEN'} · term ${n.term}\ncommit ${n.commitIndex} / last ${n.lastIndex}${n.snapshotIndex > 0 ? ` · ⛃${n.snapshotIndex} compacted` : ''}${n.electionIn >= 0 ? `\nelection timer fires in ${n.electionIn} ticks` : ''}\nclick to ${n.up ? 'freeze' : 'thaw'}`}
      </title>
      {n.up && n.role === 'LEADER' && (
        <circle r={28} fill="none" stroke="#10b981" strokeOpacity={0.5}>
          <animate attributeName="r" from="28" to="48" dur="1.6s" repeatCount="indefinite" />
          <animate attributeName="stroke-opacity" from="0.5" to="0" dur="1.6s" repeatCount="indefinite" />
        </circle>
      )}
      {n.up && n.role === 'CANDIDATE' && <circle r={31} fill="none" stroke="#f59e0b" strokeWidth={1.6} className="pulse-ring" />}

      {/* election-timer arc (§5.2): drains to 0 → the node campaigns; heartbeats refill it */}
      {n.up && n.electionIn >= 0 && (
        <>
          <circle r={ARC_R} fill="none" stroke="#223047" strokeOpacity={0.5} strokeWidth={2.4} />
          <circle
            r={ARC_R}
            fill="none"
            stroke={urgent ? '#f59e0b' : '#4d6285'}
            strokeWidth={2.4}
            strokeLinecap="round"
            strokeDasharray={`${C * frac} ${C}`}
            transform="rotate(-90)"
            style={{ transition: 'stroke-dasharray 140ms linear, stroke 300ms' }}
            opacity={0.95}
          />
        </>
      )}

      <circle r={26} fill={fill} stroke={n.up ? ring : '#ef4444'} strokeWidth={2.2} strokeDasharray={n.up ? undefined : '4 3'} strokeOpacity={n.up ? 1 : 0.8} />
      <text y={-1} textAnchor="middle" fontFamily="ui-monospace, monospace" fontWeight={700} fontSize={15} fill="#e5e7eb">
        {n.id}
      </text>
      <text y={13} textAnchor="middle" fontSize={10} fill="#cbd5e1" opacity={0.85}>
        T{n.term}
      </text>

      {/* votes collected while campaigning */}
      {n.up && n.role === 'CANDIDATE' && n.votes > 0 && (
        <g transform="translate(20,-33)">
          <rect x={-15} y={-9} width={30} height={17} rx={8.5} fill="#3b2a0a" stroke="#f59e0b" strokeOpacity={0.7} />
          <text textAnchor="middle" y={3.5} fontSize={10} fontWeight={700} fill="#fde68a">
            ✓{n.votes}
          </text>
        </g>
      )}

      <text y={47} textAnchor="middle" fontSize={10.5} fontWeight={600} fill={n.up ? ring : '#ef4444'}>
        {n.up ? n.role.toLowerCase() : '❄ frozen'}
      </text>
      {/* replication progress: committed fraction of this node's log */}
      <rect x={-22} y={52} width={44} height={3} rx={1.5} fill="#223047" />
      <rect x={-22} y={52} width={44 * (n.lastIndex > 0 ? n.commitIndex / n.lastIndex : 0)} height={3} rx={1.5} fill="#10b981" opacity={0.9} style={{ transition: 'width 200ms' }} />
      <text y={66} textAnchor="middle" fontSize={9.5} fill="#64748b" fontFamily="ui-monospace, monospace">
        {n.commitIndex}/{n.lastIndex}
      </text>
    </g>
  );
}

function Hud({ snapshot, connected }: { snapshot: ClusterSnapshot | null; connected: boolean }) {
  if (!snapshot) return <div className="hud">connecting…</div>;
  const leader = snapshot.nodes.find((n) => n.role === 'LEADER' && n.up);
  const term = snapshot.nodes.reduce((m, n) => Math.max(m, n.term), 0);
  const committed = snapshot.nodes.reduce((m, n) => Math.max(m, n.commitIndex), 0);
  const alive = snapshot.nodes.filter((n) => n.up).length;
  return (
    <div className="hud">
      <div className="hudgrid">
        <span className="k">tick</span>
        <span className="v">{snapshot.tick}</span>
        <span className="k">term</span>
        <span className="v">{term}</span>
        <span className="k">leader</span>
        <span className={`v ${leader ? 'lead' : 'nolead'}`}>{leader ? leader.id : '—'}</span>
        <span className="k">commit</span>
        <span className="v">#{committed}</span>
        <span className="k">up</span>
        <span className="v">
          {alive}/{snapshot.nodes.length}
        </span>
      </div>
      {(!connected || snapshot.preVote || snapshot.snapshotThreshold > 0 || snapshot.joint) && (
        <div className="hudflags">
          {!connected && <span className="flag lost">reconnecting…</span>}
          {snapshot.preVote && <span className="flag pv">pre-vote</span>}
          {snapshot.snapshotThreshold > 0 && <span className="flag cp">⛃ compact ≥{snapshot.snapshotThreshold}</span>}
          {snapshot.joint && <span className="flag joint">JOINT C_old,new</span>}
        </div>
      )}
    </div>
  );
}

function Legend() {
  return (
    <div className="legend">
      <div className="lrow">
        <span>
          <i className="swatch" style={{ background: '#10b981' }} />
          leader
        </span>
        <span>
          <i className="swatch" style={{ background: '#f59e0b' }} />
          candidate
        </span>
        <span>
          <i className="swatch" style={{ background: '#5b6b85' }} />
          follower
        </span>
        <span>
          <i className="swatch" style={{ background: '#ef4444' }} />
          frozen
        </span>
      </div>
      <div className="lrow dim">
        <span>
          <i className="pdot" style={{ background: '#10b981' }} />
          heartbeat
        </span>
        <span>
          <i className="pdot" style={{ background: '#38bdf8' }} />
          append
        </span>
        <span>
          <i className="pdot" style={{ background: '#f59e0b' }} />
          vote
        </span>
        <span>
          <i className="pdot" style={{ background: '#a78bfa' }} />
          pre-vote
        </span>
        <span>
          <i className="pdot hollow" />
          reply
        </span>
      </div>
    </div>
  );
}

export function ClusterCanvas({
  snapshot,
  connected,
  onNodeClick,
}: {
  snapshot: ClusterSnapshot | null;
  connected: boolean;
  onNodeClick: (n: NodeView) => void;
}) {
  const laid = snapshot ? layout(snapshot.nodes) : null;
  return (
    <div className="stage">
      <Hud snapshot={snapshot} connected={connected} />
      <Legend />
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: 'block' }}>
        <defs>
          {/* dots as rects, not circles — the demo capture scripts wait on `.stage svg circle`
              meaning a *node*, and a <circle> inside <defs> would match first yet never render */}
          <pattern id="grid" width={26} height={26} patternUnits="userSpaceOnUse">
            <rect x={0.2} y={0.2} width={2.2} height={2.2} rx={1.1} fill="rgba(148,163,184,0.08)" />
          </pattern>
        </defs>
        <rect width={W} height={H} fill="url(#grid)" />

        {laid && laid.groups > 1 && (
          <>
            {laid.hulls.map((h, i) => (
              <rect
                key={`hull-${i}`}
                x={h.x}
                y={h.y}
                width={h.w}
                height={h.h}
                rx={26}
                fill="rgba(239,68,68,0.045)"
                stroke="rgba(239,68,68,0.22)"
                strokeWidth={1.2}
                strokeDasharray="3 5"
              />
            ))}
            {Array.from({ length: laid.groups - 1 }, (_, i) => (
              <line
                key={`div-${i}`}
                x1={(W * (i + 1)) / laid.groups}
                y1={36}
                x2={(W * (i + 1)) / laid.groups}
                y2={H - 26}
                stroke="#ef4444"
                strokeOpacity={0.3}
                strokeWidth={1.5}
                strokeDasharray="6 8"
              />
            ))}
          </>
        )}

        {snapshot && laid && (
          <>
            <g>
              {snapshot.events.map((e, i) => (
                <Packet key={`${snapshot.tick}-${i}`} e={e} pos={laid.pos} />
              ))}
            </g>
            {snapshot.nodes.map((n) => (
              <Node key={n.id} n={n} p={laid.pos[n.id]} electionMax={snapshot.electionMax} onClick={() => onNodeClick(n)} />
            ))}
          </>
        )}
      </svg>
      {!snapshot && (
        <div className="stage-empty">
          <span className="pulse-ring">waiting for the cluster stream on :8104…</span>
        </div>
      )}
    </div>
  );
}
