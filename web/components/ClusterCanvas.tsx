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
};

type Pos = { x: number; y: number };

function layout(nodes: NodeView[]): { pos: Record<string, Pos>; groups: number } {
  const sides = Array.from(new Set(nodes.map((n) => n.side))).sort((a, b) => a - b);
  const groups = sides.length;
  const pos: Record<string, Pos> = {};
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
  });
  return { pos, groups };
}

function Packet({ e, pos }: { e: MessageEvent; pos: Record<string, Pos> }) {
  const a = pos[e.from];
  const b = pos[e.to];
  if (!a || !b) return null;
  const color = EVENT_COLOR[e.type] ?? '#64748b';
  return (
    <g>
      <line x1={a.x} y1={a.y} x2={b.x} y2={b.y} stroke={color} strokeOpacity={0.16} strokeWidth={1.4} />
      <circle r={3.4} fill={color}>
        <animate attributeName="cx" from={a.x} to={b.x} dur="0.5s" repeatCount="1" fill="freeze" />
        <animate attributeName="cy" from={a.y} to={b.y} dur="0.5s" repeatCount="1" fill="freeze" />
        <animate attributeName="opacity" from="1" to="0" dur="0.5s" repeatCount="1" fill="freeze" />
      </circle>
    </g>
  );
}

function Node({ n, p, onClick }: { n: NodeView; p: Pos; onClick: () => void }) {
  if (!p) return null;
  const ring = ROLE_RING[n.role] ?? '#5b6b85';
  const fill = n.up ? ROLE_FILL[n.role] ?? '#26324a' : '#1a2030';
  return (
    <g onClick={onClick} style={{ cursor: 'pointer' }} opacity={n.up ? 1 : 0.5}>
      {n.up && n.role === 'LEADER' && (
        <circle cx={p.x} cy={p.y} r={28} fill="none" stroke="#10b981" strokeOpacity={0.5}>
          <animate attributeName="r" from="28" to="48" dur="1.6s" repeatCount="indefinite" />
          <animate attributeName="stroke-opacity" from="0.5" to="0" dur="1.6s" repeatCount="indefinite" />
        </circle>
      )}
      {n.up && n.role === 'CANDIDATE' && (
        <circle cx={p.x} cy={p.y} r={31} fill="none" stroke="#f59e0b" strokeWidth={1.6} className="pulse-ring" />
      )}
      <circle
        cx={p.x}
        cy={p.y}
        r={26}
        fill={fill}
        stroke={ring}
        strokeWidth={2.2}
        strokeDasharray={n.up ? undefined : '4 3'}
      />
      <text x={p.x} y={p.y - 1} textAnchor="middle" fontFamily="ui-monospace, monospace" fontWeight={700} fontSize={15} fill="#e5e7eb">
        {n.id}
      </text>
      <text x={p.x} y={p.y + 13} textAnchor="middle" fontSize={10} fill="#cbd5e1" opacity={0.85}>
        T{n.term}
      </text>
      <text x={p.x} y={p.y + 45} textAnchor="middle" fontSize={10.5} fontWeight={600} fill={n.up ? ring : '#ef4444'}>
        {n.up ? n.role.toLowerCase() : 'frozen'}
      </text>
      <text x={p.x} y={p.y + 58} textAnchor="middle" fontSize={9.5} fill="#64748b">
        {n.commitIndex}/{n.lastIndex}
      </text>
    </g>
  );
}

function Readout({ snapshot }: { snapshot: ClusterSnapshot | null }) {
  if (!snapshot) return <div className="readout">connecting…</div>;
  const leader = snapshot.nodes.find((n) => n.role === 'LEADER' && n.up);
  const term = snapshot.nodes.reduce((m, n) => Math.max(m, n.term), 0);
  const committed = snapshot.nodes.reduce((m, n) => Math.max(m, n.commitIndex), 0);
  return (
    <div className="readout">
      tick <b>{snapshot.tick}</b>
      <br />
      term <b>{term}</b>
      <br />
      leader <span className="lead">{leader ? leader.id : '—'}</span>
      <br />
      committed <b>{committed}</b>
      <br />
      pre-vote <b style={{ color: snapshot.preVote ? '#a78bfa' : '#64748b' }}>{snapshot.preVote ? 'on' : 'off'}</b>
    </div>
  );
}

function Legend() {
  return (
    <div className="legend">
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
  );
}

export function ClusterCanvas({
  snapshot,
  onNodeClick,
}: {
  snapshot: ClusterSnapshot | null;
  onNodeClick: (n: NodeView) => void;
}) {
  const laid = snapshot ? layout(snapshot.nodes) : null;
  return (
    <div className="stage">
      <Readout snapshot={snapshot} />
      <Legend />
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: 'block' }}>
        {laid && laid.groups > 1 &&
          Array.from({ length: laid.groups - 1 }, (_, i) => (
            <line
              key={i}
              x1={(W * (i + 1)) / laid.groups}
              y1={40}
              x2={(W * (i + 1)) / laid.groups}
              y2={H - 30}
              stroke="#ef4444"
              strokeOpacity={0.35}
              strokeWidth={1.5}
              strokeDasharray="6 8"
            />
          ))}
        {snapshot && laid && (
          <>
            <g>
              {snapshot.events.map((e, i) => (
                <Packet key={`${snapshot.tick}-${i}`} e={e} pos={laid.pos} />
              ))}
            </g>
            {snapshot.nodes.map((n) => (
              <Node key={n.id} n={n} p={laid.pos[n.id]} onClick={() => onNodeClick(n)} />
            ))}
          </>
        )}
      </svg>
    </div>
  );
}
