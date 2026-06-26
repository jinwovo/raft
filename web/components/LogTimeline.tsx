import { ClusterSnapshot } from '../lib/protocol';

const ROLE_COLOR: Record<string, string> = { LEADER: '#10b981', CANDIDATE: '#f59e0b', FOLLOWER: '#5b6b85' };

/**
 * One row per node showing its replicated log as cells — committed entries are solid green, entries
 * present but not yet committed are faint. When the cluster has converged, every row is identical.
 */
export function LogTimeline({ snapshot }: { snapshot: ClusterSnapshot | null }) {
  if (!snapshot) return null;
  return (
    <div className="logpanel">
      <h3>REPLICATED LOG — solid = committed · faint = present, not yet committed · rows align when converged</h3>
      {snapshot.nodes.map((n) => (
        <div className="logrow" key={n.id}>
          <span className="who">
            <span className="node-id">{n.id}</span>
            <span
              className="role"
              style={{ color: n.up ? ROLE_COLOR[n.role] : '#ef4444', background: '#0b1120' }}
            >
              {n.up ? n.role[0] : '×'}
            </span>
          </span>
          <div className="cells">
            {n.snapshotIndex > 0 && (
              <div className="snap" title={`${n.snapshotIndex} committed entries compacted into a snapshot`}>
                ⛃ {n.snapshotIndex}
              </div>
            )}
            {n.log.length === 0 && n.snapshotIndex === 0 ? (
              <span className="empty-log">empty</span>
            ) : (
              n.log.map((cmd, i) => (
                <div
                  key={i}
                  className={`cell ${i < n.commitIndex - n.snapshotIndex ? 'committed' : 'present'}`}
                  title={`#${n.snapshotIndex + i + 1} · ${cmd}`}
                />
              ))
            )}
          </div>
        </div>
      ))}
    </div>
  );
}
