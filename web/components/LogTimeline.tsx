import { ClusterSnapshot } from '../lib/protocol';

const ROLE_COLOR: Record<string, string> = { LEADER: '#10b981', CANDIDATE: '#f59e0b', FOLLOWER: '#5b6b85' };

/**
 * Deterministic identity hue per command, so the same entry lines up as the same color on every
 * row. The multiplicative scramble matters: sequential commands ("cmd-7", "cmd-8") hash one apart,
 * and without it they'd land one degree apart on the wheel and look identical.
 */
function hueOf(cmd: string): number {
  let h = 0;
  for (let i = 0; i < cmd.length; i++) h = (h * 31 + cmd.charCodeAt(i)) | 0;
  return (Math.imul(h, 2654435761) >>> 0) % 360;
}

/**
 * One row per node showing its replicated log as color-coded cells. The hue is the entry's
 * identity (same command ⇒ same color on every row), the fill treatment is its state: solid =
 * committed, faint+dashed = present but not yet committed — so divergence during a partition and
 * the log-repair after healing are directly visible. Rows align when the cluster has converged.
 */
export function LogTimeline({ snapshot }: { snapshot: ClusterSnapshot | null }) {
  if (!snapshot) return null;
  return (
    <div className="logpanel">
      <h3>
        REPLICATED LOG
        <span className="loglegend">
          <i className="cell committed demo" style={{ background: 'hsl(160 55% 45%)', borderColor: 'hsl(160 55% 52%)' }} /> committed
          <i className="cell present demo" style={{ background: 'hsl(160 40% 26%)', borderColor: 'hsl(160 35% 38%)' }} /> uncommitted
          <span className="snap demo">⛃ n</span> compacted · color = entry identity · rows align when converged
        </span>
      </h3>
      {snapshot.nodes.map((n) => (
        <div className="logrow" key={n.id}>
          <span className="who">
            <span className="node-id">{n.id}</span>
            <span className="role" style={{ color: n.up ? ROLE_COLOR[n.role] : '#ef4444' }}>
              {n.up ? n.role[0] : '×'}
            </span>
            <span className="counts">
              {n.commitIndex}/{n.lastIndex}
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
              n.log.map((cmd, i) => {
                const localCommit = n.commitIndex - n.snapshotIndex;
                const committed = i < localCommit;
                const hue = hueOf(cmd);
                return (
                  <span key={i} style={{ display: 'contents' }}>
                    <div
                      className={`cell ${committed ? 'committed' : 'present'}`}
                      style={
                        committed
                          ? { background: `hsl(${hue} 55% 45%)`, borderColor: `hsl(${hue} 55% 52%)` }
                          : { background: `hsl(${hue} 40% 26%)`, borderColor: `hsl(${hue} 35% 38%)` }
                      }
                      title={`#${n.snapshotIndex + i + 1} · ${cmd} · ${committed ? 'committed' : 'not yet committed'}`}
                    />
                    {i === localCommit - 1 && i < n.log.length - 1 && (
                      <i className="frontier" title={`commit frontier — #${n.commitIndex}`} />
                    )}
                  </span>
                );
              })
            )}
          </div>
        </div>
      ))}
    </div>
  );
}
