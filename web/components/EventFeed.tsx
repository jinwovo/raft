import { FeedItem, FeedKind } from '../lib/useEventFeed';

const KIND_COLOR: Record<FeedKind, string> = {
  election: '#f59e0b',
  leader: '#10b981',
  commit: '#38bdf8',
  fault: '#ef4444',
  heal: '#34d399',
  config: '#a78bfa',
  snapshot: '#60a5fa',
  membership: '#e879f9',
  info: '#64748b',
};

/**
 * The cluster's story, told from the same frames the canvas draws — elections, commits, faults,
 * config epochs — newest first. Derived entirely client-side by diffing consecutive snapshots.
 */
export function EventFeed({ items }: { items: FeedItem[] }) {
  return (
    <div className="feedpanel">
      <h3>
        EVENTS <span className="feedsub">derived live from the stream</span>
      </h3>
      <div className="feed">
        {items.length === 0 && <div className="feed-empty">quiet so far — try a button above, or freeze a node</div>}
        {items.map((it) => (
          <div className="fitem" key={it.key}>
            <i className="fdot" style={{ background: KIND_COLOR[it.kind] }} />
            <span className="ftick">t{it.tick}</span>
            <span className="ftext">{it.text}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
