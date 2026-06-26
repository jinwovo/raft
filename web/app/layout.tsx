import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'raft — consensus, live',
  description: 'Watch a from-scratch Raft cluster elect a leader, replicate a log, and survive partitions.',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
