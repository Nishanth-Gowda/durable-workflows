"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { Run } from "@/lib/types";

function relativeTime(value: string, now: number | null) {
  if (now === null) return new Date(value).toISOString().slice(0, 10);
  const seconds = Math.max(0, Math.round((now - new Date(value).getTime()) / 1000));
  if (seconds < 10) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return new Date(value).toISOString().slice(0, 10);
}

function Status({ status }: { status: string }) {
  return <span className={`status status-${status.toLowerCase()}`}>{status.replaceAll("_", " ")}</span>;
}

export function RunList({ initialRuns }: { initialRuns: Run[] }) {
  const [runs, setRuns] = useState(initialRuns);
  const [updated, setUpdated] = useState<number | null>(null);

  useEffect(() => {
    setUpdated(Date.now());
    const refresh = async () => {
      try {
        const response = await fetch("/engine/api/runs", { cache: "no-store" });
        if (!response.ok) return;
        const body = await response.json() as { runs: Run[] };
        setRuns(body.runs);
        setUpdated(Date.now());
      } catch { /* Engine may be restarting; retain the last known data. */ }
    };
    const interval = window.setInterval(refresh, 5000);
    return () => window.clearInterval(interval);
  }, []);

  return (
    <section className="runs-panel" aria-live="polite">
      <div className="section-heading">
        <div><p className="eyebrow">Live queue</p><h2>Workflow runs</h2></div>
        <span className="refresh-note">Updates every 5s · checked {updated === null ? "pending" : relativeTime(new Date(updated).toISOString(), updated)}</span>
      </div>
      {runs.length === 0 ? <div className="empty"><p>No runs yet.</p><span>Start a workflow to see its durable history here.</span></div> : (
        <div className="run-table" role="table">
          <div className="table-row table-header" role="row"><span>Workflow</span><span>Status</span><span>Updated</span><span /></div>
          {runs.map((run) => (
            <Link className="table-row" role="row" href={`/runs/${encodeURIComponent(run.id)}`} key={run.id}>
              <span><strong>{run.workflow_name}</strong><small>{run.id}</small></span>
              <Status status={run.status} />
              <span className="updated-at">{relativeTime(run.updated_at, updated)}</span><span className="arrow">→</span>
            </Link>
          ))}
        </div>
      )}
    </section>
  );
}
