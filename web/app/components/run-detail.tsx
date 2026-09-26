"use client";

import { useEffect, useState } from "react";
import { asEventDetails } from "@/lib/api";
import { RunDetail } from "@/lib/types";

const terminal = new Set(["completed", "failed", "cancelled", "canceled"]);
const timestamp = (value: string) => new Date(value).toLocaleString("en-US", { dateStyle: "medium", timeStyle: "medium", timeZone: "UTC" }) + " UTC";

export function RunDetailView({ id, initial }: { id: string; initial: RunDetail }) {
  const [detail, setDetail] = useState(initial);

  useEffect(() => {
    if (terminal.has(detail.run.status.toLowerCase())) return;
    const refresh = async () => {
      try {
        const response = await fetch(`/engine/api/runs/${encodeURIComponent(id)}`, { cache: "no-store" });
        if (response.ok) setDetail(await response.json() as RunDetail);
      } catch { /* Preserve the last durable state when the engine is unavailable. */ }
    };
    const interval = window.setInterval(refresh, 3000);
    return () => window.clearInterval(interval);
  }, [detail.run.status, id]);

  const { run, events } = detail;
  return (
    <>
      <section className="run-summary">
        <div><p className="eyebrow">{run.workflow_name}</p><h1>Run <code>{run.id}</code></h1></div>
        <span className={`status status-${run.status.toLowerCase()}`}>{run.status.replaceAll("_", " ")}</span>
      </section>
      <section className="detail-grid">
        <aside className="metadata-card"><h2>Run data</h2><dl><dt>Started</dt><dd>{timestamp(run.created_at)}</dd><dt>Last update</dt><dd>{timestamp(run.updated_at)}</dd></dl><h3>Input</h3><pre>{JSON.stringify(run.input ?? {}, null, 2)}</pre></aside>
        <section className="timeline-card"><div className="section-heading"><div><p className="eyebrow">Append-only history</p><h2>Event timeline</h2></div><span className="refresh-note">{terminal.has(run.status.toLowerCase()) ? "Final state" : "Refreshing every 3s"}</span></div>
          {events.length === 0 ? <div className="empty"><p>Waiting for the first event.</p></div> : <ol className="timeline">{events.map((event) => {
            const details = asEventDetails(event.details);
            return <li key={event.sequence}><div className="event-dot" /><div className="event-content"><div><strong>{event.type.replaceAll("_", " ")}</strong>{event.step_name && <span className="step-name">{event.step_name}</span>}<time>{timestamp(event.occurred_at)}</time></div>{details && <pre>{details}</pre>}</div></li>;
          })}</ol>}
        </section>
      </section>
    </>
  );
}
