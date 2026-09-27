"use client";

import { FormEvent, useEffect, useState } from "react";
import { asEventDetails } from "@/lib/api";
import { RunDetail } from "@/lib/types";

const terminal = new Set(["completed", "failed", "cancelled", "canceled"]);
const timestamp = (value: string) => new Date(value).toLocaleString("en-US", { dateStyle: "medium", timeStyle: "medium", timeZone: "UTC" }) + " UTC";

export function RunDetailView({ id, initial }: { id: string; initial: RunDetail }) {
  const [detail, setDetail] = useState(initial);
  const [selectedCheckpoint, setSelectedCheckpoint] = useState<string>(initial.checkpoints.at(-1)?.sequence.toString() ?? "");
  const [resetting, setResetting] = useState(false);
  const [resetError, setResetError] = useState<string | null>(null);

  const refresh = async () => {
    try {
      const response = await fetch(`/engine/api/runs/${encodeURIComponent(id)}`, { cache: "no-store" });
      if (response.ok) {
        const next = await response.json() as RunDetail;
        setDetail(next);
        setSelectedCheckpoint((current) => current || next.checkpoints.at(-1)?.sequence.toString() || "");
      }
    } catch { /* Preserve the last durable state when the engine is unavailable. */ }
  };

  useEffect(() => {
    if (terminal.has(detail.run.status.toLowerCase())) return;
    const interval = window.setInterval(refresh, 3000);
    return () => window.clearInterval(interval);
  }, [detail.run.status, id]);

  async function resetRun(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const checkpointSequence = Number(selectedCheckpoint);
    if (!Number.isInteger(checkpointSequence)) {
      setResetError("Choose a checkpoint to reset this run.");
      return;
    }

    setResetting(true);
    setResetError(null);
    try {
      const response = await fetch(`/engine/api/runs/${encodeURIComponent(id)}/reset`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ checkpoint_sequence: checkpointSequence }),
        cache: "no-store",
      });
      if (!response.ok) {
        let message = `Could not reset this run (${response.status}).`;
        try {
          const body = await response.json() as { error?: string };
          if (body.error) message = body.error;
        } catch { /* Keep the useful HTTP fallback. */ }
        throw new Error(message);
      }
      const next = await response.json() as RunDetail;
      setDetail(next);
      setSelectedCheckpoint(checkpointSequence.toString());
    } catch (error) {
      setResetError(error instanceof Error ? error.message : "Could not reset this run.");
    } finally {
      setResetting(false);
    }
  }

  const { run, events, checkpoints } = detail;
  return (
    <>
      <section className="run-summary">
        <div><p className="eyebrow">{run.workflow_name}</p><h1>Run <code>{run.id}</code></h1></div>
        <span className={`status status-${run.status.toLowerCase()}`}>{run.status.replaceAll("_", " ")}</span>
      </section>
      <section className="detail-grid">
        <aside className="metadata-card"><h2>Run data</h2><dl><dt>Started</dt><dd>{timestamp(run.created_at)}</dd><dt>Last update</dt><dd>{timestamp(run.updated_at)}</dd></dl><h3>Input</h3><pre>{JSON.stringify(run.input ?? {}, null, 2)}</pre>
          <section className="checkpoint-panel" aria-labelledby="checkpoint-heading"><p className="eyebrow">Recovery point</p><h3 id="checkpoint-heading">Reset this run</h3><p>Resume from a saved checkpoint. Earlier events remain in the history.</p>
            {checkpoints.length === 0 ? <span className="checkpoint-empty">No checkpoints have been saved yet.</span> : <form onSubmit={resetRun}><label htmlFor="checkpoint">Checkpoint<select id="checkpoint" value={selectedCheckpoint} onChange={(event) => setSelectedCheckpoint(event.target.value)} disabled={resetting}>
              {checkpoints.map((checkpoint) => <option key={checkpoint.sequence} value={checkpoint.sequence}>#{checkpoint.sequence} · {checkpoint.step_name ?? "Before workflow"} · next step {checkpoint.next_step}</option>)}
            </select></label><button className="secondary-button" type="submit" disabled={resetting}>{resetting ? "Resetting…" : "Reset to checkpoint"}</button></form>}
            {resetError && <p className="form-error" role="alert">{resetError}</p>}
          </section>
        </aside>
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
