import { Event, Run, RunDetail, Workflow } from "@/lib/types";

const engineURL = process.env.ENGINE_URL ?? "http://localhost:8080";

async function engineFetch<T>(path: string): Promise<T> {
  const response = await fetch(`${engineURL}${path}`, { cache: "no-store" });
  if (!response.ok) throw new Error(`Engine returned ${response.status}`);
  return response.json() as Promise<T>;
}

export async function getWorkflows(): Promise<Workflow[]> {
  const data = await engineFetch<{ workflows: Workflow[] }>("/api/workflows");
  return data.workflows;
}

export async function getRuns(): Promise<Run[]> {
  const data = await engineFetch<{ runs: Run[] }>("/api/runs");
  return data.runs;
}

export async function getRun(id: string): Promise<RunDetail> {
  return engineFetch<RunDetail>(`/api/runs/${encodeURIComponent(id)}`);
}

export async function startRun(workflowName: string, input?: Record<string, unknown>) {
  const response = await fetch(`${engineURL}/api/runs`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ workflow_name: workflowName, input }),
    cache: "no-store",
  });
  if (!response.ok) throw new Error(`Could not start workflow (${response.status})`);
  return response.json() as Promise<{ run_id: string }>;
}

export function asEventDetails(details: Event["details"]): string | null {
  if (details === undefined || details === null || details === "") return null;
  return typeof details === "string" ? details : JSON.stringify(details, null, 2);
}
