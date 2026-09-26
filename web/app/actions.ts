"use server";

import { redirect } from "next/navigation";
import { startRun } from "@/lib/api";

export type StartState = { error?: string };

export async function createRun(_previous: StartState, formData: FormData): Promise<StartState> {
  const workflowName = String(formData.get("workflow_name") ?? "").trim();
  const inputText = String(formData.get("input") ?? "").trim();

  if (!workflowName) return { error: "Choose a workflow before starting a run." };

  let input: Record<string, unknown> | undefined;
  if (inputText) {
    try {
      const parsed: unknown = JSON.parse(inputText);
      if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") {
        return { error: "Input must be a JSON object." };
      }
      input = parsed as Record<string, unknown>;
    } catch {
      return { error: "Input must be valid JSON." };
    }
  }

  let runId: string;
  try {
    const result = await startRun(workflowName, input);
    runId = result.run_id;
  } catch (error) {
    return { error: error instanceof Error ? error.message : "Could not start the run." };
  }
  redirect(`/runs/${encodeURIComponent(runId)}`);
}
