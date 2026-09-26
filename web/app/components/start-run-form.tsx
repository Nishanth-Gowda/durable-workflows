"use client";

import { useActionState } from "react";
import { createRun, StartState } from "@/app/actions";
import { Workflow } from "@/lib/types";

const initialState: StartState = {};

export function StartRunForm({ workflows }: { workflows: Workflow[] }) {
  const [state, action, pending] = useActionState(createRun, initialState);

  return (
    <form action={action} className="start-form">
      <label>
        <span>Workflow</span>
        <select name="workflow_name" disabled={workflows.length === 0} defaultValue="">
          <option value="" disabled>Select a workflow</option>
          {workflows.map((workflow) => <option key={workflow.name} value={workflow.name}>{workflow.name}</option>)}
        </select>
      </label>
      <label>
        <span>Input <em>JSON object with text</em></span>
        <textarea name="input" rows={3} defaultValue={'{"text":"hello world"}'} spellCheck={false} required />
      </label>
      {state.error && <p className="form-error" role="alert">{state.error}</p>}
      <button className="primary-button" type="submit" disabled={pending || workflows.length === 0}>
        {pending ? "Starting…" : "Start run"}
      </button>
    </form>
  );
}
