import { StartRunForm } from "@/app/components/start-run-form";
import { RunList } from "@/app/components/run-list";
import { getRuns, getWorkflows } from "@/lib/api";

export const dynamic = "force-dynamic";

export default async function Home() {
  const [workflowResult, runResult] = await Promise.allSettled([getWorkflows(), getRuns()]);
  const workflows = workflowResult.status === "fulfilled" ? workflowResult.value : [];
  const runs = runResult.status === "fulfilled" ? runResult.value : [];
  const engineAvailable = workflowResult.status === "fulfilled" || runResult.status === "fulfilled";

  return <>
    <header className="topbar"><a href="/" className="brand"><span className="brand-mark">◆</span> Runbook</a><span className={`connection ${engineAvailable ? "online" : "offline"}`}><i />{engineAvailable ? "Engine connected" : "Engine unavailable"}</span></header>
    <section className="hero"><div><p className="eyebrow">Durable workflow engine</p><h1>See every decision<br />and every retry.</h1><p className="intro">Start a workflow, follow its event history, and see durable state as it progresses.</p></div><div className="workflow-count"><strong>{workflows.length}</strong><span>registered workflows</span></div></section>
    <section className="dashboard-grid"><aside className="start-card"><p className="eyebrow">New execution</p><h2>Start a run</h2>{workflows.length === 0 && <p className="engine-hint">The engine must be running at <code>localhost:8080</code> to load available workflows.</p>}<StartRunForm workflows={workflows} /></aside><RunList initialRuns={runs} /></section>
    {workflows.length > 0 && <section className="workflows"><div className="section-heading"><div><p className="eyebrow">Definitions</p><h2>Registered workflows</h2></div></div><div className="workflow-grid">{workflows.map((workflow) => <article key={workflow.name}><h3>{workflow.name}</h3><p>{workflow.description}</p><div className="step-list">{workflow.steps.map((step) => <span key={step.name}>{step.name}</span>)}</div></article>)}</div></section>}
  </>;
}
