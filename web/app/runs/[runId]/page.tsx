import Link from "next/link";
import { notFound } from "next/navigation";
import { RunDetailView } from "@/app/components/run-detail";
import { getRun } from "@/lib/api";

export const dynamic = "force-dynamic";

export default async function RunPage({ params }: { params: Promise<{ runId: string }> }) {
  const { runId } = await params;
  try {
    const run = await getRun(runId);
    return <><header className="topbar"><Link href="/" className="back-link">← All runs</Link><span className="connection online"><i />Live detail</span></header><RunDetailView id={runId} initial={run} /></>;
  } catch {
    notFound();
  }
}
