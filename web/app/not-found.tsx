import Link from "next/link";

export default function NotFound() {
  return <section className="not-found"><p className="eyebrow">Run unavailable</p><h1>We could not find that run.</h1><p>It may have been removed, or the workflow engine is not running.</p><Link className="primary-button" href="/">Back to dashboard</Link></section>;
}
