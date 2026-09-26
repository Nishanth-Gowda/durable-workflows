import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Runbook | Durable Workflows",
  description: "Inspect and operate durable workflow runs.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>
        <main className="shell">{children}</main>
      </body>
    </html>
  );
}
