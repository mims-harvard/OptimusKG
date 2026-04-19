import "./landing.css";

export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <div
      className="flex flex-1 flex-col bg-[var(--l-bg)] text-[var(--l-ink)] antialiased"
      data-landing
    >
      {children}
    </div>
  );
}
