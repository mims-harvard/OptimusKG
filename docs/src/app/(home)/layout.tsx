import "./landing.css";

export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <div
      data-landing
      className="flex flex-1 flex-col bg-[var(--l-bg)] text-[var(--l-ink)] antialiased"
    >
      {children}
    </div>
  );
}
