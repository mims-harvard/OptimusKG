import "./landing.css";

export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <div
      className="flex flex-1 flex-col bg-(--l-bg) text-(--l-ink) antialiased"
      data-landing
    >
      {children}
    </div>
  );
}
