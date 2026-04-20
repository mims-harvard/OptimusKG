import "./landing.css";

export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <div
      className="flex flex-1 flex-col bg-fd-background text-fd-foreground antialiased"
      data-landing
    >
      {children}
    </div>
  );
}
