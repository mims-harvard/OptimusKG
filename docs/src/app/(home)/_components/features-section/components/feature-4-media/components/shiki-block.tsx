export function ShikiBlock({ children }: { children: React.ReactNode }) {
  return (
    <div className="l-shiki-block h-full w-full overflow-auto p-2">
      {children}
    </div>
  );
}
