import Link from 'next/link';

export default function HomePage() {
  return (
    <div className="flex flex-col justify-center text-center flex-1 gap-4">
      <h1 className="text-4xl font-bold">OptimusKG</h1>
      <p className="text-lg text-fd-muted-foreground max-w-xl mx-auto">
        An opinionated, production-ready data pipeline for constructing
        biomedical knowledge graphs.
      </p>
      <div className="flex gap-4 justify-center mt-4">
        <Link
          href="/docs"
          className="px-4 py-2 rounded-lg bg-fd-primary text-fd-primary-foreground font-medium"
        >
          Get Started
        </Link>
        <Link
          href="https://github.com/mims-harvard/optimuskg"
          className="px-4 py-2 rounded-lg border font-medium"
        >
          GitHub
        </Link>
      </div>
      <p className="text-sm text-fd-muted-foreground mt-8">
        Zitnik Lab, Harvard Medical School
      </p>
    </div>
  );
}
