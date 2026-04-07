const BLOG_POSTS = [
  {
    title: "Fast Apply",
    description:
      "A new model for applying code changes that is 10× faster than existing approaches.",
    author: "Sasha Rush",
    date: "Nov 2024",
    avatar:
      "https://www.figma.com/api/mcp/asset/2b14f2b8-2f53-45af-9828-67b02b5f2f6d",
  },
  {
    title: "Shadow Workspace",
    description:
      "How Cursor uses a background workspace to verify AI edits pass your tests before showing them.",
    author: "Michael & Sualeh",
    date: "Oct 2024",
    avatar:
      "https://www.figma.com/api/mcp/asset/5fe38b8b-cb53-4d25-b866-a354e0c97336",
  },
  {
    title: "Introducing Composer",
    description:
      "Composer lets Cursor make coordinated edits across multiple files with a single instruction.",
    author: "Cursor Team",
    date: "Sep 2024",
    avatar:
      "https://www.figma.com/api/mcp/asset/d743aa67-12f1-4f9e-a685-21065a069209",
  },
  {
    title: "BM25 + Embeddings",
    description:
      "How we index your entire codebase for fast, context-aware retrieval using hybrid search.",
    author: "Naman Jain",
    date: "Aug 2024",
    avatar:
      "https://www.figma.com/api/mcp/asset/d7a5ee39-9905-4441-821e-838e973f0f8c",
  },
];

export function RecentHighlights() {
  return (
    <section className="bg-[#f7f7f4] py-24">
      <div className="max-w-[1200px] mx-auto px-6">
        <h2 className="text-[1.5rem] font-normal text-[#26251e] tracking-[-0.02em] mb-10">
          Recent highlights
        </h2>

        <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-4">
          {BLOG_POSTS.map((post) => (
            <a
              key={post.title}
              href="#blog"
              className="group bg-[#f2f1ed] rounded-2xl p-6 flex flex-col gap-4 hover:bg-[#ece9e4] transition-colors"
            >
              <div className="flex-1">
                <h3 className="text-[1rem] font-normal text-[#26251e] tracking-[-0.02em] mb-2 group-hover:opacity-80 transition-opacity">
                  {post.title}
                </h3>
                <p className="text-[0.8125rem] text-[#26251e]/55 leading-[1.55]">
                  {post.description}
                </p>
              </div>

              <div className="flex items-center gap-2.5 pt-3 border-t border-[rgba(38,37,30,0.08)]">
                <img
                  src={post.avatar}
                  alt={post.author}
                  className="w-6 h-6 rounded-full object-cover shrink-0"
                />
                <div>
                  <p className="text-[0.75rem] text-[#26251e]/60">{post.author}</p>
                  <p className="text-[0.6875rem] text-[#26251e]/35">{post.date}</p>
                </div>
              </div>
            </a>
          ))}
        </div>
      </div>
    </section>
  );
}
