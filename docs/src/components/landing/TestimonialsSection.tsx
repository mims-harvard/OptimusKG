const AVATARS = {
  diana:
    "https://www.figma.com/api/mcp/asset/c3618acd-fa17-445a-b728-eee59f10739d",
  jensen:
    "https://www.figma.com/api/mcp/asset/82c4dd96-0bb4-4036-b62c-27e451820cdc",
  andrej:
    "https://www.figma.com/api/mcp/asset/1d81342b-9d7a-42e1-82aa-a96029c3cd88",
  patrick:
    "https://www.figma.com/api/mcp/asset/efbbe09f-2b8c-4e4c-817d-27436322d24f",
  shadcn:
    "https://www.figma.com/api/mcp/asset/62596d70-9050-4cfc-a959-1caabe26e4d4",
  greg: "https://www.figma.com/api/mcp/asset/5673dc98-51d2-4cb0-ba9b-6fa38fe74de9",
};

const testimonials = [
  {
    quote:
      "Not sure how we ever shipped code without Cursor. It is the single biggest unlock for productivity I have seen in the last decade.",
    name: "Diana Hu",
    title: "YC Resident",
    avatar: AVATARS.diana,
  },
  {
    quote:
      "I use Cursor every day. The way it reasons about code and your intentions is just incredible. Cursor is clearly better.",
    name: "Jensen Huang",
    title: "CEO, NVIDIA",
    avatar: AVATARS.jensen,
  },
  {
    quote:
      "Been using Cursor for a while now, and it is truly next level good. The future of coding is writing code in plain English.",
    name: "Andrej Karpathy",
    title: "Former OpenAI",
    avatar: AVATARS.andrej,
  },
  {
    quote:
      "Cursor is one of the most impressive pieces of software I have seen in a long time. It makes you feel like you are coding in the future.",
    name: "Patrick Collison",
    title: "CEO, Stripe",
    avatar: AVATARS.patrick,
  },
  {
    quote:
      "I moved to Cursor a while back and my productivity has gone through the roof. I don't think I could go back to anything else.",
    name: "shadcn",
    title: "Creator of shadcn/ui",
    avatar: AVATARS.shadcn,
  },
  {
    quote:
      "This is going to be a very big deal. Cursor is genuinely an amazing tool and I think it represents the future of programming.",
    name: "Greg Brockman",
    title: "Co-founder, OpenAI",
    avatar: AVATARS.greg,
  },
];

export function TestimonialsSection() {
  return (
    <section className="bg-[#f2f1ed] py-24">
      <div className="max-w-[1200px] mx-auto px-6">
        <h2 className="text-[2.75rem] leading-[1.1] tracking-[-0.04em] font-normal text-[#26251e] text-center mb-16">
          The new way to build software.
        </h2>

        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-4">
          {testimonials.map((t) => (
            <div
              key={t.name}
              className="bg-[#f7f7f4] rounded-2xl p-7 flex flex-col gap-5"
            >
              <p className="text-[0.9375rem] text-[#26251e]/80 leading-[1.65] flex-1">
                &ldquo;{t.quote}&rdquo;
              </p>
              <div className="flex items-center gap-3 pt-2 border-t border-[rgba(38,37,30,0.08)]">
                <img
                  src={t.avatar}
                  alt={t.name}
                  className="w-8 h-8 rounded-full object-cover shrink-0"
                />
                <div>
                  <p className="text-[0.8125rem] font-medium text-[#26251e]">
                    {t.name}
                  </p>
                  <p className="text-[0.75rem] text-[#26251e]/50">{t.title}</p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
