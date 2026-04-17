import { getPageImage, source } from '@/lib/source';
import { notFound } from 'next/navigation';
import { ImageResponse } from '@takumi-rs/image-response';

export const revalidate = false;

export async function GET(_req: Request, { params }: RouteContext<'/og/docs/[...slug]'>) {
  const { slug } = await params;
  const page = source.getPage(slug.slice(0, -1));
  if (!page) notFound();

  return new ImageResponse(
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'flex-end',
        width: '100%',
        height: '100%',
        backgroundColor: '#ffffff',
        padding: '4rem',
      }}
    >
      <p style={{ fontWeight: 800, fontSize: '72px', margin: '0 0 16px', color: '#000000', lineHeight: 1.1 }}>
        {page.data.title}
      </p>
      <p style={{ fontSize: '40px', color: 'rgba(0,0,0,0.5)', margin: 0, lineHeight: 1.3 }}>
        {page.data.description}
      </p>
    </div>,
    {
      width: 1200,
      height: 630,
      format: 'webp',
    },
  );
}

export function generateStaticParams() {
  return source.getPages().map((page) => ({
    lang: page.locale,
    slug: getPageImage(page).segments,
  }));
}
