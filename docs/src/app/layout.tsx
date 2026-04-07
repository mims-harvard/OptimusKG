import { RootProvider } from 'fumadocs-ui/provider/next';
import './global.css';
import { Inter } from 'next/font/google';

const inter = Inter({
  subsets: ['latin'],
});

export default function Layout({ children }: LayoutProps<'/'>) {
  return (
    <html lang="en" className={`${inter.className} h-full antialiased`} suppressHydrationWarning>
      <body className="flex flex-col min-h-screen bg-[#f7f7f4] text-[#26251e]">
        <RootProvider>{children}</RootProvider>
      </body>
    </html>
  );
}
