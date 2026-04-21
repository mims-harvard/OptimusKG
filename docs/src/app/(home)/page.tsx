import { FeaturesSection } from "./_components/features-section";
import { Footer } from "./_components/footer";
import { HeroSection } from "./_components/hero-section";
import { Navbar } from "./_components/navbar";
import { StatsScroller } from "./_components/stats-scroller";

export default function HomePage() {
  return (
    <>
      <Navbar />
      <main>
        <HeroSection />
        <StatsScroller />
        <FeaturesSection />
      </main>
      <Footer />
    </>
  );
}
