import { FeaturesSection } from "./_components/features-section";
import { Footer } from "./_components/footer";
import { FrontierSection } from "./_components/frontier-section";
import { HeroSection } from "./_components/hero-section";
import { LogoGarden } from "./_components/logo-garden";
import { Navbar } from "./_components/navbar";
import { StatsScroller } from "./_components/stats-scroller";

export default function HomePage() {
  return (
    <>
      <Navbar />
      <main>
        <HeroSection />
        <StatsScroller />
        {/* <LogoGarden /> */}
        <FeaturesSection />
        {/* <FrontierSection /> */}
      </main>
      <Footer />
    </>
  );
}
