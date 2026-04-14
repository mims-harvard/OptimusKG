import { Navbar } from "./_components/Navbar";
import { HeroSection } from "./_components/HeroSection";
import { LogoGarden } from "./_components/LogoGarden";
import { FeaturesSection } from "./_components/FeaturesSection";
import { FrontierSection } from "./_components/FrontierSection";
import { Footer } from "./_components/Footer";

export default function HomePage() {
  return (
    <>
      <Navbar />
      <main>
        <HeroSection />
        <LogoGarden />
        <FeaturesSection />
        <FrontierSection />
      </main>
      <Footer />
    </>
  );
}
