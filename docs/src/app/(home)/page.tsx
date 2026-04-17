import { FeaturesSection } from "./_components/FeaturesSection";
import { Footer } from "./_components/Footer";
import { FrontierSection } from "./_components/FrontierSection";
import { HeroSection } from "./_components/HeroSection";
import { LogoGarden } from "./_components/LogoGarden";
import { Navbar } from "./_components/Navbar";

export default function HomePage() {
  return (
    <>
      <Navbar />
      <main>
        <HeroSection />
        <LogoGarden />
        {/* <FeaturesSection /> */}
        {/* <FrontierSection /> */}
      </main>
      <Footer />
    </>
  );
}
