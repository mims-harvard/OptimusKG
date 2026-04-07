import { Navbar } from "@/components/landing/Navbar";
import { HeroSection } from "@/components/landing/HeroSection";
import { LogoGarden } from "@/components/landing/LogoGarden";
import { FeaturesSection } from "@/components/landing/FeaturesSection";
import { FrontierSection } from "@/components/landing/FrontierSection";
import { Footer } from "@/components/landing/Footer";

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
