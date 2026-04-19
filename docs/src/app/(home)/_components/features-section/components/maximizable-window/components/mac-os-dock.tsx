"use client";

import type React from "react";
import { useCallback, useEffect, useRef, useState } from "react";

export type DockApp = {
  icon: string;
  id: string;
  name: string;
};

type MacOSDockProps = {
  apps: DockApp[];
  className?: string;
  onAppClick: (appId: string) => void;
  openApps?: string[];
};

type GsapLike = {
  to: (
    target: unknown,
    vars: {
      y: number;
      duration: number;
      ease: string;
      yoyo: boolean;
      repeat: number;
      transformOrigin: string;
    },
  ) => void;
};

const MacOSDock: React.FC<MacOSDockProps> = ({
  apps,
  onAppClick,
  openApps = [],
  className = "",
}) => {
  const [mouseX, setMouseX] = useState<number | null>(null);
  const [currentScales, setCurrentScales] = useState<number[]>(() => apps.map(() => 1));
  const [currentPositions, setCurrentPositions] = useState<number[]>([]);
  const dockRef = useRef<HTMLDivElement>(null);
  const iconRefs = useRef<(HTMLButtonElement | null)[]>([]);
  const animationFrameRef = useRef<number | undefined>(undefined);
  const lastMouseMoveTime = useRef<number>(0);

  const getResponsiveConfig = useCallback(() => {
    if (typeof window === "undefined") {
      return { baseIconSize: 44, maxScale: 1.5, effectWidth: 180 };
    }

    const smallerDimension = Math.min(window.innerWidth, window.innerHeight);

    if (smallerDimension < 480) {
      return {
        baseIconSize: Math.max(32, smallerDimension * 0.06),
        maxScale: 1.35,
        effectWidth: smallerDimension * 0.3,
      };
    }
    if (smallerDimension < 768) {
      return {
        baseIconSize: Math.max(36, smallerDimension * 0.055),
        maxScale: 1.4,
        effectWidth: smallerDimension * 0.28,
      };
    }
    if (smallerDimension < 1024) {
      return {
        baseIconSize: Math.max(40, smallerDimension * 0.045),
        maxScale: 1.5,
        effectWidth: smallerDimension * 0.24,
      };
    }
    return {
      baseIconSize: Math.max(44, Math.min(56, smallerDimension * 0.035)),
      maxScale: 1.6,
      effectWidth: 220,
    };
  }, []);

  const [config, setConfig] = useState({
    baseIconSize: 44,
    maxScale: 1.5,
    effectWidth: 180,
  });
  const { baseIconSize, maxScale, effectWidth } = config;
  const minScale = 1.0;
  const baseSpacing = Math.max(4, baseIconSize * 0.08);

  useEffect(() => {
    setConfig(getResponsiveConfig());
    const handleResize = () => {
      setConfig(getResponsiveConfig());
    };

    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, [getResponsiveConfig]);

  const calculateTargetMagnification = useCallback(
    (mousePosition: number | null) => {
      if (mousePosition === null) {
        return apps.map(() => minScale);
      }

      return apps.map((_, index) => {
        const normalIconCenter = index * (baseIconSize + baseSpacing) + baseIconSize / 2;
        const minX = mousePosition - effectWidth / 2;
        const maxX = mousePosition + effectWidth / 2;

        if (normalIconCenter < minX || normalIconCenter > maxX) {
          return minScale;
        }

        const theta = ((normalIconCenter - minX) / effectWidth) * 2 * Math.PI;
        const cappedTheta = Math.min(Math.max(theta, 0), 2 * Math.PI);
        const scaleFactor = (1 - Math.cos(cappedTheta)) / 2;

        return minScale + scaleFactor * (maxScale - minScale);
      });
    },
    [apps, baseIconSize, baseSpacing, effectWidth, maxScale],
  );

  const calculatePositions = useCallback(
    (scales: number[]) => {
      let currentX = 0;

      return scales.map((scale) => {
        const scaledWidth = baseIconSize * scale;
        const centerX = currentX + scaledWidth / 2;
        currentX += scaledWidth + baseSpacing;
        return centerX;
      });
    },
    [baseIconSize, baseSpacing],
  );

  useEffect(() => {
    const initialScales = apps.map(() => minScale);
    const initialPositions = calculatePositions(initialScales);
    setCurrentScales(initialScales);
    setCurrentPositions(initialPositions);
  }, [apps, calculatePositions]);

  const animateToTarget = useCallback(() => {
    const targetScales = calculateTargetMagnification(mouseX);
    const targetPositions = calculatePositions(targetScales);
    const lerpFactor = mouseX === null ? 0.12 : 0.2;

    setCurrentScales((prevScales) =>
      prevScales.map((currentScale, index) => {
        const diff = (targetScales[index] ?? minScale) - currentScale;
        return currentScale + diff * lerpFactor;
      }),
    );

    setCurrentPositions((prevPositions) =>
      prevPositions.map((currentPos, index) => {
        const diff = (targetPositions[index] ?? 0) - currentPos;
        return currentPos + diff * lerpFactor;
      }),
    );

    const scalesNeedUpdate = currentScales.some(
      (scale, index) => Math.abs(scale - (targetScales[index] ?? minScale)) > 0.002,
    );
    const positionsNeedUpdate = currentPositions.some(
      (pos, index) => Math.abs(pos - (targetPositions[index] ?? 0)) > 0.1,
    );

    if (scalesNeedUpdate || positionsNeedUpdate || mouseX !== null) {
      animationFrameRef.current = requestAnimationFrame(animateToTarget);
    }
  }, [mouseX, calculateTargetMagnification, calculatePositions, currentScales, currentPositions]);

  useEffect(() => {
    if (animationFrameRef.current) {
      cancelAnimationFrame(animationFrameRef.current);
    }
    animationFrameRef.current = requestAnimationFrame(animateToTarget);

    return () => {
      if (animationFrameRef.current) {
        cancelAnimationFrame(animationFrameRef.current);
      }
    };
  }, [animateToTarget]);

  const handleMouseMove = useCallback(
    (e: React.MouseEvent) => {
      const now = performance.now();

      if (now - lastMouseMoveTime.current < 16) {
        return;
      }

      lastMouseMoveTime.current = now;

      if (dockRef.current) {
        const rect = dockRef.current.getBoundingClientRect();
        const padding = Math.max(8, baseIconSize * 0.12);
        setMouseX(e.clientX - rect.left - padding);
      }
    },
    [baseIconSize],
  );

  const handleMouseLeave = useCallback(() => {
    setMouseX(null);
  }, []);

  const createBounceAnimation = useCallback(
    (element: HTMLElement) => {
      const bounceHeight = Math.max(-8, -baseIconSize * 0.15);
      element.style.transition = "transform 0.2s ease-out";
      element.style.transform = `translateY(${bounceHeight}px)`;

      setTimeout(() => {
        element.style.transform = "translateY(0px)";
      }, 200);
    },
    [baseIconSize],
  );

  const handleAppClick = (appId: string, index: number) => {
    const iconEl = iconRefs.current[index];
    if (iconEl) {
      const gsap =
        typeof window === "undefined" ? undefined : (window as unknown as { gsap?: GsapLike }).gsap;
      if (gsap) {
        const bounceHeight =
          (currentScales[index] ?? 1) > 1.3 ? -baseIconSize * 0.2 : -baseIconSize * 0.15;

        gsap.to(iconEl, {
          y: bounceHeight,
          duration: 0.2,
          ease: "power2.out",
          yoyo: true,
          repeat: 1,
          transformOrigin: "bottom center",
        });
      } else {
        createBounceAnimation(iconEl);
      }
    }

    onAppClick(appId);
  };

  const contentWidth =
    currentPositions.length > 0
      ? Math.max(
          ...currentPositions.map(
            (pos, index) => pos + (baseIconSize * (currentScales[index] ?? 1)) / 2,
          ),
        )
      : apps.length * (baseIconSize + baseSpacing) - baseSpacing;

  const padding = Math.max(8, baseIconSize * 0.12);

  return (
    // biome-ignore lint/a11y/noNoninteractiveElementInteractions: the dock is decorative macOS mimicry, mouse-move tracking drives magnification only, keyboard users reach apps via the tabbable icon buttons
    // biome-ignore lint/a11y/noStaticElementInteractions: same, hover magnification is a cosmetic enhancement on top of the focusable icon buttons
    <div
      className={`relative ${className}`}
      onMouseLeave={handleMouseLeave}
      onMouseMove={handleMouseMove}
      ref={dockRef}
      style={{
        width: `${contentWidth + padding * 2}px`,
        borderRadius: "18px",
        padding: `${padding}px`,
      }}
    >
      <svg aria-hidden="true" style={{ display: "none" }}>
        <title>Dock glass distortion</title>
        <filter
          filterUnits="objectBoundingBox"
          height="100%"
          id="optimus-dock-glass-distortion"
          width="100%"
          x="0%"
          y="0%"
        >
          <feTurbulence
            baseFrequency="0.001 0.005"
            numOctaves="1"
            result="turbulence"
            seed="17"
            type="fractalNoise"
          />
          <feComponentTransfer in="turbulence" result="mapped">
            <feFuncR amplitude="1" exponent="10" offset="0.5" type="gamma" />
            <feFuncG amplitude="0" exponent="1" offset="0" type="gamma" />
            <feFuncB amplitude="0" exponent="1" offset="0.5" type="gamma" />
          </feComponentTransfer>
          <feGaussianBlur in="turbulence" result="softMap" stdDeviation="3" />
          <feSpecularLighting
            in="softMap"
            lightingColor="white"
            result="specLight"
            specularConstant="1"
            specularExponent="100"
            surfaceScale="5"
          >
            <fePointLight x="-200" y="-200" z="300" />
          </feSpecularLighting>
          <feComposite
            in="specLight"
            k1="0"
            k2="1"
            k3="1"
            k4="0"
            operator="arithmetic"
            result="litImage"
          />
          <feDisplacementMap
            in="SourceGraphic"
            in2="softMap"
            scale="200"
            xChannelSelector="R"
            yChannelSelector="G"
          />
        </filter>
      </svg>
      <div
        aria-hidden="true"
        className="absolute inset-0"
        style={{
          borderRadius: "inherit",
          overflow: "hidden",
          backdropFilter: "blur(3px)",
          WebkitBackdropFilter: "blur(3px)",
          filter: "url(#optimus-dock-glass-distortion)",
          isolation: "isolate",
          zIndex: 0,
          pointerEvents: "none",
        }}
      />
      <div
        aria-hidden="true"
        className="absolute inset-0"
        style={{
          borderRadius: "inherit",
          background: "rgba(255, 255, 255, 0.25)",
          zIndex: 1,
          pointerEvents: "none",
        }}
      />
      <div
        aria-hidden="true"
        className="absolute inset-0"
        style={{
          borderRadius: "inherit",
          overflow: "hidden",
          boxShadow:
            "inset 0.5px 0.5px 0 0 rgba(255, 255, 255, 0.5), inset -0.5px -0.5px 0 0 rgba(255, 255, 255, 0.5)",
          zIndex: 2,
          pointerEvents: "none",
        }}
      />
      <div
        className="relative"
        style={{
          height: `${baseIconSize}px`,
          width: "100%",
          zIndex: 30,
        }}
      >
        {apps.map((app, index) => {
          const scale = currentScales[index] ?? 1;
          const position = currentPositions[index] ?? 0;
          const scaledSize = baseIconSize * scale;

          return (
            <button
              aria-label={`Open ${app.name}`}
              className="absolute flex cursor-pointer flex-col items-center justify-end border-0 bg-transparent p-0"
              key={app.id}
              onClick={() => handleAppClick(app.id, index)}
              ref={(el) => {
                iconRefs.current[index] = el;
              }}
              style={{
                left: `${position - scaledSize / 2}px`,
                bottom: "0px",
                width: `${scaledSize}px`,
                height: `${scaledSize}px`,
                transformOrigin: "bottom center",
                zIndex: Math.round(scale * 10),
              }}
              title={app.name}
              type="button"
            >
              {/* biome-ignore lint/performance/noImgElement: dock icons scale dynamically on hover, next/image requires static width/height */}
              <img
                alt={app.name}
                className="object-contain"
                height={scaledSize}
                src={app.icon}
                style={{
                  filter: `drop-shadow(0 ${scale > 1.2 ? Math.max(2, baseIconSize * 0.05) : Math.max(1, baseIconSize * 0.03)}px ${scale > 1.2 ? Math.max(4, baseIconSize * 0.1) : Math.max(2, baseIconSize * 0.06)}px rgba(0,0,0,${0.2 + (scale - 1) * 0.15}))`,
                  transform: "scale(0.88)",
                }}
                width={scaledSize}
              />

              {openApps.includes(app.id) && (
                <div
                  className="absolute"
                  style={{
                    bottom: `${Math.max(-2, -baseIconSize * 0.05)}px`,
                    left: "50%",
                    transform: "translateX(-50%)",
                    width: `${Math.max(3, baseIconSize * 0.06)}px`,
                    height: `${Math.max(3, baseIconSize * 0.06)}px`,
                    borderRadius: "50%",
                    backgroundColor: "rgba(255, 255, 255, 0.8)",
                    boxShadow: "0 0 4px rgba(0, 0, 0, 0.3)",
                  }}
                />
              )}
            </button>
          );
        })}
      </div>
    </div>
  );
};

export default MacOSDock;
