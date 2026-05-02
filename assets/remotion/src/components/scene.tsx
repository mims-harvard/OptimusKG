import { createContext, useContext, type ReactNode } from "react";
import { AbsoluteFill, Sequence, useCurrentFrame } from "remotion";
import { holdAndExit } from "../animations";

const DEFAULT_EXIT_FRAMES = 18;

// Exit policy applied at the scene boundary.
export type SceneExit =
  | { type: "dissolve-white"; duration?: number }
  | { type: "none" }
  | {
      type: "custom";
      opacity: (params: { localFrame: number; sceneEnd: number }) => number;
    };

const DEFAULT_EXIT: SceneExit = { type: "dissolve-white" };

interface SceneContextValue {
  // Total duration of the enclosing scene, in frames.
  sceneEnd: number;
}

const SceneContext = createContext<SceneContextValue | null>(null);

export function useScene(): SceneContextValue {
  const ctx = useContext(SceneContext);
  if (!ctx) {
    throw new Error("useScene must be called inside <Scene>");
  }
  return ctx;
}

interface SceneProps {
  from: number;
  durationInFrames: number;
  name?: string;
  exit?: SceneExit;
  children: ReactNode;
}

export const Scene: React.FC<SceneProps> = ({
  from,
  durationInFrames,
  name,
  exit = DEFAULT_EXIT,
  children,
}) => (
  <Sequence durationInFrames={durationInFrames} from={from} name={name}>
    <SceneInner durationInFrames={durationInFrames} exit={exit}>
      {children}
    </SceneInner>
  </Sequence>
);

const SceneInner: React.FC<{
  durationInFrames: number;
  exit: SceneExit;
  children: ReactNode;
}> = ({ durationInFrames, exit, children }) => {
  const localFrame = useCurrentFrame();

  const opacity =
    exit.type === "none"
      ? 1
      : exit.type === "custom"
        ? exit.opacity({ localFrame, sceneEnd: durationInFrames })
        : holdAndExit({
            frame: localFrame,
            sceneEnd: durationInFrames,
            exitDuration: exit.duration ?? DEFAULT_EXIT_FRAMES,
          });

  const ctx: SceneContextValue = { sceneEnd: durationInFrames };

  // Dissolve-to-white: white backdrop on the outer fill, content fades to 0
  // on top of it. For other exits the white backdrop is a no-op since the
  // composition background already shows through past opacity 0.
  return (
    <SceneContext.Provider value={ctx}>
      <AbsoluteFill style={{ backgroundColor: "white" }}>
        <AbsoluteFill style={{ opacity }}>{children}</AbsoluteFill>
      </AbsoluteFill>
    </SceneContext.Provider>
  );
};
