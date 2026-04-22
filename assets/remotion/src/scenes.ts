import type { ComponentType } from "react";

import { EndCard } from "./beats/EndCard";
import { GraphSchemaWindow } from "./beats/GraphSchemaWindow";
import { GraphViz } from "./beats/GraphViz";
import { HookText } from "./beats/HookText";
import { OntologyGrounded } from "./beats/OntologyGrounded";
import { PaperQA3AnalysisWindow } from "./beats/PaperQA3AnalysisWindow";
import { PythonClientWindow } from "./beats/PythonClientWindow";
import { UnifiedSchema } from "./beats/UnifiedSchema";

export type BeatLayout =
  | "default"
  | "push-up"
  | "hero-only"
  | "exit-up"
  | "enter-from-below"
  | "end-card";

export interface BeatRenderProps {
  heroText?: string | string[];
  layout?: BeatLayout;
  [key: string]: unknown;
}

export interface BeatSpec {
  id: string;
  label: string;
  durationInFrames: number;
  enterOverlap?: number;
  component: ComponentType<BeatRenderProps>;
  heroText?: string | string[];
  layout?: BeatLayout;
  props?: Record<string, unknown>;
}

export const BEATS: BeatSpec[] = [
  {
    id: "q-trust-a",
    label: "question-1",
    durationInFrames: 66,
    component: HookText,
    heroText: "How do you use a biomedical knowledge graph",
  },
  {
    id: "q-trust-b",
    label: "question-2",
    durationInFrames: 66,
    component: HookText,
    heroText: "you can actually trust?",
  },
  {
    id: "introducing",
    label: "showcase",
    durationInFrames: 48,
    component: HookText,
    heroText: "Introducing OptimusKG",
  },
  // {
  //   id: "wordmark",
  //   label: "wordmark",
  //   durationInFrames: 48,
  //   component: Wordmark,
  //   heroText: "OptimusKG",
  // },
  {
    id: "multimodal",
    label: "unified-schema",
    durationInFrames: 66,
    component: UnifiedSchema,
    heroText: "Multimodal, unified schema",
    layout: "push-up",
  },
  {
    id: "grounded",
    label: "ontology-grounded",
    durationInFrames: 90,
    component: OntologyGrounded,
    heroText: "Every entity is ontology grounded",
    props: {
      entities: [
        { position: -5, curie: "ECTO:0000907" },
        { position: -4, curie: "HP:0001250" },
        { position: -3, curie: "REACT:R-HSA-162582" },
        { position: -2, curie: "MONDO:0005148" },
        { position: -1, curie: "GO:0003674" },
        { position: 1, curie: "ENSG00000163513" },
        { position: 2, curie: "GO:0005634" },
        { position: 3, curie: "UBERON:0002107" },
        { position: 4, curie: "CHEBI:15365" },
        { position: 5, curie: "GO:0008152" },
      ],
    },
  },
  {
    id: "nodes",
    label: "nodes-count",
    durationInFrames: 54,
    component: GraphViz,
    heroText: "190,531 nodes",
    props: { phase: "nodes" },
  },
  {
    id: "edges",
    label: "edges-count",
    durationInFrames: 54,
    component: GraphViz,
    heroText: "21,813,816 edges",
    props: { phase: "edges" },
  },
  {
    id: "properties",
    label: "properties-count",
    durationInFrames: 54,
    component: GraphSchemaWindow,
    heroText: "67,249,863 properties",
  },
  {
    id: "validated",
    label: "validated",
    durationInFrames: 150,
    component: PaperQA3AnalysisWindow,
    heroText: "Rigorously validated.",
  },
  {
    id: "python",
    label: "python-client",
    durationInFrames: 138,
    component: PythonClientWindow,
    heroText: "Delightfully simple Python client",
  },
  {
    id: "read-more",
    label: "read-more",
    durationInFrames: 54,
    component: HookText,
    heroText: "Read more about our research below",
    layout: "exit-up",
  },
  {
    id: "research-url",
    label: "research-url",
    durationInFrames: 54,
    enterOverlap: 6,
    component: HookText,
    heroText: "optimuskg.ai/research →",
    layout: "enter-from-below",
  },
  {
    id: "end-card",
    label: "end-card",
    durationInFrames: 48,
    component: EndCard,
    layout: "end-card",
  },
];

export const DURATION_IN_FRAMES = BEATS.reduce(
  (acc, b) => acc + b.durationInFrames - (b.enterOverlap ?? 0),
  0,
);

export interface BeatPlacement extends BeatSpec {
  from: number;
}

export const BEAT_PLACEMENTS: BeatPlacement[] = (() => {
  const result: BeatPlacement[] = [];
  let cursor = 0;
  for (const beat of BEATS) {
    const from = cursor - (beat.enterOverlap ?? 0);
    result.push({ ...beat, from });
    cursor = from + beat.durationInFrames;
  }
  return result;
})();
