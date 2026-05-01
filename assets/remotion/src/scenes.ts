import type { ComponentType } from "react";

import { EndCard } from "./beats/EndCard";
import { HookText } from "./beats/HookText";
import { NodesEdges } from "./beats/NodesEdges";
import { OntologyGrounded } from "./beats/OntologyGrounded";
import { PaperQA3AnalysisWindow } from "./beats/PaperQA3AnalysisWindow";
import { PythonClientWindow } from "./beats/PythonClientWindow";
import { ResearchUrl } from "./beats/ResearchUrl";
import { SchemaScroll } from "./beats/SchemaScroll";
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
    durationInFrames: 132,
    component: HookText,
    heroText: "How do you use a biomedical knowledge graph",
  },
  {
    id: "q-trust-b",
    label: "question-2",
    durationInFrames: 132,
    component: HookText,
    heroText: "you can actually trust?",
  },
  {
    id: "introducing",
    label: "showcase",
    durationInFrames: 96,
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
    id: "usecase-graph-ml",
    label: "usecase-graph-ml",
    durationInFrames: 100,
    component: HookText,
    heroText: "A standardized resource for graph AI,",
  },
  {
    id: "usecase-llm-retrieval",
    label: "usecase-llm-retrieval",
    durationInFrames: 100,
    component: HookText,
    heroText: "biomedical discovery,",
  },
  {
    id: "usecase-discovery",
    label: "usecase-discovery",
    durationInFrames: 100,
    component: HookText,
    heroText: "and hypothesis generation.",
  },
  {
    id: "multimodal",
    label: "unified-schema",
    durationInFrames: 132,
    component: UnifiedSchema,
    heroText: "Multimodal, unified schema",
    layout: "push-up",
  },
  {
    id: "grounded",
    label: "ontology-grounded",
    durationInFrames: 180,
    component: OntologyGrounded,
    heroText: "Every entity is ontology grounded",
    props: {
      entities: [
        // Anatomy (UBERON)
        { position: -5, curie: "UBERON:0002107" }, // liver
        { position: -4, curie: "UBERON:0000955" }, // brain
        { position: -3, curie: "UBERON:0000948" }, // heart
        { position: -2, curie: "UBERON:0002048" }, // lung
        { position: -1, curie: "UBERON:0002113" }, // kidney
        { position: 1, curie: "UBERON:0000945" }, // stomach
        { position: 2, curie: "UBERON:0001264" }, // pancreas
        { position: 3, curie: "UBERON:0002385" }, // muscle
        // Biological Process (GO)
        { position: 4, curie: "GO:0008152" }, // metabolic process
        { position: 5, curie: "GO:0006915" }, // apoptotic process
        { position: 6, curie: "GO:0007049" }, // cell cycle
        { position: 7, curie: "GO:0006955" }, // immune response
        { position: 8, curie: "GO:0051301" }, // cell division
        { position: 9, curie: "GO:0006954" }, // inflammatory response
        { position: 10, curie: "GO:0023052" }, // signaling
        { position: 11, curie: "GO:0006351" }, // transcription
        // Cellular Component (GO)
        { position: 12, curie: "GO:0005634" }, // nucleus
        { position: 13, curie: "GO:0005739" }, // mitochondrion
        { position: 14, curie: "GO:0005886" }, // plasma membrane
        { position: 15, curie: "GO:0005783" }, // endoplasmic reticulum
        { position: 16, curie: "GO:0005840" }, // ribosome
        { position: 17, curie: "GO:0005737" }, // cytoplasm
        { position: 18, curie: "GO:0005794" }, // Golgi apparatus
        { position: 19, curie: "GO:0005764" }, // lysosome
        // Disease (MONDO)
        { position: 20, curie: "MONDO:0005148" }, // type 2 diabetes
        { position: 21, curie: "MONDO:0007254" }, // breast cancer
        { position: 22, curie: "MONDO:0005575" }, // colorectal cancer
        { position: 23, curie: "MONDO:0004979" }, // asthma
        { position: 24, curie: "MONDO:0004975" }, // Alzheimer
        { position: 25, curie: "MONDO:0005180" }, // Parkinson
        { position: 26, curie: "MONDO:0005027" }, // epilepsy
        { position: 27, curie: "MONDO:0007915" }, // lupus
        // Drug (CHEBI)
        { position: 28, curie: "CHEBI:15365" }, // aspirin
        { position: 29, curie: "CHEBI:27732" }, // caffeine
        { position: 30, curie: "CHEBI:45783" }, // imatinib
        { position: 31, curie: "CHEBI:6904" }, // methotrexate
        { position: 32, curie: "CHEBI:41774" }, // tamoxifen
        { position: 33, curie: "CHEBI:6801" }, // metformin
        { position: 34, curie: "CHEBI:5855" }, // ibuprofen
        { position: 35, curie: "CHEBI:46195" }, // paracetamol
        // Exposure (ECTO)
        { position: 36, curie: "ECTO:0000907" },
        { position: 37, curie: "ECTO:0009017" },
        { position: 38, curie: "ECTO:1000022" },
        { position: 39, curie: "ECTO:0000009" },
        { position: 40, curie: "ECTO:0000110" },
        { position: 41, curie: "ECTO:0000191" },
        { position: 42, curie: "ECTO:0001090" },
        { position: 43, curie: "ECTO:0005015" },
        // Gene (ENSG)
        { position: 44, curie: "ENSG00000163513" }, // TGFBR2
        { position: 45, curie: "ENSG00000141510" }, // TP53
        { position: 46, curie: "ENSG00000139618" }, // BRCA2
        { position: 47, curie: "ENSG00000146648" }, // EGFR
        { position: 48, curie: "ENSG00000136997" }, // MYC
        { position: 49, curie: "ENSG00000157764" }, // BRAF
        { position: 50, curie: "ENSG00000133703" }, // KRAS
        { position: 51, curie: "ENSG00000130203" }, // APOE
        // Molecular Function (GO)
        { position: 52, curie: "GO:0003674" }, // molecular function
        { position: 53, curie: "GO:0005515" }, // protein binding
        { position: 54, curie: "GO:0003677" }, // DNA binding
        { position: 55, curie: "GO:0005524" }, // ATP binding
        { position: 56, curie: "GO:0003723" }, // RNA binding
        { position: 57, curie: "GO:0016301" }, // kinase activity
        { position: 58, curie: "GO:0016791" }, // phosphatase activity
        { position: 59, curie: "GO:0003700" }, // transcription factor
        // Pathway (REACT)
        { position: 60, curie: "REACT:R-HSA-162582" }, // Signal Transduction
        { position: 61, curie: "REACT:R-HSA-1640170" }, // Cell Cycle
        { position: 62, curie: "REACT:R-HSA-168256" }, // Immune System
        { position: 63, curie: "REACT:R-HSA-1430728" }, // Metabolism
        { position: 64, curie: "REACT:R-HSA-74160" }, // Gene expression
        { position: 65, curie: "REACT:R-HSA-73894" }, // DNA Repair
        { position: 66, curie: "REACT:R-HSA-5357801" }, // Programmed Cell Death
        { position: 67, curie: "REACT:R-HSA-382551" }, // Transport of small molecules
        // Phenotype (HP)
        { position: 68, curie: "HP:0001250" }, // seizures
        { position: 69, curie: "HP:0001263" }, // global dev. delay
        { position: 70, curie: "HP:0001508" }, // failure to thrive
        { position: 71, curie: "HP:0000252" }, // microcephaly
        { position: 72, curie: "HP:0001252" }, // hypotonia
        { position: 73, curie: "HP:0001249" }, // intellectual disability
        { position: 74, curie: "HP:0000256" }, // macrocephaly
        { position: 75, curie: "HP:0002376" }, // developmental regression
      ],
    },
  },
  {
    id: "nodes-edges",
    label: "scale-stack",
    durationInFrames: 204,
    component: NodesEdges,
    props: {
      nodesText: "190,531 nodes",
      edgesText: "21,813,816 edges",
      propertiesText: "110,276,843 typed properties",
    },
  },
  {
    id: "schema-scroll",
    label: "schema-scroll",
    durationInFrames: 220,
    component: SchemaScroll,
    props: {
      nodesText: "190,531 nodes",
      edgesText: "21,813,816 edges",
      propertiesText: "110,276,843 typed properties",
    },
  },
  {
    id: "validated",
    label: "validated",
    durationInFrames: 380,
    component: PaperQA3AnalysisWindow,
    heroText: [
      "Independently validated using PaperQA3",
      "a multimodal agent that retrieves and reasons over millions of scientific articles",
    ],
  },
  {
    id: "python",
    label: "python-client",
    durationInFrames: 348,
    component: PythonClientWindow,
    heroText: "Delightfully simple Python client",
  },
  {
    id: "read-more",
    label: "read-more",
    durationInFrames: 108,
    component: HookText,
    heroText: "An open science, research initiative",
    layout: "exit-up",
  },
  {
    id: "research-url",
    label: "research-url",
    durationInFrames: 108,
    enterOverlap: 12,
    component: ResearchUrl,
  },
  {
    id: "end-card",
    label: "end-card",
    durationInFrames: 198,
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
