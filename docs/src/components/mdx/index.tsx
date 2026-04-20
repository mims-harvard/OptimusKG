import defaultMdxComponents from "fumadocs-ui/mdx";
import * as Python from "fumadocs-python/components";
import type { MDXComponents } from "mdx/types";

import { DisGenEdge, PheGenEdge } from "../disease-assoc-edge-schemas";
// Node schemas
import { GeneSchema } from "../gene-schema";
// Edge schemas
import {
  AnaAnaEdge,
  BpoBpoEdge,
  CcoCcoEdge,
  DisDieEdge,
  GenGenEdge,
  MfnMfnEdge,
  PhePheEdge,
  PwyGenEdge,
  PwyPwyEdge,
} from "./components/base-edge-schemas";
// Base schemas
import { BaseEdgeSchema, BaseNodeSchema } from "./components/base-schemas";
import { DiseaseSchema } from "./components/disease-schema";
import {
  DrgDisEdge,
  DrgDrgEdge,
  DrgGenEdge,
  DrgPheEdge,
} from "./components/drug-edge-schemas";
import { DrugSchema } from "./components/drug-schema";
import { ExposureSchema } from "./components/exposure-schema";
import {
  BpoGenEdge,
  CcoGenEdge,
  MfnGenEdge,
} from "./components/go-annotation-edge-schemas";
import {
  AnatomySchema,
  BiologicalProcessSchema,
  CellularComponentSchema,
  MolecularFunctionSchema,
} from "./components/ontology-node-schemas";
import { PathwaySchema } from "./components/pathway-schema";
import { PhenotypeSchema } from "./components/phenotype-schema";
import {
  AnaGenEdge,
  DisPheEdge,
  ExpBpoEdge,
  ExpCcoEdge,
  ExpDisEdge,
  ExpExpEdge,
  ExpGenEdge,
  ExpMfnEdge,
} from "./components/remaining-edge-schemas";

export function getMDXComponents(components?: MDXComponents) {
  return {
    ...defaultMdxComponents,
    ...Python,
    // Base schemas
    BaseNodeSchema,
    BaseEdgeSchema,
    // Node schemas
    GeneSchema,
    DrugSchema,
    DiseaseSchema,
    PhenotypeSchema,
    AnatomySchema,
    BiologicalProcessSchema,
    CellularComponentSchema,
    MolecularFunctionSchema,
    PathwaySchema,
    ExposureSchema,
    // Edge schemas
    AnaAnaEdge,
    AnaGenEdge,
    BpoBpoEdge,
    BpoGenEdge,
    CcoCcoEdge,
    CcoGenEdge,
    DisDieEdge,
    DisGenEdge,
    DisPheEdge,
    DrgDisEdge,
    DrgDrgEdge,
    DrgGenEdge,
    DrgPheEdge,
    ExpBpoEdge,
    ExpCcoEdge,
    ExpDisEdge,
    ExpExpEdge,
    ExpGenEdge,
    ExpMfnEdge,
    GenGenEdge,
    MfnGenEdge,
    MfnMfnEdge,
    PheGenEdge,
    PhePheEdge,
    PwyGenEdge,
    PwyPwyEdge,
    ...components,
  } satisfies MDXComponents;
}

export const useMDXComponents = getMDXComponents;

declare global {
  type MDXProvidedComponents = ReturnType<typeof getMDXComponents>;
}
