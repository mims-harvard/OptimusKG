import defaultMdxComponents from 'fumadocs-ui/mdx';
import type { MDXComponents } from 'mdx/types';
// Base schemas
import { BaseNodeSchema, BaseEdgeSchema } from './components/base-schemas';
// Node schemas
import { GeneSchema } from '../gene-schema';
import { DrugSchema } from './components/drug-schema';
import { DiseaseSchema } from './components/disease-schema';
import { PhenotypeSchema } from './components/phenotype-schema';
import { AnatomySchema, BiologicalProcessSchema, CellularComponentSchema, MolecularFunctionSchema } from './components/ontology-node-schemas';
import { PathwaySchema } from './components/pathway-schema';
import { ExposureSchema } from './components/exposure-schema';
// Edge schemas
import { AnaAnaEdge, BpoBpoEdge, CcoCcoEdge, DisDieEdge, GenGenEdge, MfnMfnEdge, PwyGenEdge, PwyPwyEdge, PhePheEdge } from './components/base-edge-schemas';
import { BpoGenEdge, CcoGenEdge, MfnGenEdge } from './components/go-annotation-edge-schemas';
import { DisGenEdge, PheGenEdge } from '../disease-assoc-edge-schemas';
import { DrgDisEdge, DrgDrgEdge, DrgGenEdge, DrgPheEdge } from './components/drug-edge-schemas';
import { AnaGenEdge, DisPheEdge, ExpBpoEdge, ExpCcoEdge, ExpDisEdge, ExpExpEdge, ExpGenEdge, ExpMfnEdge } from './components/remaining-edge-schemas';

export function getMDXComponents(components?: MDXComponents) {
  return {
    ...defaultMdxComponents,
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
