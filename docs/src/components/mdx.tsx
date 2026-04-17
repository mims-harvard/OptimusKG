import defaultMdxComponents from 'fumadocs-ui/mdx';
import type { MDXComponents } from 'mdx/types';
// Node schemas
import { GeneSchema } from './gene-schema';
import { DrugSchema } from './drug-schema';
import { DiseaseSchema } from './disease-schema';
import { PhenotypeSchema } from './phenotype-schema';
import { AnatomySchema, BiologicalProcessSchema, CellularComponentSchema, MolecularFunctionSchema } from './ontology-node-schemas';
import { PathwaySchema } from './pathway-schema';
import { ExposureSchema } from './exposure-schema';
// Edge schemas
import { AnaAnaEdge, BpoBpoEdge, CcoCcoEdge, DisDieEdge, GenGenEdge, MfnMfnEdge, PwyGenEdge, PwyPwyEdge, PhePheEdge } from './base-edge-schemas';
import { BpoGenEdge, CcoGenEdge, MfnGenEdge } from './go-annotation-edge-schemas';
import { DisGenEdge, PheGenEdge } from './disease-assoc-edge-schemas';
import { DrgDisEdge, DrgDrgEdge, DrgGenEdge, DrgPheEdge } from './drug-edge-schemas';
import { AnaGenEdge, DisPheEdge, ExpBpoEdge, ExpCcoEdge, ExpDisEdge, ExpExpEdge, ExpGenEdge, ExpMfnEdge } from './remaining-edge-schemas';

export function getMDXComponents(components?: MDXComponents) {
  return {
    ...defaultMdxComponents,
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
