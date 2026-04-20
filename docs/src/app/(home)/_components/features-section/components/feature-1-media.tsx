import Image from "next/image";

import { type SchemaField, SchemaTreeView } from "@/components/schema-tree-view";

import { MaximizableWindow } from "./maximizable-window";
import { SchemaTabContent } from "./schema-tab-content";
import { WindowTabbedEditor } from "./window-tabbed-editor";

const sourcesField: SchemaField = {
  name: "sources",
  type: "Struct",
  description: "Provenance of this edge",
  children: [
    {
      name: "direct",
      type: "List[String]",
      description: "Datasets that directly contributed this relationship",
    },
    {
      name: "indirect",
      type: "List[String]",
      description: "Datasets that referenced this relationship",
    },
  ],
};

const geneFields: SchemaField[] = [
  {
    name: "id",
    type: "String",
    description: "Node identifier in CURIE format (e.g. ENSG00000141510)",
  },
  { name: "label", type: "String", description: "Node type abbreviation (GEN)" },
  {
    name: "properties",
    type: "Struct",
    description: "Gene-specific properties",
    children: [
      {
        name: "symbol",
        type: "String",
        description: "Official HGNC gene symbol (e.g. TP53)",
      },
      { name: "name", type: "String", description: "Full gene name" },
      {
        name: "biotype",
        type: "String",
        description: "Gene biotype (e.g. protein_coding, lncRNA)",
      },
      {
        name: "genomic_location",
        type: "Struct",
        description: "Chromosomal coordinates",
        children: [
          { name: "chromosome", type: "String", description: "Chromosome name" },
          { name: "start", type: "Int64", description: "Start position (0-based)" },
          { name: "end", type: "Int64", description: "End position" },
          { name: "strand", type: "Int32", description: "Strand (+1 forward, -1 reverse)" },
        ],
      },
      {
        name: "transcription_start_site",
        type: "Int64",
        description: "Transcription start site position",
      },
      {
        name: "transcript_ids",
        type: "List[String]",
        description: "All associated Ensembl transcript IDs",
      },
      {
        name: "function_descriptions",
        type: "List[String]",
        description: "Functional descriptions",
      },
      {
        name: "xrefs",
        type: "List[Struct]",
        description: "Cross-references to external databases",
        children: [
          { name: "id", type: "String", description: "External identifier" },
          { name: "source", type: "String", description: "Database name" },
        ],
      },
      {
        name: "sources",
        type: "Struct",
        description: "Provenance of this node",
        children: [
          {
            name: "direct",
            type: "List[String]",
            description: "Datasets that directly contributed this entity",
          },
          {
            name: "indirect",
            type: "List[String]",
            description: "Datasets that referenced this entity",
          },
        ],
      },
    ],
  },
];

const disGenFields: SchemaField[] = [
  { name: "from", type: "String", description: "Source node ID (CURIE format)" },
  { name: "to", type: "String", description: "Target node ID (CURIE format)" },
  { name: "label", type: "String", description: "Edge type label (DIS-GEN)" },
  { name: "relation", type: "String", description: "Relation type" },
  { name: "undirected", type: "Boolean", description: "True" },
  {
    name: "properties",
    type: "Struct",
    description: "Edge-specific properties",
    children: [
      {
        name: "evidence_score",
        type: "Float64",
        description: "Aggregated association evidence score",
      },
      {
        name: "evidence_count",
        type: "Int64",
        description: "Number of evidence items supporting the association",
      },
      {
        name: "evidence_index",
        type: "Float64",
        description: "Combined evidence index (Open Targets)",
      },
      {
        name: "disease_specificity_index",
        type: "Float64",
        description: "DSI, specificity of the gene to this disease",
      },
      {
        name: "disease_pleiotropy_index",
        type: "Float64",
        description: "DPI, number of disease classes the gene is associated with",
      },
      {
        name: "disgenet_score",
        type: "Float64",
        description: "DisGeNET gene–disease association score",
      },
      {
        name: "year_initial",
        type: "String",
        description: "Year of the earliest supporting publication",
      },
      {
        name: "year_final",
        type: "String",
        description: "Year of the most recent supporting publication",
      },
      {
        name: "number_of_pmids",
        type: "Int16",
        description: "Number of supporting PubMed publications",
      },
      {
        name: "number_of_snps",
        type: "Int16",
        description: "Number of supporting SNPs (GWAS evidence)",
      },
      sourcesField,
    ],
  },
];

export function Feature1Media() {
  return (
    <div className="absolute inset-0 flex items-center justify-center overflow-hidden rounded-[1px]">
      <Image
        alt=""
        className="pointer-events-none scale-[1.1] object-cover"
        fill
        sizes="(min-width: 900px) 1200px, 100vw"
        src="/hero/lakeside-village.png"
      />
      <div
        className="pointer-events-none absolute inset-0"
        style={{
          background: "linear-gradient(90deg,rgba(0,0,0,0.12) 0%,rgba(0,0,0,0.22) 100%)",
        }}
      />

      <MaximizableWindow
        appIcon="/dock/editor.svg"
        appId="graph-schema"
        appName="Graph Schema"
        normalStyle={{
          width: "min(42.5rem, calc(100% - var(--l-window-inset, 4rem)))",
          height: "min(35rem, calc(100% - var(--l-window-inset, 4rem)))",
        }}
        title="Graph Schema"
      >
        <WindowTabbedEditor
          tabs={[
            {
              name: "Gene Nodes Schema",
              content: (
                <SchemaTabContent>
                  <SchemaTreeView fields={geneFields} />
                </SchemaTabContent>
              ),
            },
            {
              name: "Disease-Gene Edges Schema",
              content: (
                <SchemaTabContent>
                  <SchemaTreeView fields={disGenFields} />
                </SchemaTabContent>
              ),
            },
          ]}
        />
      </MaximizableWindow>

      <div className="pointer-events-none absolute inset-0 rounded-[1px] border border-(--l-border-subtle)" />
    </div>
  );
}
