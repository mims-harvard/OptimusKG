import defaultMdxComponents from "fumadocs-ui/mdx";
import * as Python from "fumadocs-python/components";
import type { MDXComponents } from "mdx/types";

import { CitationBlock } from "./components/citation-block";
import { DocsSchemaTree } from "./components/docs-schema-tree";

export function getMDXComponents(components?: MDXComponents) {
  return {
    ...defaultMdxComponents,
    ...Python,
    DocsSchemaTree,
    CitationBlock,
    ...components,
  } satisfies MDXComponents;
}

export const useMDXComponents = getMDXComponents;

declare global {
  type MDXProvidedComponents = ReturnType<typeof getMDXComponents>;
}
