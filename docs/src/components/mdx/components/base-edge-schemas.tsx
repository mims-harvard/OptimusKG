/**
 * Edge types whose properties contain only the sources provenance struct.
 * ANA-ANA, BPO-BPO, CCO-CCO, DIS-DIS, GEN-GEN, MFN-MFN, PWY-GEN, PWY-PWY, PHE-PHE
 */

import { baseEdgeFields, sourcesField } from "../../edge-schema-shared";
import { SchemaTreeView } from "../../schema-tree-view";

const onlySources = [sourcesField];

export function AnaAnaEdge() {
  return (
    <SchemaTreeView
      fields={baseEdgeFields("ANA-ANA", "PARENT", false, onlySources)}
    />
  );
}
export function BpoBpoEdge() {
  return (
    <SchemaTreeView
      fields={baseEdgeFields("BPO-BPO", "IS_A", false, onlySources)}
    />
  );
}
export function CcoCcoEdge() {
  return (
    <SchemaTreeView
      fields={baseEdgeFields("CCO-CCO", "IS_A", false, onlySources)}
    />
  );
}
export function DisDieEdge() {
  return (
    <SchemaTreeView
      fields={baseEdgeFields("DIS-DIS", "PARENT", false, onlySources)}
    />
  );
}
export function GenGenEdge() {
  return (
    <SchemaTreeView
      fields={baseEdgeFields("GEN-GEN", "INTERACTS_WITH", false, onlySources)}
    />
  );
}
export function MfnMfnEdge() {
  return (
    <SchemaTreeView
      fields={baseEdgeFields("MFN-MFN", "IS_A", false, onlySources)}
    />
  );
}
export function PwyGenEdge() {
  return (
    <SchemaTreeView
      fields={baseEdgeFields("PWY-GEN", "INTERACTS_WITH", true, onlySources)}
    />
  );
}
export function PwyPwyEdge() {
  return (
    <SchemaTreeView
      fields={baseEdgeFields("PWY-PWY", "PARENT", false, onlySources)}
    />
  );
}
export function PhePheEdge() {
  return (
    <SchemaTreeView
      fields={baseEdgeFields("PHE-PHE", "PARENT", false, onlySources)}
    />
  );
}
