/**
 * Edge types whose properties contain only the sources provenance struct.
 * ANA-ANA, BPO-BPO, CCO-CCO, DIS-DIS, GEN-GEN, MFN-MFN, PWY-GEN, PWY-PWY, PHE-PHE
 */
import { SchemaTree } from './schema-tree';
import { baseEdgeFields, sourcesField } from './edge-schema-shared';

const onlySources = [sourcesField];

export function AnaAnaEdge() {
  return <SchemaTree fields={baseEdgeFields('ANA-ANA', 'PARENT', false, onlySources)} />;
}
export function BpoBpoEdge() {
  return <SchemaTree fields={baseEdgeFields('BPO-BPO', 'IS_A', false, onlySources)} />;
}
export function CcoCcoEdge() {
  return <SchemaTree fields={baseEdgeFields('CCO-CCO', 'IS_A', false, onlySources)} />;
}
export function DisDieEdge() {
  return <SchemaTree fields={baseEdgeFields('DIS-DIS', 'PARENT', false, onlySources)} />;
}
export function GenGenEdge() {
  return <SchemaTree fields={baseEdgeFields('GEN-GEN', 'INTERACTS_WITH', false, onlySources)} />;
}
export function MfnMfnEdge() {
  return <SchemaTree fields={baseEdgeFields('MFN-MFN', 'IS_A', false, onlySources)} />;
}
export function PwyGenEdge() {
  return <SchemaTree fields={baseEdgeFields('PWY-GEN', 'INTERACTS_WITH', true, onlySources)} />;
}
export function PwyPwyEdge() {
  return <SchemaTree fields={baseEdgeFields('PWY-PWY', 'PARENT', false, onlySources)} />;
}
export function PhePheEdge() {
  return <SchemaTree fields={baseEdgeFields('PHE-PHE', 'PARENT', false, onlySources)} />;
}
