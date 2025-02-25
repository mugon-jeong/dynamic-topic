'use server'

import ky from "ky";
import {Edge, Node} from "@xyflow/react";
import {FlowNodeType, FlowTypes} from "@/types/editor";

export const onCreateFlow = async ({id, name, nodes, edges}: {
  id: string,
  name: string,
  nodes: Node[],
  edges: Edge[],
}) => {
  const node = nodes.map(node => {
    return {
      id: node.id,
      type: node.type,
      data: JSON.stringify(node),
    };
  })
  await ky.post("http://127.0.0.1:8080/pipelines", {
    json: {
      id,
      name,
      nodes: node,
      edges
    }
  }).json();
}
interface FlowRes {
  id: string;
  name: string;
}
export const getFlows = async () => {
  return await ky.get("http://127.0.0.1:8080/pipelines").json<FlowRes[]>();
}
interface FlowNodeRes {
  id: string;
  name: string;
  data: string;
}
interface FlowEdgeRes {
  source: string;
  target: string;
}
interface FlowDetailRes {
  id: string;
  name: string;
  nodes: FlowNodeRes[];
  edges: FlowEdgeRes[];
}
export const getFlow = async (id: string) => {
  return await ky.get(`http://127.0.0.1:8080/pipelines/${id}`).json<FlowDetailRes>();
}

export const onRunFlow = async (id: string, message:string) => {
  await ky.post(`http://127.0.0.1:8080/pipelines/${id}/run`,{
    json: {
      message,
    }
  })
}