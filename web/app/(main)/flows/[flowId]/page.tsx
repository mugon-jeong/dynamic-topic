'use client'
import React, {useCallback, useEffect, useMemo} from 'react';
import {ResizableHandle, ResizablePanel, ResizablePanelGroup} from "@/components/ui/resizable";
import {
  addEdge,
  Background,
  Connection,
  Controls,
  Edge,
  ReactFlow,
  useEdgesState,
  useNodesState,
  useReactFlow
} from "@xyflow/react";
import {FlowNodeType, FlowTypes} from "@/types/editor";
import {EditorCanvasDefaultCardTypes} from "@/lib/constants";
import {v4} from 'uuid';
import FlowCard from "@/app/(main)/flows/[flowId]/_components/flow-card";
import FlowInstance from "@/app/(main)/flows/[flowId]/_components/flow-instance";
import EditorSidebar from "@/app/(main)/flows/[flowId]/_components/editor-sidebar";
import {useSearchParams} from "next/navigation";
import {getFlow} from "@/app/(main)/flows/_actions/flow-action";

type Props = {
  searchParams: {
    id: string;
  };
};
const initialNodes: FlowNodeType[] = [];
const initialEdges: { id: string; source: string; target: string }[] = [];
const Page = ({
                params,
              }: {
  params: Promise<{ flowId: string }>
}) => {
  const param = useSearchParams()
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
  const {screenToFlowPosition} = useReactFlow();
  const onDragOver = useCallback((event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);
  const onDrop = useCallback(
      (event: {
        preventDefault: () => void;
        dataTransfer: { getData: (arg0: string) => string };
        clientX: number;
        clientY: number;
      }) => {
        event.preventDefault();

        const type = event.dataTransfer.getData(
            'application/reactflow',
        ) as FlowTypes;

        // check if the dropped element is valid
        if (typeof type === 'undefined' || !type) {
          return;
        }

        const position = screenToFlowPosition({
          x: event.clientX,
          y: event.clientY,
        });
        const newNode: FlowNodeType = {
          id: v4(),
          type: type,
          position: position,
          selected: false,
          data: {
            title: type,
            description: EditorCanvasDefaultCardTypes[type].description,
            completed: false,
            current: false,
            metadata: {},
            status: 'pending',
            type: type,
            fileUrl: null,
            fileName: null,
          },
        };

        setNodes(nds => nds.concat(newNode));
      },
      [screenToFlowPosition],
  );

  const nodeTypes = useMemo(
      () => ({
        Start: FlowCard,
        Action: FlowCard,
        Trigger: FlowCard,
        Condition: FlowCard,
        End: FlowCard,
      }),
      [],
  );

  const handleClickCanvas = () => {
    console.log('handleClickCanvas');
    setNodes(nodes.map(node => ({...node, selected: false})));
  };

  const onConnect = useCallback(
      (params: Edge | Connection) => setEdges(eds => addEdge(params, eds)),
      [],
  );

  const onGetWorkFlow = async () => {
    const flowId = await params.then((res) => res.flowId);
    console.log("flowId", flowId)
    const response = await getFlow(flowId);
    setEdges(response.edges.map(edge => {
      return {
        id: edge.source + edge.target,
        source: edge.source,
        target: edge.target
      }
    }));
    setNodes(response.nodes.map(node => JSON.parse(node.data)))
    console.log("response", response)
  };

  useEffect(() => {
    onGetWorkFlow();
  }, []);
  return (
      <ResizablePanelGroup direction="horizontal">
        <ResizablePanel defaultSize={70}>
          <ReactFlow
              onDrop={onDrop}
              onDragOver={onDragOver}
              nodes={nodes}
              onNodesChange={onNodesChange}
              edges={edges}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              fitView
              onClick={handleClickCanvas}
              nodeTypes={nodeTypes}
          >
            <Controls/>
            <Background/>
            {/*<Controls position={'top-left'} className={'text-black'}/>*/}
            {/*<MiniMap*/}
            {/*    position="bottom-left"*/}
            {/*    className="!bg-background"*/}
            {/*    zoomable*/}
            {/*    pannable*/}
            {/*/>*/}
          </ReactFlow>
        </ResizablePanel>
        <ResizableHandle/>
        <ResizablePanel defaultSize={40} className="relative sm:block">
          <FlowInstance>
            <EditorSidebar/>
          </FlowInstance>
        </ResizablePanel>
      </ResizablePanelGroup>
  );
};

export default Page;