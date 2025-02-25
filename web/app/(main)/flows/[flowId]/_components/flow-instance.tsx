'use client';
import React, {useCallback, useEffect, useState} from 'react';
import {Button} from '@/components/ui/button';
import {useReactFlow} from '@xyflow/react';
import {v4} from 'uuid';
import {onCreateFlow} from "@/app/(main)/flows/_actions/flow-action";

type Props = {
  children: React.ReactNode;
};
const FlowInstance = ({children}: Props) => {
  const {getNodes, getEdges} = useReactFlow();
  const [isFlow, setIsFlow] = useState<string[]>([]);
  const onFlowCreate = useCallback(async () => {
    const data = {
      id: v4(),
      name: 'Flow',
      nodes: getNodes(),
      edges: getEdges(),
    }
    await onCreateFlow(data);
    console.log(data);
    // if (status == 200) alert('Flow created successfully');
  }, [getNodes, getEdges, isFlow]);

  useEffect(() => {
    console.log(getEdges());
    setIsFlow(getEdges().map(edge => edge.id));
  }, [getEdges]);

  return (
      <div className="flex flex-col gap-2">
        <div className="flex gap-3 p-4">
          <Button
              onClick={onFlowCreate}
              disabled={getNodes().length - getEdges().length != 1}
          >
            Save
          </Button>
          <Button disabled={isFlow.length < 1} onClick={() => {}}>
            Publish
          </Button>
        </div>
        {children}
      </div>
  );
};
export default FlowInstance;