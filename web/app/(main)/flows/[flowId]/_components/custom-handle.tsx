"use client"
import React, {CSSProperties} from 'react';
import {Handle, HandleProps, useEdges, useNodes} from '@xyflow/react';

type Props = HandleProps & {style?: CSSProperties};

const CustomHandle = (props: Props) => {
  const edges = useEdges();
  const nodes = useNodes();
  return (
      <Handle
          {...props}
          isValidConnection={e => {
            console.log('e', e);
            const sourcesFromHandleInState = edges.filter(
                edge => edge.source === e.source,
            ).length;
            const sourceNode = nodes.find(node => node.id === e.source);
            //target
            const targetFromHandleInState = edges.filter(
                edge => edge.target === e.target,
            ).length;

            if (targetFromHandleInState === 1) return false;
            if (sourceNode?.type === 'Condition') return true;
            if (sourcesFromHandleInState < 1) return true;
            return false;
          }}
          className="!-bottom-2 !h-4 !w-4 dark:bg-neutral-800"
      />
  );
};
export default CustomHandle;