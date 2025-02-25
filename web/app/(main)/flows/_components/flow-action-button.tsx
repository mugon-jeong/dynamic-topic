"use client";
import React from 'react';
import {Button} from "@/components/ui/button";
import {onRunFlow} from "@/app/(main)/flows/_actions/flow-action";
import {Input} from "@/components/ui/input";

const FlowActionButton = ({flowId}: { flowId: string }) => {
  const [message, setMessage] = React.useState<string>('')
  const runFlow = async () => {
    await onRunFlow(flowId, message)
  }
  return (
      <div className={'flex gap-2'}>
        <Input value={message} onChange={event => setMessage(event.target.value)}/>
        <Button onClick={() => runFlow()}>Run</Button>
      </div>
  );
};

export default FlowActionButton;