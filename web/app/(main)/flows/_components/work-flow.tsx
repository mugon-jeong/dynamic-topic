import React from 'react';
import {Card, CardHeader, CardTitle} from "@/components/ui/card";
import Link from "next/link";
import FlowActionButton from "@/app/(main)/flows/_components/flow-action-button";

const WorkFlow = ({id, title}: { id: string, title: string }) => {
  return (
      <Card className="flex flex-col w-full items-center justify-between mb-2">
        <CardHeader className="w-full flex flex-row justify-between">
          <Link href={`/flows/${id}`}>
            <div className="flex flex-col gap-4">
              <CardTitle className="text-lg">{title}</CardTitle>
            </div>
          </Link>
          <FlowActionButton flowId={id}/>
        </CardHeader>
      </Card>
  );
};

export default WorkFlow;