import React from 'react';
import {Card, CardDescription, CardHeader, CardTitle} from "@/components/ui/card";
import Link from "next/link";

const WorkFlow = ({id, title}: { id: string, title: string }) => {
  return (
      <Card className="flex flex-col w-full items-center justify-between mb-2">
        <Link className={'w-full'} href={`/flows/${id}`}>
          <CardHeader className="flex flex-row justify-between">
            <div className="flex flex-col gap-4">
              <CardTitle className="text-lg">{title}</CardTitle>
            </div>
          </CardHeader>
        </Link>
      </Card>
  );
};

export default WorkFlow;