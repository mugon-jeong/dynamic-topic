import React from 'react';
import {getFlows} from "@/app/(main)/flows/_actions/flow-action";
import WorkFlow from "@/app/(main)/flows/_components/work-flow";
import {Button} from "@/components/ui/button";
import {Plus} from "lucide-react";
import Link from "next/link";
import {v4} from "uuid";

const Page = async () => {
  const flows = await getFlows()
  console.log(flows)
  return (
      <div className="relative flex flex-col">
        <h1 className="sticky top-0 z-[10] flex items-center justify-between border-b bg-background/50 p-6 text-4xl backdrop-blur-lg">
          Workflows
          <Button size={'icon'}>
            <Link href={`/flows/${v4()}`}>
              <Plus/>
            </Link>
          </Button>
        </h1>
        <div className="relative flex flex-col gap-4">
          <section className="m-2 flex flex-col">
            {flows.map((flow) => (
                <div key={flow.id}>
                  <WorkFlow id={flow.id} title={flow.name}/>
                </div>
            ))}
          </section>
        </div>
      </div>
  );
};

export default Page;