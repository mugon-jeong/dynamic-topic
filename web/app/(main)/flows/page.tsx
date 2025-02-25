import React from 'react';
import Link from "next/link";
import {getFlows} from "@/app/(main)/flows/_actions/flow-action";
import WorkFlow from "@/app/(main)/flows/_components/work-flow";

const Page = async () => {
  const flows = await getFlows()
  console.log(flows)
  return (
      <div className="relative flex flex-col">
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